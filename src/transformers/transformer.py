"""Xetra ETL Component"""
import logging
from datetime import datetime
from typing import NamedTuple

import pandas as pd

from src.common.s3 import S3BucketConnector
from src.common.meta_process import MetaProcess

class SourceConfig(NamedTuple):
    """
    Class for source configuration data
    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volumne in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str




class TargetConfig(NamedTuple):
    """
    Class for target configuration data
    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class ETL():
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, meta_key: str,
                 src_args: SourceConfig, trg_args: TargetConfig):
        """
        Constructor for XetraTransformer
        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param trg_args: NamedTouple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.src_args.src_first_extract_date, self.meta_key, self.s3_bucket_trg)
        self.meta_update_list = [date for date in self.extract_date_list\
            if date >= self.extract_date]

    def extract(self) -> pd.DataFrame():
        """
        Read the source data and concatenates them to one Pandas DataFrame
        :returns:
          data_frame: Pandas DataFrame with the extracted data
        """
        self._logger.info("Extracting data...")
        files = [key for date in self.extract_date_list for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            df = pd.DataFrame()
        else:
            df = pd.concat([self.s3_bucket_src.read_csv_to_df(obj) for obj in files],ignore_index=True)
        self._logger.info("Extracting data finished.")
        return df

    def transform_report1(self, df: pd.DataFrame):
        """
        Applies the necessary transformation to create report 1
        :param data_frame: Pandas DataFrame as Input
        :returns:
          data_frame: Transformed Pandas DataFrame as Output
        """
        if df.empty:
            self._logger.info("Empty Dataframe")
            return df
        self._logger.info("Starting trasformation...")
        # filter for columns
        df = df.loc[:,self.src_args.src_columns]
        # drop null values
        df.dropna(inplace=True)
        # calculate opening price 
        df[self.trg_args.trg_col_op_price] = df.sort_values(by=[self.src_args.src_col_time])\
                                            .groupby([self.src_args.src_col_isin,self.src_args.src_col_date])[self.src_args.src_col_start_price].transform('first')
        # calculate closing price
        df[self.trg_args.trg_col_clos_price] = df.sort_values(by=[self.src_args.src_col_time])\
                                            .groupby([self.src_args.src_col_isin,self.src_args.src_col_date])[self.src_args.src_col_start_price].transform('last')
         # Renaming columns
        df.rename(columns={
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_dail_trad_vol
            }, inplace=True)
        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, traded volume
        df = df.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date], as_index=False)\
                .agg({
                    self.trg_args.trg_col_op_price: 'min',
                    self.trg_args.trg_col_clos_price: 'min',
                    self.trg_args.trg_col_min_price: 'min',
                    self.trg_args.trg_col_max_price: 'max',
                    self.trg_args.trg_col_dail_trad_vol: 'sum'})
       
        # Change of current day's closing price compared to the
        # previous trading day's closing price in %
        df[self.trg_args.trg_col_ch_prev_clos] = df\
            .sort_values(by=[self.src_args.src_col_date])\
                .groupby([self.src_args.src_col_isin])[self.trg_args.trg_col_op_price]\
                    .shift(1)
        df[self.trg_args.trg_col_ch_prev_clos] = (
            df[self.trg_args.trg_col_op_price] \
            - df[self.trg_args.trg_col_ch_prev_clos]
            ) / df[self.trg_args.trg_col_ch_prev_clos ] * 100
        # Rounding to 2 decimals
        df = df.round(decimals=2)        
        # Removing the day before extract_date
        df = df[df[self.trg_args.trg_col_date] >= self.extract_date].reset_index(drop=True)
        self._logger.info('Applying transformations to data finished.')        
        return df

    def load(self, data_frame: pd.DataFrame):
        """
        Saves a Pandas DataFrame to the target
        :param data_frame: Pandas DataFrame as Input
        """
        # writing to target
        key = self.trg_args.trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + "." + self.trg_args.trg_format
        self.s3_bucket_trg.write_df_to_s3(data_frame,key,self.trg_args.trg_format)
        self._logger.info("Loading finished.")
        # updating meta file
        MetaProcess.update_meta_file(self.meta_update_list,self.meta_key,self.s3_bucket_trg)
        self._logger.info("Updated meta file.")
        return True
    
    def etl_report1(self):
        """
        Extract, transform and load to create report 1
        """
        self._logger.info("ETL Started")
        # extract values
        df = self.extract()
        # transform the dataframe
        df = self.transform_report1(df)
        # write dataframe to target
        self.load(df)
        self._logger.info("ETL finished")
        return True