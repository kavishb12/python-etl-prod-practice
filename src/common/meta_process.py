"""
Methods for processing the meta file
"""
from cmath import e
import collections
from datetime import datetime,timedelta
from typing import Counter

import pandas as pd

from src.common.s3 import S3BucketConnector
from src.common.constants import MetaProcessFormat
from src.common.custom_exceptions import WrongMetaFileException

class MetaProcess():

  
    """
    class for working with the meta file
    """

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):

        """
        Updating the meta file with the processed Xetra dates and todays date as processed date
        :param: extract_date_list -> a list of dates that are extracted from the source
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        """
      
        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value,
                                      MetaProcessFormat.META_PROCESS_COL.value])
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_concat = pd.concat([df_new,df_old])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey: 
            df_concat = df_new 
        s3_bucket_meta.write_df_to_s3(df_concat,meta_key,MetaProcessFormat.META_FILE_FORMAT.value)
        return True


    @staticmethod
    def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file
        :param: first_date -> the earliest date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        returns:
        min_date: first date that should be processed
        return_date_list: list of all dates from min_date till today
        """     
        min_date = datetime.strptime(first_date,MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)
        today = datetime.today().date()
        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)
            src_dates = set(pd.to_datetime(df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]).dt.date)
            date_list = [(min_date + timedelta(days=x)) for x in range((today - min_date).days + 1)]
            if (set(date_list[1:]) - src_dates):
                min_date = min(set(date_list[1:]) - src_dates) - timedelta(days=1)
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) for date in date_list if date >= min_date]
                return_min_date = (min_date +  timedelta(days=1)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            else:
                return_dates = []
                return_min_date = datetime(2200,1,1).date()
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            return_dates = [(min_date + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for x in range((today - min_date).days + 1)]    
            return_min_date = first_date
        return return_min_date, return_dates
