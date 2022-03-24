"""
Methods for processing the meta file
"""
import collections
from datetime import datetime, timedelta

import pandas as pd

from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException

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
        pass
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
        pass