""" Test S3 Bucket Methods"""
import os
import unittest
import pandas as pd

from moto import mock_s3
from io import StringIO, BytesIO

import boto3
from src.common.custom_exceptions import WrongFormatException

from src.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the s3 bucket connector methods
    """
    def setUp(self):
        """
        Setting up environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 args as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating a bucket on the mocked s3
        self.s3 = boto3.resource(service_name = 's3', endpoint_url = self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': 'eu-central-1'
                                })
        self.s3_bucket=self.s3.Bucket(self.s3_bucket_name)
        # creatiing a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                            self.s3_secret_key,
                                            self.s3_endpoint_url,
                                            self.s3_bucket_name)

    def tearDown(self):
        """
        Executing after unit test
        """
        # mocking s3 set up stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 keys
        as list on the mocked s3 bucket
        """
        # Expected results
        prefix_exp = "prefix/"
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        # Test init
        csv_content = """col1,col2
                        val1,val2"""
        self.s3_bucket.put_object(Body=csv_content,Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content,Key=key2_exp)
        # Method execution
        list_results = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method exection
        self.assertEqual(len(list_results),2)
        self.assertIn(key1_exp,list_results)
        self.assertIn(key2_exp,list_results)
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )
    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of wrong
        or empty input
        """
        # Expected results
        prefix_exp = "no-prefix/"
        # Method execution
        list_results = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method exection
        self.assertTrue(not list_results)
    
    def test_read_csv_to_df_ok(self):
        """
        Tests read_csv_to_df for reading
        a .csv file from the mocked s3 bucket
        """
        # expected results
        key_exp = 'test.csv'
        col_1_exp = 'col1'
        col_2_exp = 'col2'
        val_1_exp = 'val1'
        val_2_exp = 'val2'
        log_exp = f"Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}"
        # test init
        csv_content = f"{col_1_exp},{col_2_exp}\n{val_1_exp},{val_2_exp}"
        self.s3_bucket.put_object(Body=csv_content,Key=key_exp)
        # method execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_csv_to_df(key_exp)
            self.assertIn(log_exp, logm.output[0])
        # run tests
        self.assertEqual(df_result.shape[0],1)
        self.assertEqual(df_result.shape[1],2)
        self.assertEqual(df_result[col_1_exp][0],val_1_exp)
        self.assertEqual(df_result[col_2_exp][0],val_2_exp)
        # cleanup
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """
        Tests write_csv_to_df for writing
        an empty file to the mocked s3 bucket
        """
        # expected results
        return_exp = None
        log_exp = "The data frame is empty."
        # test init
        df_empty = pd.DataFrame()
        key='key.csv'
        file_format = 'csv'
        # method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty,key,file_format)
            self.assertIn(log_exp, logm.output[0])
        # run tests
        self.assertEqual(return_exp,result)

    def test_write_df_to_s3_csv(self):
        """
        Tests write_csv_to_df for writing
        a csv file to the mocked s3 bucket
        """
        # expected results
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp = 'test.csv'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        # test init
        file_format = 'csv'
        # method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,file_format)
            self.assertIn(log_exp, logm.output[0])
        # run tests
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)        
        self.assertEqual(return_exp,result)
        self.assertTrue(df_exp.equals(df_result))
        # cleanup
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
        

    def test_write_df_to_s3_parquet(self):
        """
        Tests write_csv_to_df for writing
        :a parquet file to the mocked s3 bucket
        """
        # expected results
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp = 'test.parquet'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        # test init
        file_format = 'parquet'
        # method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,file_format)
            self.assertIn(log_exp, logm.output[0])
        # run tests
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)        
        self.assertEqual(return_exp,result)
        self.assertTrue(df_exp.equals(df_result))
        # cleanup
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_wrong(self):
        """
        Tests write_csv_to_df for writing
        :a parquet file to the mocked s3 bucket
        """
        # expected results
        exception_exp = WrongFormatException
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp = 'test.parquet'
        file_format = 'tsv1'
        log_exp = f'The file format {file_format} is not supported'
        # test init
        # method execution
        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,file_format)
            self.assertIn(log_exp, logm.output[0])


if __name__ == "__main__":
    unittest.main()
