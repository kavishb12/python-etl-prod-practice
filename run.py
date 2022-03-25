"""Running the Xetra ETL application"""
import argparse
import logging
import logging.config

import yaml

from src.common.s3 import S3BucketConnector
from src.transformers.transformer import ETL, SourceConfig, TargetConfig


def main():
    """
      entry point to run the xetra ETL job
    """
    # configure parser
    parser = argparse.ArgumentParser(description='Run the ETL job.')
    parser.add_argument('config_path', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config_path))
    
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("this is a test.")

    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector class instances for source and target
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])
    # reading source configuration
    source_config = SourceConfig(**config['source'])
    # reading target configuration
    target_config = TargetConfig(**config['target'])
    # reading meta file configuration
    meta_config = config['meta']
    # creating XetraETL class instance
    logger.info('ETL job started')
    xetra_etl = ETL(s3_bucket_src, s3_bucket_trg,
                    meta_config['meta_key'], source_config, target_config)
    # running etl job for xetra report1
    xetra_etl.etl_report1()
    logger.info('Xetra ETL job finished.')


if __name__ == '__main__':
    main()