"""Running the Xetra ETL application"""
import argparse
import logging
import logging.config

import yaml

# from xetra.common.s3 import S3BucketConnector
# from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    """
      entry point to run the xetra ETL job
    """
    config_path = "C:/Users/Kavish Bhathija/Desktop/Python/data-pipeline/python-etl-prod-practice/configs/config.yml"
    config = yaml.safe_load(open(config_path))
    # print(config)
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("this is a test.")


if __name__ == '__main__':
    main()