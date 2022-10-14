"""Running the Xetra ETL application"""
from argparse import ArgumentParser
from logging import getLogger
from logging.config import dictConfig

from yaml import safe_load

from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import (
    XetraETL,
    XetraSourceConfig,
    XetraTargetConfig,
)


def main():
    """
    entry point to run the xetra ETL job
    """
    parser = ArgumentParser(description="Run the Xetra ETL job.")
    parser.add_argument("config", help="A configuration file in YAML format.")
    args = parser.parse_args()
    config = safe_load(open(args.config))
    log_config = config["logging"]
    dictConfig(log_config)
    logger = getLogger(__name__)
    s3_config = config["s3"]
    s3_bucket_src = S3BucketConnector(
        endpoint_url=s3_config["src_endpoint_url"],
        bucket=s3_config["src_bucket"]
    )
    s3_bucket_trg = S3BucketConnector(
        endpoint_url=s3_config["trg_endpoint_url"],
        bucket=s3_config["trg_bucket"]
    )
    source_config = XetraSourceConfig(**config["source"])
    target_config = XetraTargetConfig(**config["target"])
    meta_config = config["meta"]
    logger.info("Xetra ETL job started")
    xetra_etl = XetraETL(
        s3_bucket_src,
        s3_bucket_trg,
        meta_config["meta_key"],
        source_config,
        target_config
    )
    xetra_etl.etl_report1()
    logger.info("Xetra ETL job finished.")


if __name__ == "__main__":
    main()
