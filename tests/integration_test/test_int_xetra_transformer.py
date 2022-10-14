"""Integration Test XetraETLMethods"""
from datetime import datetime, timedelta
from io import BytesIO
from unittest import main, TestCase

from boto3 import resource
from pandas import DataFrame, read_parquet

from xetra.common.constants import MetaProcessFormat
from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import (
    XetraETL,
    XetraSourceConfig,
    XetraTargetConfig,
)


class IntTestXetraETLMethods(TestCase):
    """
    Integration testing the XetraETL class.
    """
    _s3_access_key = "AWS_ACCESS_KEY_ID"
    _s3_secret_key = "AWS_SECRET_ACCESS_KEY"
    _s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
    _s3_bucket_name_src = "xetra-integration-test-src"
    _s3_bucket_name_trg = "xetra-integration-test-trg"
    _meta_key = "meta_file.csv"

    def setUp(self):
        """
        Setting up the environment
        """
        self._s3 = resource(
            service_name="s3",
            endpoint_url=self._s3_endpoint_url
        )
        self._src_bucket = self._s3.Bucket(self._s3_bucket_name_src)
        self._trg_bucket = self._s3.Bucket(self._s3_bucket_name_trg)
        self._s3_bucket_src = S3BucketConnector(
            self._s3_endpoint_url,
            self._s3_bucket_name_src
        )
        self._s3_bucket_trg = S3BucketConnector(
            self._s3_endpoint_url,
            self._s3_bucket_name_trg
        )
        self._dates = [
            (
                datetime.today().date() - timedelta(days=day)
            ).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            for day in range(8)
        ]
        conf_dict_src = {
            "src_first_extract_date": self._dates[3],
            "src_columns": [
                "ISIN",
                "Mnemonic",
                "Date",
                "Time",
                "StartPrice",
                "EndPrice",
                "MinPrice",
                "MaxPrice",
                "TradedVolume",
            ],
            "src_col_date": "Date",
            "src_col_isin": "ISIN",
            "src_col_time": "Time",
            "src_col_start_price": "StartPrice",
            "src_col_min_price": "MinPrice",
            "src_col_max_price": "MaxPrice",
            "src_col_traded_vol": "TradedVolume",
        }
        conf_dict_trg = {
            "trg_col_isin": "isin",
            "trg_col_date": "date",
            "trg_col_op_price": "opening_price_eur",
            "trg_col_close_price": "closing_price_eur",
            "trg_col_min_price": "minimum_price_eur",
            "trg_col_max_price": "maximum_price_eur",
            "trg_col_daily_trade_vol": "daily_traded_volume",
            "trg_col_ch_prev_close": "change_prev_closing_%",
            "trg_key": "report1/xetra_daily_report1_",
            "trg_key_date_format": "%Y%m%d_%H%M%S",
            "trg_format": "parquet"
        }
        self._source_config = XetraSourceConfig(**conf_dict_src)
        self._target_config = XetraTargetConfig(**conf_dict_trg)
        columns_src = [
            "ISIN",
            "Mnemonic",
            "Date",
            "Time",
            "StartPrice",
            "EndPrice",
            "MinPrice",
            "MaxPrice",
            "TradedVolume",
        ]
        data = [
            ["AT0000A0E9W5", "SANT", self._dates[5], "12:00", 20.19, 18.45, 18.20, 20.33, 877],
            ["AT0000A0E9W5", "SANT", self._dates[4], "15:00", 18.27, 21.19, 18.27, 21.34, 987],
            ["AT0000A0E9W5", "SANT", self._dates[3], "13:00", 20.21, 18.27, 18.21, 20.42, 633],
            ["AT0000A0E9W5", "SANT", self._dates[3], "14:00", 18.27, 21.19, 18.27, 21.34, 455],
            ["AT0000A0E9W5", "SANT", self._dates[2], "07:00", 20.58, 19.27, 18.89, 20.58, 9066],
            ["AT0000A0E9W5", "SANT", self._dates[2], "08:00", 19.27, 21.14, 19.27, 21.14, 1220],
            ["AT0000A0E9W5", "SANT", self._dates[1], "07:00", 23.58, 23.58, 23.58, 23.58, 1035],
            ["AT0000A0E9W5", "SANT", self._dates[1], "08:00", 23.58, 24.22, 23.31, 24.34, 1028],
            ["AT0000A0E9W5", "SANT", self._dates[1], "09:00", 24.22, 22.21, 22.21, 25.01, 1523],
        ]
        self._df_src = DataFrame(data, columns=columns_src)
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[0:0],
            f"{self._dates[5]}/{self._dates[5]}_BINS_XETR12.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[1:1],
            f"{self._dates[4]}/{self._dates[4]}_BINS_XETR15.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[2:2],
            f"{self._dates[3]}/{self._dates[3]}_BINS_XETR13.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[3:3],
            f"{self._dates[3]}/{self._dates[3]}_BINS_XETR14.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[4:4],
            f"{self._dates[2]}/{self._dates[2]}_BINS_XETR07.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[5:5],
            f"{self._dates[2]}/{self._dates[2]}_BINS_XETR08.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[6:6],
            f"{self._dates[1]}/{self._dates[1]}_BINS_XETR07.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[7:7],
            f"{self._dates[1]}/{self._dates[1]}_BINS_XETR08.csv",
            "csv"
        )
        self._s3_bucket_src.write_df_to_s3(
            self._df_src.loc[8:8],
            f"{self._dates[1]}/{self._dates[1]}_BINS_XETR09.csv",
            "csv"
        )
        columns_report = [
            "ISIN",
            "Date",
            "opening_price_eur",
            "closing_price_eur",
            "minimum_price_eur",
            "maximum_price_eur",
            "daily_traded_volume",
            "change_prev_closing_%",
        ]
        data_report = [
            ["AT0000A0E9W5", self._dates[3], 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
            ["AT0000A0E9W5", self._dates[2], 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
            ["AT0000A0E9W5", self._dates[1], 23.58, 24.22, 22.21, 25.01, 3586, 14.58],
        ]
        self._df_report = DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        for key in self._src_bucket.objects.all():
            key.delete()
        for key in self._trg_bucket.objects.all():
            key.delete()

    def test_int_etl_report1_no_metafile(self):
        """
        Integration test for the etl_report1 method
        """
        df_expected = self._df_report
        meta_expected = [
            self._dates[3],
            self._dates[2],
            self._dates[1],
            self._dates[0]
        ]
        xetra_etl = XetraETL(
            self._s3_bucket_src,
            self._s3_bucket_trg,
            self._meta_key,
            self._source_config,
            self._target_config
        )
        xetra_etl.etl_report1()
        trg_file = (
            self._s3_bucket_trg
            .list_files_in_prefix(self._target_config.trg_key)[0]
        )
        data = self._trg_bucket.Object(key=trg_file).get().get("Body").read()
        out_buffer = BytesIO(data)
        df_result = read_parquet(out_buffer)
        self.assertTrue(df_expected.equals(df_result))
        meta_file = self._s3_bucket_trg.list_files_in_prefix(self._meta_key)[0]
        df_meta_result = self._s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result["source_date"]), meta_expected)

if __name__ == "__main__":
    main()
