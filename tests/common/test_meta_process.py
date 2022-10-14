from datetime import datetime, timedelta
from io import StringIO
from os import environ
from unittest import TestCase

from boto3 import resource
from moto import mock_s3
from pandas import read_csv, to_datetime

from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.meta_process import MetaProcess
from xetra.common.s3 import S3BucketConnector

_META_DATE_FORMAT = MetaProcessFormat.META_DATE_FORMAT
_META_PROCESS_COL = MetaProcessFormat.META_PROCESS_COL
_META_PROCESS_DATE_FORMAT = MetaProcessFormat.META_PROCESS_DATE_FORMAT
_META_SOURCE_DATE_COL = MetaProcessFormat.META_SOURCE_DATE_COL


class TestMetaProcessMethods(TestCase):
    """
    Testing the MetaProcess class.
    """
    _mock_s3 = mock_s3()
    _s3_access_key = "AWS_ACCESS_KEY_ID"
    _s3_secret_key = "AWS_SECRET_ACCESS_KEY"
    _s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
    _s3_bucket_name = "test-bucket"
    environ[_s3_access_key] = "KEY1"
    environ[_s3_secret_key] = "KEY2"

    def setUp(self):
        """
        Setting up the environment
        """
        self._mock_s3.start()
        self._s3 = resource("s3", endpoint_url=self._s3_endpoint_url)
        self._s3.create_bucket(
            Bucket=self._s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"}
        )
        self._s3_bucket = self._s3.Bucket(self._s3_bucket_name)
        self._s3_bucket_meta = S3BucketConnector(
            self._s3_endpoint_url,
            self._s3_bucket_name
        )
        self._dates = [
            (
                datetime.today().date() - timedelta(days=day)
            ).strftime(_META_DATE_FORMAT.value)
            for day in range(8)
        ]

    def tearDown(self):
        self._mock_s3.stop()

    def test_update_meta_file_no_meta_file(self):
        """
        Tests the update_meta_file method when there is no meta file
        """
        date_list_expected = ["2021-04-16", "2021-04-17"]
        proc_date_list_expected = [datetime.today().date()] * 2
        meta_key = "meta.csv"
        MetaProcess.update_meta_file(
            date_list_expected,
            meta_key,
            self._s3_bucket_meta
        )
        data = (
            self._s3_bucket.Object(key=meta_key).get().get("Body").read()
            .decode("utf-8")
        )
        out_buffer = StringIO(data)
        df_meta_result = read_csv(out_buffer)
        date_list_result = list(df_meta_result[_META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            to_datetime(df_meta_result[_META_PROCESS_COL.value]).dt.date
        )
        self.assertEqual(date_list_expected, date_list_result)
        self.assertEqual(proc_date_list_expected, proc_date_list_result)
        self._s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": meta_key}]}
        )

    def test_update_meta_file_empty_date_list(self):
        """
        Tests the update_meta_file method when the argument extract_date_list
        is empty
        """
        return_expected = True
        log_expected = "The dataframe is empty! No file will be written!"
        date_list = []
        meta_key = "meta.csv"
        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(
                date_list,
                meta_key,
                self._s3_bucket_meta
            )
            self.assertIn(log_expected, logm.output[1])
        self.assertEqual(return_expected, result)

    def test_update_meta_file_meta_file_ok(self):
        """
        Tests the update_meta_file method when there already a meta file
        """
        date_list_old = ["2021-04-12", "2021-04-13"]
        date_list_new = ["2021-04-16", "2021-04-17"]
        date_list_expected = date_list_old + date_list_new
        proc_date_list_expected = [datetime.today().date()] * 4
        meta_key = "meta.csv"
        meta_content = (
            f"{_META_SOURCE_DATE_COL.value},"
            f"{_META_PROCESS_COL.value}\n"
            f"{date_list_old[0]},"
            f"{datetime.today().strftime(_META_PROCESS_DATE_FORMAT.value)}\n"
            f"{date_list_old[1]},"
            f"{datetime.today().strftime(_META_PROCESS_DATE_FORMAT.value)}"
        )
        self._s3_bucket.put_object(Body=meta_content, Key=meta_key)
        MetaProcess.update_meta_file(
            date_list_new,
            meta_key,
            self._s3_bucket_meta
        )
        data = (
            self._s3_bucket.Object(key=meta_key).get().get("Body").read()
            .decode("utf-8")
        )
        out_buffer = StringIO(data)
        df_meta_result = read_csv(out_buffer)
        date_list_result = list(df_meta_result[_META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            to_datetime(df_meta_result[_META_PROCESS_COL.value]).dt.date
        )
        self.assertEqual(date_list_expected, date_list_result)
        self.assertEqual(proc_date_list_expected, proc_date_list_result)
        self._s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": meta_key}]}
        )

    def test_update_meta_file_meta_file_wrong(self):
        """
        Tests the update_meta_file method when there is a wrong meta file
        """
        date_list_old = ["2021-04-12", "2021-04-13"]
        date_list_new = ["2021-04-16", "2021-04-17"]
        meta_key = "meta.csv"
        meta_content = (
            f"wrong_column,{_META_SOURCE_DATE_COL.value},"

            f"{date_list_old[0]},"
            f"{datetime.today().strftime(_META_PROCESS_DATE_FORMAT.value)}\n"
            f"{date_list_old[1]},"
            f"{datetime.today().strftime(_META_PROCESS_DATE_FORMAT.value)}\n"
        )
        self._s3_bucket.put_object(Body=meta_content, Key=meta_key)
        with self.assertRaises(WrongMetaFileException):
            MetaProcess.update_meta_file(
                date_list_new,
                meta_key,
                self._s3_bucket_meta
            )
        self._s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": meta_key}]}
        )

    def test_return_date_list_no_meta_file(self):
        """
        Tests the return_date_list method when there is no meta file
        """
        date_list_expected = [
            (
                datetime.today().date() - timedelta(days=day)
            ).strftime(_META_DATE_FORMAT.value)
            for day in range(4)
        ]
        min_date_expected = (
            datetime.today().date() - timedelta(days=2)
        ).strftime(_META_DATE_FORMAT.value)
        first_date = min_date_expected
        meta_key = "meta.csv"
        min_date_return, date_list_return = MetaProcess.return_date_list(
            first_date,
            meta_key,
            self._s3_bucket_meta
        )
        self.assertEqual(set(date_list_expected), set(date_list_return))
        self.assertEqual(min_date_expected, min_date_return)

    def test_return_date_list_meta_file_ok(self):
        """
        Tests the return_date_list method when there is a meta file
        """
        min_date_expected = [
            (
                datetime.today().date() - timedelta(days=1)
            ).strftime(_META_DATE_FORMAT.value),
            (
                datetime.today().date() - timedelta(days=2)
            ).strftime(_META_DATE_FORMAT.value),
            (
                datetime.today().date() - timedelta(days=7)
            ).strftime(_META_DATE_FORMAT.value),
        ]
        date_list_expected = [
            [
                (
                    datetime.today().date() - timedelta(days=day)
                ).strftime(_META_DATE_FORMAT.value)
                for day in range(3)
            ],
            [
                (
                    datetime.today().date() - timedelta(days=day)
                ).strftime(_META_DATE_FORMAT.value)
                for day in range(4)
            ],
            [
                (
                    datetime.today().date() - timedelta(days=day)
                ).strftime(_META_DATE_FORMAT.value)
                for day in range(9)
            ],
        ]
        meta_key = "meta.csv"
        meta_content = (
            f"{_META_SOURCE_DATE_COL.value},"
            f"{_META_PROCESS_COL.value}\n"
            f"{self._dates[3]},{self._dates[0]}\n"
            f"{self._dates[4]},{self._dates[0]}"
        )
        self._s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date_list = [
            self._dates[1],
            self._dates[4],
            self._dates[7],
        ]
        for count, first_date in enumerate(first_date_list):
            min_date_return, date_list_return = MetaProcess.return_date_list(
                first_date,
                meta_key,
                self._s3_bucket_meta
            )
            self.assertEqual(
                set(date_list_expected[count]),
                set(date_list_return)
            )
            self.assertEqual(min_date_expected[count], min_date_return)
        self._s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": meta_key}]}
        )

    def test_return_date_list_meta_file_wrong(self):
        """
        Tests the return_date_list method when there is a wrong meta file
        """
        meta_key = "meta.csv"
        meta_content = (
            f"wrong_column,{_META_PROCESS_COL.value}\n"
            f"{self._dates[3]},{self._dates[0]}\n"
            f"{self._dates[4]},{self._dates[0]}"
        )
        self._s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self._dates[1]
        with self.assertRaises(KeyError):
            MetaProcess.return_date_list(
                first_date,
                meta_key,
                self._s3_bucket_meta
            )
        self._s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": meta_key}]}
        )

    def test_return_date_list_empty_date_list(self):
        """
        Tests the return_date_list method when there are no dates to be returned
        """
        min_date_expected = "2200-01-01"
        date_list_expected = []
        meta_key = "meta.csv"
        meta_content = (
            f"{_META_SOURCE_DATE_COL.value},"
            f"{_META_PROCESS_COL.value}\n"
            f"{self._dates[0]},{self._dates[0]}\n"
            f"{self._dates[1]},{self._dates[0]}"
        )
        self._s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self._dates[0]
        min_date_return, date_list_return = MetaProcess.return_date_list(
            first_date,
            meta_key,
            self._s3_bucket_meta
        )
        self.assertEqual(
                set(date_list_expected),
                set(date_list_return)
            )
        self.assertEqual(min_date_expected, min_date_return)
        self._s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": meta_key}]}
        )
