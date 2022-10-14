"""TestS3BucketConnectorMethods"""
from io import BytesIO, StringIO
from os import environ
from unittest import main, TestCase

from boto3 import resource
from moto import mock_s3
from pandas import DataFrame, read_csv, read_parquet

from xetra.common.custom_exceptions import WrongFormatException
from xetra.common.s3 import S3BucketConnector


class TestS3BucketConnectorMethods(TestCase):
    """
    Testing the S3BucketConnector class
    """
    _mock_s3 = mock_s3()
    _s3_access_key = "AWS_ACCESS_KEY_ID"
    _s3_secret_key = "AWS_SECRET_ACCESS_KEY"
    _s3_endpoint_url = "https://s3.eu-central-2.amazonaws.com"
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
            CreateBucketConfiguration={"LocationConstraint": "eu-central-2"}
        )
        self._s3_bucket = self._s3.Bucket(self._s3_bucket_name)
        self._s3_bucket_conn = S3BucketConnector(
            self._s3_endpoint_url,
            self._s3_bucket_name
        )

    def tearDown(self):
        """
        Executing after unittests
        """
        self._mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 file keys as list
        on the mocked s3 bucket
        """
        prefix_expected = "prefix/"
        key1_expected = f"{prefix_expected}test1.csv"
        key2_expected = f"{prefix_expected}test2.csv"
        csv_content = "col1,col2\nvalA,valB"
        self._s3_bucket.put_object(Body=csv_content, Key=key1_expected)
        self._s3_bucket.put_object(Body=csv_content, Key=key2_expected)
        list_result = self._s3_bucket_conn.list_files_in_prefix(prefix_expected)
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_expected, list_result)
        self.assertIn(key2_expected, list_result)
        self._s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {"Key": key1_expected},
                    {"Key": key2_expected},
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of a wrong or not
        existing prefix
        """
        prefix_expected = "no-prefix/"
        list_result = self._s3_bucket_conn.list_files_in_prefix(prefix_expected)
        self.assertFalse(list_result)

    def test_read_csv_to_df_ok(self):
        """
        Tests the read_csv_to_df method for reading 1 .csv file from the mocked
        s3 bucket
        """
        key_expected = "test.csv"
        col1_expected = "col1"
        col2_expected = "col2"
        val1_expected = "val1"
        val2_expected = "val2"
        log_expected = (
            f"Reading file {self._s3_endpoint_url}/{self._s3_bucket_name}/"
            f"{key_expected}"
        )
        csv_content = (
            f"{col1_expected},{col2_expected}\n{val1_expected},{val2_expected}"
        )
        self._s3_bucket.put_object(Body=csv_content, Key=key_expected)
        with self.assertLogs() as logm:
            df_result = self._s3_bucket_conn.read_csv_to_df(key_expected)
            self.assertIn(log_expected, logm.output[0])
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_expected, df_result[col1_expected][0])
        self.assertEqual(val2_expected, df_result[col2_expected][0])
        self._s3_bucket.delete_objects(
            Delete={
                "Objects": [{"Key": key_expected}]
            }
        )

    def test_write_df_to_s3_empty(self):
        """
        Tests the write_df_to_s3 method with an empty DataFrame as input
        """
        return_expected = None
        log_expected = "The dataframe is empty! No file will be written!"
        df_empty = DataFrame()
        key = "key.csv"
        file_format = "csv"
        with self.assertLogs() as logm:
            result = self._s3_bucket_conn.write_df_to_s3(
                df_empty,
                key,
                file_format
            )
            self.assertIn(log_expected, logm.output[0])
        self.assertEqual(return_expected, result)

    def test_write_df_to_s3_csv(self):
        """
        Tests the write_df_to_s3 method if writing csv is successful
        """
        return_expected = True
        df_expected = DataFrame(
            [["A", "B"], ["C", "D"]],
            columns=["col1", "col2"]
        )
        key_expected = "test.csv"
        log_expected = (
            f"Writing file to {self._s3_endpoint_url}"
            f"/{self._s3_bucket_name}/{key_expected}"
        )
        file_format = "csv"
        with self.assertLogs() as logm:
            result = self._s3_bucket_conn.write_df_to_s3(
                df_expected,
                key_expected,
                file_format
            )
            self.assertIn(log_expected, logm.output[0])
        data = (
            self._s3_bucket.Object(key=key_expected).get().get("Body").read()
            .decode("utf-8")
        )
        out_buffer = StringIO(data)
        df_result = read_csv(out_buffer)
        self.assertEqual(return_expected, result)
        self.assertTrue(df_expected.equals(df_result))
        self._s3_bucket.delete_objects(
            Delete={
                "Objects": [{"Key": key_expected}]
            }
        )

    def test_write_df_to_s3_parquet(self):
        """
        Tests the write_df_to_s3 method if writting parquet is successful
        """
        return_expected = True
        df_expected = DataFrame(
            [["A", "B"], ["C", "D"]],
            columns=["col1", "col2"]
        )
        key_expected = "test.parquet"
        log_expected = (
            f"Writing file to {self._s3_endpoint_url}"
            f"/{self._s3_bucket_name}/{key_expected}"
        )
        file_format = "parquet"
        with self.assertLogs() as logm:
            result = self._s3_bucket_conn.write_df_to_s3(
                df_expected,
                key_expected,
                file_format
            )
            self.assertIn(log_expected, logm.output[0])
        data = self._s3_bucket.Object(key=key_expected).get().get("Body").read()
        out_buffer = BytesIO(data)
        df_result = read_parquet(out_buffer)
        self.assertEqual(return_expected, result)
        self.assertTrue(df_expected.equals(df_result))
        self._s3_bucket.delete_objects(
            Delete={
                "Objects": [{"Key": key_expected}]
            }
        )

    def test_write_df_to_s3_wrong_format(self):
        """
        Tests the write_df_to_s3 method if a not supported format is given as
        argument
        """
        df_expected = DataFrame(
            [["A", "B"], ["C", "D"]],
            columns=["col1", "col2"]
        )
        key_expected = "test.parquet"
        format_expected = "wrong_format"
        log_expected = (
            f"The file format {format_expected} is not supported to be written"
            " to s3!"
        )
        exception_expected = WrongFormatException
        with self.assertLogs() as logm:
            with self.assertRaises(exception_expected):
                self._s3_bucket_conn.write_df_to_s3(
                    df_expected,
                    key_expected,
                    format_expected
                )
            self.assertIn(log_expected, logm.output[0])


if __name__ == "__main__":
    main()
