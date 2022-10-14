"""Connector and methods accessing S3"""
from io import BytesIO, StringIO
from logging import getLogger

from boto3 import Session
from pandas import DataFrame, read_csv

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


_CSV = S3FileTypes.CSV
_PARQUET = S3FileTypes.PARQUET


class S3BucketConnector:
    """
    Class for interacting with S3 Buckets
    """

    def __init__(self, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = getLogger(__name__)
        self._endpoint_url=endpoint_url
        self.session = Session()
        self._s3 = self.session.resource(
            service_name="s3",
            endpoint_url=endpoint_url
        )
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str) -> list[str]:
        """
        listing all files with a prefix on the S3 bucket

        :param prefix: prefix on the S3 bucket that should be filtered with

        returns:
            list of all the file names containing the prefix in the key
        """
        return [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]

    def read_csv_to_df(self,
        key: str,
        encoding = "utf-8",
        separator = ","
        ) -> DataFrame:
        """
        reading a csv file from the S3 bucket and returning a dataframe

        :param key: key of the file that should be read
        :encoding: encoding of the date inside the csv file
        :separator: separator of the csv file

        returns:
            Pandas DataFrame containing the data of the csv file
        """
        self._logger.info(
            "Reading file %s/%s/%s",
            self._endpoint_url,
            self._bucket.name,
            key
        )
        csv_obj = (
            self._bucket.Object(key=key).get().get("Body").read()
            .decode(encoding)
        )
        data = StringIO(csv_obj)
        return read_csv(data, sep=separator)

    def write_df_to_s3(self, data_frame: DataFrame, key: str, file_format: str):
        """
        writing a Pandas DataFrame to S3
        supported formats: .csv, .parquet

        :df: Pandas DataFrame that should be written
        :key: target key of the saved file
        :file_format: format of the saved file
        """
        if data_frame.empty:
            self._logger.info(
                "The dataframe is empty! No file will be written!"
            )
            return None
        match file_format:
            case _CSV.value:
                out_buffer = StringIO()
                data_frame.to_csv(out_buffer, index=False)
            case _PARQUET.value:
                out_buffer = BytesIO()
                data_frame.to_parquet(out_buffer, index=False)
            case _:
                self._logger.info(
                    "The file format %s is not supported to be"
                    " written to s3!", file_format
                )
                raise WrongFormatException
        return self.__put_object(out_buffer, key)

    def __put_object(self, out_buffer: StringIO | BytesIO, key: str) -> bool:
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info(
            "Writing file to %s/%s/%s",
            self._endpoint_url,
            self._bucket.name,
            key
        )
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
