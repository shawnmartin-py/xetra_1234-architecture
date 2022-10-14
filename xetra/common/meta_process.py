"""
Methods for processing the meta file
"""
from collections import Counter
from datetime import date, datetime, timedelta

from pandas import concat, DataFrame, to_datetime

from xetra.common.constants import MetaProcessFormat
from xetra.common.s3 import S3BucketConnector
from xetra.common.custom_exceptions import WrongMetaFileException


_META_DATE_FORMAT = MetaProcessFormat.META_DATE_FORMAT
_META_SOURCE_DATE_COL = MetaProcessFormat.META_SOURCE_DATE_COL
_META_PROCESS_COL = MetaProcessFormat.META_PROCESS_COL
_META_PROCESS_DATE_FORMAT = MetaProcessFormat.META_PROCESS_DATE_FORMAT
_META_FILE_FORMAT = MetaProcessFormat.META_FILE_FORMAT

class MetaProcess:
    """
    class for working with the meta file
    """

    @staticmethod
    def update_meta_file(
        extract_date_list: list[str],
        meta_key: str,
        s3_bucket_meta: S3BucketConnector
    ) -> bool:
        """
        Updating the meta file with the processed Xetra dates and todays date
        as processed date

        :param: extract_date_list -> a list of dates that are extracted from the
        source
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta
        file
        """
        df_new = DataFrame(
            columns=[_META_SOURCE_DATE_COL.value, _META_PROCESS_COL.value]
        )
        df_new[_META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[_META_PROCESS_COL.value] = datetime.today().strftime(
            _META_PROCESS_DATE_FORMAT.value
        )
        try:
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if Counter(df_old.columns) != Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = concat([df_old, df_new])
        except s3_bucket_meta.session.client("s3").exceptions.NoSuchKey:
            df_all = df_new
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, _META_FILE_FORMAT.value)
        return True

    @staticmethod
    def return_date_list(
        first_date: str,
        meta_key: str,
        s3_bucket_meta: S3BucketConnector
    ):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file

        :param: first_date -> the earliest date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the
        meta file

        returns:
            tuple:
                - first date that should be processed
                - list of all dates from min_date till today
        """
        start = (
            datetime.strptime(first_date, _META_DATE_FORMAT.value).date() -
            timedelta(days=1)
        )
        today = datetime.today().date()
        try:
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)
            dates = [
                start + timedelta(days=x)
                for x in range(0, (today - start).days + 1)
            ]
            src_dates = set(
                to_datetime(df_meta[_META_SOURCE_DATE_COL.value]).dt.date
            )
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing:
                min_date = min(dates_missing) - timedelta(days=1)
                return_min_date = (
                    (min_date + timedelta(days=1))
                    .strftime(_META_DATE_FORMAT.value)
                )
                return_dates = [
                    date.strftime(_META_DATE_FORMAT.value)
                    for date in dates if date >= min_date
                ]
            else:
                return_dates = []
                return_min_date = (
                    datetime(2200, 1, 1).date()
                    .strftime(_META_DATE_FORMAT.value)
                )
        except s3_bucket_meta.session.client("s3").exceptions.NoSuchKey:
            return_min_date = first_date
            return_dates = [
                (start + timedelta(days=x)).strftime(_META_DATE_FORMAT.value)
                for x in range(0, (today - start).days + 1)
            ]
        return return_min_date, return_dates
