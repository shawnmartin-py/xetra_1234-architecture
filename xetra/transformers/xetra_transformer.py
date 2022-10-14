"""Xetra ETL Component"""
from datetime import datetime
from logging import getLogger
from typing import NamedTuple

from pandas import concat, DataFrame

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source
    """
    src_first_extract_date: str
    src_columns: list[str]
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_close_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_daily_trade_vol: column name for daily traded volume in target
    trg_col_ch_prev_close: column name for change to previous day's closing price target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_close_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_daily_trade_vol: str
    trg_col_ch_prev_close: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL:
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """

    def __init__(
        self,
        s3_bucket_src: S3BucketConnector,
        s3_bucket_trg: S3BucketConnector,
        meta_key: str,
        src_args: XetraSourceConfig,
        trg_args: XetraTargetConfig
    ):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self._meta_key -> key of meta file
        :param src_args: NamedTuple class with source configuration data
        :param trg_args: NamedTuple class with target configuration data
        """
        self._logger = getLogger(__name__)
        self._s3_bucket_src = s3_bucket_src
        self._s3_bucket_trg = s3_bucket_trg
        self._meta_key = meta_key
        self._src_args = src_args
        self._trg_args = trg_args
        self._extract_date, self._extract_date_list = (
            MetaProcess.return_date_list(
                self._src_args.src_first_extract_date,
                self._meta_key,
                self._s3_bucket_trg
            )
        )
        self._meta_update_list = [
            date for date in self._extract_date_list
            if date >= self._extract_date
        ]

    def extract(self) -> DataFrame:
        """
        Read the source data and concatenates them to one Pandas DataFrame

        :returns:
            Pandas DataFrame with the extracted data
        """
        self._logger.info("Extracting Xetra source files started...")
        files = [
            key for date in self._extract_date_list
            for key in self._s3_bucket_src.list_files_in_prefix(date)
        ]
        if not files:
            data_frame = DataFrame()
        else:
            data_frame = concat(
                [self._s3_bucket_src.read_csv_to_df(file) for file in files],
                ignore_index=True
            )
        self._logger.info("Extracting Xetra source files finished.")
        return data_frame

    def transform_report1(self, data_frame: DataFrame):
        """
        Applies the necessary transformation to create report 1

        :param: data_frame: Pandas DataFrame as Input

        :returns:
            Transformed Pandas DataFrame as Output
        """
        if data_frame.empty:
            self._logger.info(
                "The dataframe is empty. No transformations will be applied."
            )
            return data_frame
        self._logger.info(
            "Applying transformations to Xetra source data for report 1 started..."
        )
        data_frame = data_frame.loc[:, self._src_args.src_columns]
        data_frame.dropna(inplace=True)
        data_frame[self._trg_args.trg_col_op_price] = data_frame.sort_values(
            by=[self._src_args.src_col_time]
        ).groupby(
            [
                self._src_args.src_col_isin,
                self._src_args.src_col_date
            ]
        )[self._src_args.src_col_start_price].transform("first")
        data_frame[self._trg_args.trg_col_close_price] = data_frame.sort_values(
            by=[self._src_args.src_col_time]
        ).groupby(
            [
                self._src_args.src_col_isin,
                self._src_args.src_col_date
            ]
        )[self._src_args.src_col_start_price].transform("last")
        data_frame.rename(
            columns={
                self._src_args.src_col_min_price:
                self._trg_args.trg_col_min_price,
                self._src_args.src_col_max_price:
                self._trg_args.trg_col_max_price,
                self._src_args.src_col_traded_vol:
                self._trg_args.trg_col_daily_trade_vol
            },
            inplace=True
        )
        data_frame = data_frame.groupby(
            [self._src_args.src_col_isin, self._src_args.src_col_date],
            as_index=False
        ).agg(
            {
                self._trg_args.trg_col_op_price: "min",
                self._trg_args.trg_col_close_price: "min",
                self._trg_args.trg_col_min_price: "min",
                self._trg_args.trg_col_max_price: "max",
                self._trg_args.trg_col_daily_trade_vol: "sum"
            }
        )
        data_frame[self._trg_args.trg_col_ch_prev_close] = data_frame.sort_values(
            by=[self._src_args.src_col_date]
        ).groupby(
            [self._src_args.src_col_isin]
        )[self._trg_args.trg_col_op_price].shift(1)
        data_frame[self._trg_args.trg_col_ch_prev_close] = (
            data_frame[self._trg_args.trg_col_op_price] -
            data_frame[self._trg_args.trg_col_ch_prev_close]
        ) / data_frame[self._trg_args.trg_col_ch_prev_close] * 100
        data_frame = data_frame.round(decimals=2)
        data_frame = (
            data_frame[data_frame.Date >= self._extract_date]
            .reset_index(drop=True)
        )
        self._logger.info(
            "Applying transformations to Xetra source data finished..."
        )
        return data_frame

    def load(self, data_frame: DataFrame):
        """
        Saves a Pandas DataFrame to the target

        :param data_frame: Pandas DataFrame as Input
        """
        target_key = (
            f"{self._trg_args.trg_key}"
            f"{datetime.today().strftime(self._trg_args.trg_key_date_format)}."
            f"{self._trg_args.trg_format}"
        )
        self._s3_bucket_trg.write_df_to_s3(
            data_frame,
            target_key,
            self._trg_args.trg_format
        )
        self._logger.info("Xetra target data successfully written.")
        MetaProcess.update_meta_file(
            self._meta_update_list,
            self._meta_key,
            self._s3_bucket_trg
        )
        self._logger.info("Xetra meta file successfully updated.")
        return True

    def etl_report1(self):
        """
        Extract, transform and load to create report 1
        """
        data_frame = self.extract()
        data_frame = self.transform_report1(data_frame)
        self.load(data_frame)
        return True
