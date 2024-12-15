
import os
import sys
import polars as pl
from decimal import Decimal

LIBRARIES_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../libs/python'))

sys.path.append(LIBRARIES_DIRECTORY_PATH)

import data_config as dc


def read_polars_dataframe(file_path, file_format):

	df = None
	if file_format == 'csv':
		df = pl.read_csv(file_path, infer_schema=False)

	elif file_format == 'parquet':
		df = pl.read_parquet(file_path, memory_map=True)

	else:
		raise NotImplementedError(f'Unknown format: {file_format}')

	return df


def aggregate_ohlcv(df, interval, symbol):

	precision_price  = Decimal(dc.PRICE_PRECISION)
	precision_volume = Decimal(dc.VOLUME_PRECISION)

	df_aggr = df.sort('datetime')
	df_aggr = df.with_columns(
		pl.col("datetime").str.strptime(pl.Datetime).dt.truncate(interval).alias("datetime"),
        pl.col("price").cast(pl.Float64),
        pl.col("size").cast(pl.Float64),
	)
	df_aggr = df_aggr.group_by("datetime").agg([
		pl.col("price").first().map_elements(lambda x: str(Decimal(x).quantize(precision_price)), return_dtype=pl.Utf8).alias("open"),
		pl.col("price").max().map_elements(lambda x: str(Decimal(x).quantize(precision_price)), return_dtype=pl.Utf8).alias("high"),
		pl.col("price").min().map_elements(lambda x: str(Decimal(x).quantize(precision_price)), return_dtype=pl.Utf8).alias("low"),
		pl.col("price").last().map_elements(lambda x: str(Decimal(x).quantize(precision_price)), return_dtype=pl.Utf8).alias("close"),
		pl.col("size").sum().map_elements(lambda x: str(Decimal(x).quantize(precision_volume)), return_dtype=pl.Utf8).alias("volume"),
	])
	df_aggr = df_aggr.sort('datetime')
	df_aggr = df_aggr.with_columns(
		pl.col("datetime").dt.strftime("%Y-%m-%d %H:%M:%S").alias("datetime")
	)

	return df_aggr
