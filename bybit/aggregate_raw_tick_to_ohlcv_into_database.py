#!/usr/bin/env python3


import os
import sys
import duckdb
import argparse
import itertools
import polars as pl
from decimal import Decimal

REPO_ROOT_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(REPO_ROOT_DIRECTORY_PATH)

import data_config as dc
import utils as u


ALLOWED_TIMEFRAMES = [tf for tf in dc.OHLCV_TIMEFRAMES if u.timeframe_to_seconds(tf) <= 24*60*60]


OUTPUT_COLUMN_ORDER = [
	'datetime',
	'timestamp',
	'price',
	'side',
	'size',
]


def read_dataframe(file_path, input_format, symbol):

	df = None
	if input_format == 'csv':
		df = pl.read_csv(file_path, infer_schema=False)

	elif input_format == 'parquet':
		df = pl.read_parquet(file_path)

	else:
		raise NotImplementedError(f'Unknown format: {input_format}')

	df = df.drop(['trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional'])
	df = df.filter(pl.col('symbol') == symbol)
	df = df.sort('timestamp')
	df = df.reverse()
	df = df.with_columns([
		(pl.col('timestamp').cast(pl.Decimal(None, 9)) * 1_000_000_000).cast(pl.Int64).cast(pl.Datetime('ns')).cast(pl.Utf8).map_elements(lambda x: x[:-3], return_dtype=pl.Utf8).alias('datetime'),
		pl.col('price').map_elements(lambda x: str(Decimal(x).quantize(Decimal(dc.PRICE_PRECISION))), return_dtype=pl.Utf8),
		pl.col('timestamp').map_elements(lambda x: str(Decimal(x).quantize(Decimal(dc.TIMESTAMP_PRECISION))), return_dtype=pl.Utf8),
	])
	df  = df.select(OUTPUT_COLUMN_ORDER)

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


def handle_timeframe_args(args):

	bad_timeframes = [tf for tf in args.timeframes if tf not in ALLOWED_TIMEFRAMES]
	if bad_timeframes:
		raise u.PreconditionError(f'TimeFrames not supported: {bad_timeframes}')

	if not args.timeframes:
		timeframes = ALLOWED_TIMEFRAMES

	return [tf for tf in args.timeframes if tf in ALLOWED_TIMEFRAMES]


def handle_formats_args(formats, default = None):

	bad_formats = [f for f in formats if f not in u.ALLOWED_FORMATS]
	if bad_formats:
		raise u.PreconditionError(f'Formats not supported: {bad_formats}')

	if not formats:
		if default:
			return [default]
		formats = u.ALLOWED_FORMATS

	return [f for f in formats if f in u.ALLOWED_FORMATS]


def main():

	default_output_directory = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__AGGR_DB)

	parser = argparse.ArgumentParser(description='ByBit raw tick data to OHLCV data aggregator')
	parser.add_argument('-s', '--symbols',
		nargs    = '+',
		required = True,
		type     = str,
		help     = 'symbols',
	)
	parser.add_argument('-t', '--timeframes',
		nargs    = '+',
		type     = str,
		help     = f'TimeFrames: {ALLOWED_TIMEFRAMES}',
        default  = ALLOWED_TIMEFRAMES,
	)
	parser.add_argument('-i', '--input_directory_path',
		type    = str,
		help    = 'Input tick directory path'
	)
	parser.add_argument('-o', '--output_directory_path',
		type    = str,
		help    = 'Output OHLCV directory path',
		default = default_output_directory,
	)
	parser.add_argument('-f', '--formats',
		nargs    = '+',
		default  = [],
		type     = u.supported_file_formats,
		help     = f'Import input as one of the supported formats: {u.ALLOWED_FORMATS}',
	)
	parser.add_argument('-e', '--exports',
		nargs   = '+',
		default = [],
		type    = u.supported_file_formats,
		help    = f'Export output as any of the supported formats: {u.ALLOWED_FORMATS}',
	)

	args                                = parser.parse_args()
	timeframes                          = handle_timeframe_args(args)
	input_formats                       = handle_formats_args(args.formats, 'parquet')
	import_args, input_directory_paths  = u.handle_input_args(
		args,
		repo_root_directory    = REPO_ROOT_DIRECTORY_PATH,
		base_data_directory    = dc.BASE_DIRECTORY__DATA,
		base_directory_csv     = dc.DIRECTORY_NAME__TICK_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__TICK_PARQUET,
	)

	process_details = []
	for symbol, input_format in itertools.product(args.symbols, input_formats):

		input_directory      = input_directory_paths.get(input_format, input_directory_paths.get('_'))
		matching_directories = u.list_subdirectories_with_matching_prefix(input_directory, symbol)
		for symbol_interval_input_subdirectory_path in matching_directories:

			process_details.append({
				'input_format' : input_format,
				'symbol'       : symbol,
				'indir_path'   : symbol_interval_input_subdirectory_path,
				'input_files'  : u.read_file_paths_by_extension(symbol_interval_input_subdirectory_path, f'*.{input_format}'),
			})

	u.create_local_folder(args.output_directory_path)

	for process_idx, process_detail in enumerate(process_details, start=1):

		print(f'\n[{process_idx}/{len(process_details)}] Processing: {process_detail["indir_path"]}\n')
		if not process_detail['input_files']:
			print(f'No input file was found for {process_detail["indir_path"]}')
			continue

		db_conn             = duckdb.connect(os.path.join(args.output_directory_path, f'{symbol}.duckdb'))
		timeframes_to_apply = [tf for tf in timeframes if tf != 'tick']

		for aggr_timeframe in timeframes_to_apply:
			db_conn.execute(f"""
				CREATE TABLE IF NOT EXISTS aggr_{aggr_timeframe} (
					datetime TEXT PRIMARY KEY,
					open     TEXT,
					high     TEXT,
					low      TEXT,
					close    TEXT,
					volume   TEXT
				)
			""")

		for file_idx, file_path in enumerate(reversed(process_detail["input_files"]), start=1):
			ticks_df = read_dataframe(file_path, process_detail['input_format'], symbol)

			for aggr_timeframe in timeframes_to_apply:
				aggr_df = aggregate_ohlcv(ticks_df, aggr_timeframe, symbol)

				db_conn.register('aggr_df', aggr_df.to_arrow())
				db_conn.execute(f"""
					INSERT INTO aggr_{aggr_timeframe}
					SELECT * FROM aggr_df
					ON CONFLICT (datetime) DO NOTHING
				""")
				db_conn.unregister('aggr_df')

			print("\033[F\033[K" + f"\t{file_idx}/{len(process_detail['input_files'])}", flush=True)


if __name__ == "__main__":
	try:
		main()
	except u.PreconditionError as e:
		print(e)
		exit(1)
