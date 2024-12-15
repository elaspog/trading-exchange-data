#!/usr/bin/env python3


import os
import sys
import duckdb
import argparse
import itertools
import polars as pl
from decimal import Decimal
from datetime import datetime

REPO_ROOT_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LIBRARIES_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../libs/python'))

sys.path.append(REPO_ROOT_DIRECTORY_PATH)
sys.path.append(LIBRARIES_DIRECTORY_PATH)

import data_config as dc
import file_utils as fu
import arg_utils as au
import errors as e
import domain as d
import utils as u


ALLOWED_TIMEFRAMES = set(['tick'] + [tf for tf in dc.OHLCV_TIMEFRAMES if d.timeframe_to_seconds(tf) <= 24*60*60])


OUTPUT_COLUMN_ORDER = [
	'datetime',
	'price',
	'side',
	'size',
]


def valid_date(s):

	try:
		return datetime.strptime(s, '%Y-%m-%d')

	except ValueError:
		raise argparse.ArgumentTypeError(f"Invalid data: '{s}'. The correct format has: YYYY-MM-DD.")


def read_dataframe(file_path, file_format, symbol):

	df = u.read_polars_dataframe(file_path, file_format)
	df = df.drop(['trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional'])
	df = df.filter(pl.col('symbol') == symbol)
	df = df.sort('timestamp')
	df = df.with_columns([
		(pl.col('timestamp').cast(pl.Decimal(None, 9)) * 1_000_000_000).cast(pl.Int64).cast(pl.Datetime('ns')).cast(pl.Utf8).map_elements(lambda x: x[:-3], return_dtype=pl.Utf8).alias('datetime'),
		pl.col('price').map_elements(lambda x: str(Decimal(x).quantize(Decimal(dc.PRICE_PRECISION))), return_dtype=pl.Utf8),
		pl.col('timestamp').map_elements(lambda x: str(Decimal(x).quantize(Decimal(dc.TIMESTAMP_PRECISION))), return_dtype=pl.Utf8),
	])
	df  = df.select(OUTPUT_COLUMN_ORDER)

	return df


def get_ordered_files_from_date_interval(matching_files, interval_begin, interval_end):

	if interval_begin:
		matching_files = {
			file_date : file_path
			for file_date, file_path in matching_files.items()
			if interval_begin <= datetime.strptime(file_date, '%Y-%m-%d')
		}

	if interval_end:
		matching_files = {
			file_date : file_path
			for file_date, file_path in matching_files.items()
			if datetime.strptime(file_date, '%Y-%m-%d') <= interval_end
		}

	min_date = min(matching_files.keys()) if matching_files else None
	max_date = max(matching_files.keys()) if matching_files else None

	return list(reversed(sorted([file_name for file_name in matching_files.values()]))), min_date, max_date


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
	parser.add_argument('-b', '--interval_begin',
		type    = valid_date,
		default = None,
		help    = 'Start date of interval (YYYY-MM-DD)',
	)
	parser.add_argument('-e', '--interval_end',
		type    = valid_date,
		default = None,
		help    = 'End date of interval (YYYY-MM-DD)',
	)
	parser.add_argument('-i', '--input_directory_path',
		type    = str,
		help    = 'Input tick directory path',
	)
	parser.add_argument('-o', '--output_directory_path',
		type    = str,
		help    = 'Output OHLCV directory path',
		default = default_output_directory,
	)
	parser.add_argument('-f', '--formats',
		nargs    = '+',
		default  = [],
		type     = au.supported_file_formats,
		help     = f'Import input as one of the supported formats: {au.ALLOWED_FORMATS}',
	)

	args                                = parser.parse_args()
	timeframes                          = au.handle_timeframe_args(args, ALLOWED_TIMEFRAMES)
	input_formats                       = au.handle_formats_args(args.formats, 'parquet')
	import_args, input_directory_paths  = au.handle_input_args(
		args,
		repo_root_directory    = REPO_ROOT_DIRECTORY_PATH,
		base_data_directory    = dc.BASE_DIRECTORY__DATA,
		base_directory_csv     = dc.DIRECTORY_NAME__TICK_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__TICK_PARQUET,
	)

	processing_details = []
	for symbol, input_format in itertools.product(args.symbols, input_formats):

		input_directory      = input_directory_paths.get(input_format, input_directory_paths.get('_'))
		matching_directories = fu.list_subdirectories_with_matching_prefix(input_directory, symbol)
		for symbol_input_subdirectory_path in matching_directories:

			input_files = {
				(file_date := os.path.basename(item).split('.')[1]) : item
				for item in fu.read_file_paths_by_extension(symbol_input_subdirectory_path, f'*.{input_format}')
			}

			files_with_matching_date, min_date, max_date = get_ordered_files_from_date_interval(input_files, args.interval_begin, args.interval_end)

			processing_details.append({
				'input_format' : input_format,
				'symbol'       : symbol,
				'indir_path'   : symbol_input_subdirectory_path,
				'input_files'  : files_with_matching_date,
				'db_file_name' : f'{symbol}.{min_date}_{len(files_with_matching_date)}_{max_date}.duckdb'.replace('-', '')
			})

	fu.create_local_folder(args.output_directory_path)

	for process_idx, process_detail in enumerate(processing_details, start=1):

		print(f'\n[{process_idx}/{len(processing_details)}] Processing: {process_detail["indir_path"]}')
		if not process_detail['input_files']:
			print(f'\tNo input file was found for symbol \'{symbol}\' in interval {str(args.interval_begin)[:-9]}...{str(args.interval_end)[:-9]}')
			continue

		ohlcv_names = [tf for tf in timeframes if tf != 'tick']
		db_conn     = duckdb.connect(os.path.join(args.output_directory_path, process_detail['db_file_name']))

		if 'tick' in timeframes:
			db_conn.execute(f"""
				CREATE TABLE IF NOT EXISTS tick (
					datetime TEXT,
					price    TEXT,
					size     TEXT,
					side     TEXT
				);
			""")

		for aggr_timeframe in ohlcv_names:
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

		print()
		for file_idx, file_path in enumerate(process_detail['input_files'], start=1):
			ticks_df = read_dataframe(file_path, process_detail['input_format'], symbol)

			if 'tick' in timeframes:
				db_conn.register('ticks_df', ticks_df.to_arrow())
				db_conn.execute(f"""
					INSERT INTO tick (datetime, open, high, low, close, volume)
					SELECT datetime, open, high, low, close, volume
					FROM ticks_df
				""")
				db_conn.unregister('ticks_df')

			for aggr_timeframe in ohlcv_names:
				aggr_df = u.aggregate_ohlcv(ticks_df, aggr_timeframe, symbol)
				db_conn.execute(f"""
					INSERT INTO aggr_{aggr_timeframe} (datetime, open, high, low, close, volume)
					SELECT datetime, open, high, low, close, volume
					FROM aggr_df
					ON CONFLICT (datetime) DO NOTHING
				""")
				db_conn.unregister('aggr_df')

			print("\033[F\033[K" + f"\t{file_idx}/{len(process_detail['input_files'])}", flush=True)

	print()


if __name__ == "__main__":
	try:
		main()
	except e.PreconditionError as e:
		print(e)
		exit(1)
