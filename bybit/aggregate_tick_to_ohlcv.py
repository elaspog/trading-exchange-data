#!/usr/bin/env python3


import os
import sys
import argparse
import itertools
import polars as pl
from decimal import Decimal

REPO_ROOT_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(REPO_ROOT_DIRECTORY_PATH)

import data_config as dc
import utils as u


ALLOWED_TIMEFRAMES  = ['tick'] + dc.OHLCV_TIMEFRAMES


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


def read_dataframe(process_detail):

	df = None
	if process_detail['input_format'] == 'csv':
		df = pl.read_csv(process_detail['input_file'], infer_schema=False)

	elif process_detail['input_format'] == 'parquet':
		df = pl.read_parquet(process_detail['input_file'])

	else:
		raise NotImplementedError(f'Unknown format: {process_detail['input_format']}')

	return df


def main():

	parser = argparse.ArgumentParser(description='ByBit tick data to OHLCV data aggregator')
	parser.add_argument('-s', '--symbols',
		nargs    = '+',
		required = True,
		type     = str,
		help     = 'symbols'
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
		help    = 'Output OHLCV directory path'
	)
	parser.add_argument('-f', '--formats',
		nargs    = '+',
		default  = [],
		type     = u.supported_file_formats,
		help     = f'Import input as one of the supported formats: {u.ALLOWED_FORMATS}'
	)
	parser.add_argument('-e', '--exports',
		nargs   = '+',
		default = [],
		type    = u.supported_file_formats,
		help    = f'Export output as any of the supported formats: {u.ALLOWED_FORMATS}'
	)

	args                                = parser.parse_args()
	timeframes                          = handle_timeframe_args(args)
	input_formats                       = handle_formats_args(args.formats, 'parquet')
	output_formats                      = handle_formats_args(args.exports, 'parquet')
	import_args, input_directory_paths  = u.handle_input_args(
		args,
		repo_root_directory    = REPO_ROOT_DIRECTORY_PATH,
		base_data_directory    = dc.BASE_DIRECTORY__DATA,
		base_directory_csv     = dc.DIRECTORY_NAME__PREP_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__PREP_PARQUET,
	)
	export_args, output_directory_path  = u.handle_output_args(
		args,
		repo_root_directory    = REPO_ROOT_DIRECTORY_PATH,
		base_data_directory    = dc.BASE_DIRECTORY__DATA,
		base_directory_csv     = dc.DIRECTORY_NAME__AGGR_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__AGGR_PARQUET,
	)

	process_details = []
	for symbol, input_format in itertools.product(args.symbols, input_formats):

		input_directory      = input_directory_paths.get(input_format, input_directory_paths.get('_'))
		matching_directories = u.list_subdirectories_with_matching_prefix(input_directory, f'{symbol}.')
		for symbol_interval_input_subdirectory_path in matching_directories:

			for output_format in output_formats:

				symbol_interval                          = os.path.basename(os.path.dirname(symbol_interval_input_subdirectory_path))
				symbol_interval_output_subdirectory_path = os.path.join(output_directory_path[output_format], symbol_interval)
				input_files                              = u.read_file_paths_by_extension(symbol_interval_input_subdirectory_path, f'{symbol_interval}.{input_format}')

				process_details.append({
					'input_format' : input_format,
					'output_format': output_format,
					'symbol'       : symbol,
					'subdir_name'  : symbol_interval,
					'indir_path'   : symbol_interval_input_subdirectory_path,
					'outdir_path'  : symbol_interval_output_subdirectory_path,
					'input_file'   : input_files[0] if len(input_files) == 1 else None,
				})

	for process_idx, process_detail in enumerate(process_details, start=1):

		print(f'\n[{process_idx}/{len(process_details)}] Processing: {process_detail["indir_path"]}')
		if process_detail["input_file"] is None:
			print(f'No input file was found for {process_detail["indir_path"]}')
			continue

		u.create_local_folder(process_detail['outdir_path'])

		ticks_df = read_dataframe(process_detail)
		for aggr_timeframe in [tf for tf in timeframes if tf != 'tick']:

			aggr_df = aggregate_ohlcv(ticks_df, aggr_timeframe, symbol)
			print(f'\tDimensions of {aggr_timeframe:>4}: {aggr_df.shape}')

			file_name_base = os.path.join(process_detail['outdir_path'], f'{process_detail["subdir_name"]}.{aggr_timeframe}')

			if 'csv' == process_detail['output_format']:
				file_name = f'{file_name_base}.csv'
				aggr_df.write_csv(file_name)
				print(f'\tFile written  {aggr_timeframe:>4}: {file_name}')

			if 'parquet' == process_detail['output_format']:
				file_name = f'{file_name_base}.parquet'
				aggr_df.write_parquet(file_name)
				print(f'\tFile written  {aggr_timeframe:>4}: {file_name}')


if __name__ == "__main__":
	try:
		main()
	except u.PreconditionError as e:
		print(e)
		exit(1)
