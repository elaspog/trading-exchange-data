#!/usr/bin/env python3


import os
import sys
import argparse
import polars as pl
from decimal import Decimal

REPO_ROOT_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LIBRARIES_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../libs/python'))

sys.path.append(REPO_ROOT_DIRECTORY_PATH)
sys.path.append(LIBRARIES_DIRECTORY_PATH)

import data_config as dc
import file_utils as fu
import arg_utils as au
import errors as e
import utils as u


ALLOWED_TIMEFRAMES  = ['tick'] + dc.OHLCV_TIMEFRAMES


def main():

	parser = argparse.ArgumentParser(description='ByBit tick data to OHLCV transformer')
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

	parser.add_argument('-f', '--formats',
		nargs    = '+',
		default  = [],
		type     = au.supported_file_formats,
		help     = f'Import input as one of the supported formats: {au.ALLOWED_FORMATS}'
	)
	parser.add_argument('-e', '--exports',
		nargs   = '+',
		default = [],
		type    = au.supported_file_formats,
		help    = f'Export output as any of the supported formats: {au.ALLOWED_FORMATS}'
	)
	parser.add_argument('-i', '--input_directory_path',
		type    = str,
		help    = 'Input tick directory path'
	)
	parser.add_argument('-o', '--output_directory_path',
		type    = str,
		help    = 'Output OHLCV directory path'
	)
	args = parser.parse_args()
	import_args, input_directory_path   = au.handle_input_args(
		args,
		repo_root_directory    = REPO_ROOT_DIRECTORY_PATH,
		base_data_directory    = dc.BASE_DIRECTORY__DATA,
		base_directory_csv     = dc.DIRECTORY_NAME__TICK_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__TICK_PARQUET,
	)
	export_args, output_directory_path  = au.handle_output_args(
		args,
		repo_root_directory    = REPO_ROOT_DIRECTORY_PATH,
		base_data_directory    = dc.BASE_DIRECTORY__DATA,
		base_directory_csv     = dc.DIRECTORY_NAME__AGGR_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__AGGR_PARQUET,
	)

	bad_timeframes = [tf for tf in args.timeframes if tf not in ALLOWED_TIMEFRAMES]
	if bad_timeframes:
		print(f'TimeFrames not supported: {bad_timeframes}')
		return

	if not args.timeframes:
		timeframes = ALLOWED_TIMEFRAMES
	timeframes     = [tf for tf in args.timeframes if tf in ALLOWED_TIMEFRAMES]

	if not args.exports:
		exports = au.ALLOWED_FORMATS
	exports     = [tf for tf in args.exports if tf in au.ALLOWED_FORMATS]

	input_format = import_args[0]
	if not fu.file_exists(input_directory_path[input_format]):
		print(f'Missing input directory: {input_directory_path[input_format]}')
		return

	for symbol_idx, symbol in enumerate(args.symbols):

		symbol_dir_path = os.path.join(input_directory_path[input_format], symbol)
		print(f'[{symbol_idx+1}/{len(args.symbols)}] Processing {symbol_dir_path}')

		input_folder_path = os.path.join(input_directory_path[input_format], symbol)
		input_files       = fu.read_file_paths_by_extension(input_folder_path, f'*.{input_format}')
		if len(input_files) == 0:
			continue

		df_tick            = u.read_and_concat_dataframes(input_files, symbol, input_format)
		min_date, max_date = u.get_interval_info(df_tick)
		date_info          = f'{min_date}_{len(input_files)}_{max_date}'.replace('-', '')
		print(f'\tDimensions of tick: {df_tick.shape}')

		results = []
		if 'tick' in timeframes:
			results.append({
				'timeframe' : 'tick',
				'dataframe' : df_tick,
				'file_name' : f'{symbol}.{date_info}.tick',
			})
		for aggr_timeframe in [tf for tf in timeframes if tf != 'tick']:
			aggregation = {
				'timeframe' : aggr_timeframe,
				'dataframe' : u.aggregate_ohlcv(df_tick, aggr_timeframe, symbol),
				'file_name' : f'{symbol}.{date_info}.{aggr_timeframe}',
			}
			print(f'\tDimensions of {aggr_timeframe:>4}: {aggregation["dataframe"].shape}')
			results.append(aggregation)

		if 'csv' in exports:
			csv_directory_path = os.path.join(output_directory_path['csv'], f'{symbol}.{date_info}')
			fu.create_local_folder(csv_directory_path)
			for result in results:
				file_name = f"{result['file_name']}.csv"
				file_path = os.path.join(csv_directory_path, file_name)
				result['dataframe'].write_csv(file_path)
				print(f'\tFile written  {result["timeframe"]:>4}: {file_path}')

		if 'parquet' in exports:
			parquet_directory_path = os.path.join(output_directory_path['parquet'], f'{symbol}.{date_info}')
			fu.create_local_folder(parquet_directory_path)
			for result in results:
				file_name = f"{result['file_name']}.parquet"
				file_path = os.path.join(parquet_directory_path, file_name)
				result['dataframe'].write_parquet(file_path)
				print(f'\tFile written  {result["timeframe"]:>4}: {file_path}')


if __name__ == '__main__':
	try:
		main()
	except e.PreconditionError as e:
		print(e)
		exit(1)
