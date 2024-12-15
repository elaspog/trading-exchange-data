#!/usr/bin/env python3


import os
import sys
import argparse
import itertools
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


OUTPUT_COLUMN_ORDER = [
	'datetime',
	'timestamp',
	'price',
	'side',
	'size',
	'direction',
]


def write_files(symbol, df, date_info, export_args, output_paths):

	print(f'\tDimensions    : {df.shape}')

	if export_args['csv']:
		output_directory_path = os.path.join(output_paths.get('csv', output_paths.get('_')), f'{symbol}.{date_info}')
		output_file_name      = os.path.join(output_directory_path, f'{symbol}.{date_info}.csv')
		fu.create_local_folder(output_directory_path)
		df.write_csv(output_file_name)
		print(f'\tFile written  : {output_file_name}')

	if export_args['parquet']:
		output_directory_path = os.path.join(output_paths.get('parquet', output_paths.get('_')), f'{symbol}.{date_info}')
		output_file_name      = os.path.join(output_directory_path, f'{symbol}.{date_info}.parquet')
		fu.create_local_folder(output_directory_path)
		df.write_parquet(output_file_name)
		print(f'\tFile written  : {output_file_name}')


def main():

	parser = argparse.ArgumentParser(description='ByBit tick data preprocessor.')
	parser.add_argument('-s', '--symbols',
		nargs    = '+',
		required = True,
		type     = str,
		help     = 'Symbols'
	)
	parser.add_argument('-i', '--input_directory_path',
		type    = str,
		help    = 'Download directory path'
	)
	parser.add_argument('-o', '--output_directory_path',
		type    = str,
		help    = 'Download directory path'
	)
	parser.add_argument('-f', '--formats',
		nargs   = '+',
		default = [],
		type    = au.supported_file_formats,
		help    = f'Import input as one of the supported formats: {au.ALLOWED_FORMATS}'
	)
	parser.add_argument('-e', '--exports',
		nargs   = '+',
		default = [],
		type    = au.supported_file_formats,
		help    = f'Export output as any of the supported formats: {au.ALLOWED_FORMATS}'
	)

	args                                = parser.parse_args()
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
		base_directory_csv     = dc.DIRECTORY_NAME__PREP_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__PREP_PARQUET,
	)

	for idx, process_data in enumerate(itertools.product(args.symbols, [input_directory_path], [output_directory_path])):
		symbol, input_paths, output_paths = process_data

		print(f'\n[{idx+1}/{len(args.symbols)}] Processing {symbol=}.')

		if 'csv' in import_args:
			symbol_directory_path = os.path.join(input_paths.get('csv', input_paths.get('_')), symbol)
			csv_file_paths        = fu.read_file_paths_by_extension(symbol_directory_path, '*.csv')

			if csv_file_paths:
				print(f'\tCSV files     : {len(csv_file_paths)}')
				df                 = u.read_and_concat_dataframes(csv_file_paths, symbol, 'csv', OUTPUT_COLUMN_ORDER)
				min_date, max_date = u.get_interval_info(df)
				date_info          = f'{min_date}_{len(csv_file_paths)}_{max_date}'.replace('-', '')
				write_files(symbol, df, date_info, export_args, output_paths)

			else:
				print(f'\tNo input CSV files were found')
				return

		if 'parquet' in import_args:
			symbol_directory_path = os.path.join(input_paths.get('parquet', input_paths.get('_')), symbol)
			parquet_file_paths    = fu.read_file_paths_by_extension(symbol_directory_path, '*.parquet')

			if parquet_file_paths:
				print(f'\tParquet files : {len(parquet_file_paths)}')
				df                 = u.read_and_concat_dataframes(parquet_file_paths, symbol, 'parquet', OUTPUT_COLUMN_ORDER)
				min_date, max_date = u.get_interval_info(df)
				date_info          = f'{min_date}_{len(parquet_file_paths)}_{max_date}'.replace('-', '')
				write_files(symbol, df, date_info, export_args, output_paths)

			else:
				print(f'\tNo input Parquet files were found')
				return


if __name__ == "__main__":
	try:
		main()
	except e.PreconditionError as e:
		print(e)
		exit(1)
