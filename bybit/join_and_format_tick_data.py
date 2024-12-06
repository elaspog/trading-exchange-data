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


OUTPUT_COLUMN_ORDER = [
	'datetime',
	'timestamp',
	'price',
	'side',
	'size',
	'direction',
]


def process_dataframes(file_paths, ticker, file_format):

	dfs = []
	for idx, file_path in enumerate(file_paths):

		if file_format == 'csv':
			df = pl.read_csv(file_path, infer_schema=False)
		elif file_format == 'parquet':
			df = pl.read_parquet(file_path)
		else:
			raise NotImplementedError(f'Unknown format: {file_format}')

		df = df.drop(['trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional'])
		df = df.filter(pl.col('symbol') == ticker)
		df = df.reverse()
		dfs.append(df)

	data_df = pl.concat(dfs, how='vertical')
	data_df = data_df.sort('timestamp')

	aggregated_df = pl.concat([
		data_df.head(1).clone(),
		data_df.tail(1).clone(),
	], how='vertical')

	data_df = data_df.with_columns([
		(pl.col('timestamp').cast(pl.Decimal(None, 9)) * 1_000_000_000).cast(pl.Int64).cast(pl.Datetime('ns')).cast(pl.Utf8).map_elements(lambda x: x[:-3], return_dtype=pl.Utf8).alias('datetime'),
		pl.col('price').map_elements(lambda x: str(Decimal(x).quantize(Decimal(dc.PRICE_PRECISION))), return_dtype=pl.Utf8),
		pl.col('timestamp').map_elements(lambda x: str(Decimal(x).quantize(Decimal(dc.TIMESTAMP_PRECISION))), return_dtype=pl.Utf8),
		pl.col('tickDirection').alias('direction'),
	])
	aggregated_df = aggregated_df.with_columns([
		(pl.col('timestamp').cast(pl.Decimal(None, 9)) * 1_000_000_000).cast(pl.Int64).cast(pl.Datetime('ns')).cast(pl.Utf8).map_elements(lambda x: x[:10], return_dtype=pl.Utf8).alias('date'),
	])

	data_df  = data_df.select(OUTPUT_COLUMN_ORDER)
	min_date = aggregated_df.select(pl.col('date').first()).get_column('date').item()
	max_date = aggregated_df.select(pl.col('date').last()).get_column('date').item()

	return data_df, min_date, max_date


def write_files(ticker, df, date_info, export_args, output_paths):

	print(f'\tDimensions    : {df.shape}')

	if export_args['csv']:
		output_directory_path = os.path.join(output_paths.get('csv', output_paths.get('_')), f'{ticker}.{date_info}')
		output_file_name      = os.path.join(output_directory_path, f'{ticker}.{date_info}.csv')
		u.create_local_folder(output_directory_path)
		df.write_csv(output_file_name)
		print(f'\tFile written  : {output_file_name}')

	if export_args['parquet']:
		output_directory_path = os.path.join(output_paths.get('parquet', output_paths.get('_')), f'{ticker}.{date_info}')
		output_file_name      = os.path.join(output_directory_path, f'{ticker}.{date_info}.parquet')
		u.create_local_folder(output_directory_path)
		df.write_parquet(output_file_name)
		print(f'\tFile written  : {output_file_name}')


def main():

	default_output_directory_base = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__PREP_CSV)
	default_output_directory_base = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__PREP_PARQUET)

	parser = argparse.ArgumentParser(description='ByBit tick data preprocessor.')
	parser.add_argument('-t', '--tickers',
		nargs    = '+',
		required = True,
		type     = str,
		help     = 'Tickers'
	)
	parser.add_argument('-i', '--input_directory_path',
		type    = str,
		help    = 'Download directory path'
	)
	parser.add_argument('-o', '--output_directory_path',
		type    = str,
		help    = 'Download directory path'
	)
	parser.add_argument('-f', '--filter',
		nargs   = '+',
		default = [],
		type    = u.supported_file_formats,
		help    = f'Import output as any of supported formats: {u.ALLOWED_FORMATS}'
	)
	parser.add_argument('-e', '--exports',
		nargs   = '+',
		default = [],
		type    = u.supported_file_formats,
		help    = f'Export output as any of supported formats: {u.ALLOWED_FORMATS}'
	)

	print()
	args        = parser.parse_args()
	import_args = u.parse_supported_file_format_arguments(u.ALLOWED_FORMATS, args.filter, 'parquet')
	export_args = u.parse_supported_file_format_arguments(u.ALLOWED_FORMATS, args.exports, 'parquet')

	input_directory_paths = {}
	if args.input_directory_path:
		input_directory_paths['_']       = args.input_directory_path
	else:
		input_directory_paths['csv']     = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__TICK_CSV)
		input_directory_paths['parquet'] = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__TICK_PARQUET)

	output_directory_paths = {}
	if args.output_directory_path:
		output_directory_paths['_']           = args.output_directory_path
	else:
		if export_args['csv']:
			output_directory_paths['csv']     = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__PREP_CSV)
		if export_args['parquet']:
			output_directory_paths['parquet'] = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__PREP_PARQUET)

	import_args = [extension for extension, is_allowed in import_args.items() if is_allowed]
	if len(import_args) > 1:
		print(f'Only one import format is allowed: {" or ".join(u.ALLOWED_FORMATS)}')
		return

	input_directory_paths     = {k: v for k, v in input_directory_paths.items() if k in import_args or k == '_'}
	missing_input_directories = [input_directory_path for extension, input_directory_path in input_directory_paths.items() if not u.file_exists(input_directory_path)]
	if missing_input_directories:
		print(f'Input directories are missing:\n\t{"\n\t".join(missing_input_directories)}')
		return

	print(f'input directory : \n\t{"\n\t".join(input_directory_paths.values())}')
	print(f'output directory: \n\t{"\n\t".join(output_directory_paths.values())}')

	for idx, process_data in enumerate(itertools.product(args.tickers, [input_directory_paths], [output_directory_paths])):
		ticker, input_paths, output_paths = process_data

		print(f'\n[{idx+1}/{len(args.tickers)}] Processing {ticker=}.')

		if 'csv' in import_args:
			csv_file_paths     = u.read_file_paths_by_extension(input_paths.get('csv', input_paths.get('_')), ticker, '*.csv')

			if csv_file_paths:
				print(f'\tCSV files     : {len(csv_file_paths)}')
				df, min_date, max_date = process_dataframes(csv_file_paths, ticker, 'csv')
				date_info              = f'{min_date}_{len(csv_file_paths)}_{max_date}'.replace('-', '')
				write_files(ticker, df, date_info, export_args, output_paths)

			else:
				print(f'\tNo input CSV files were found')
				return

		if 'parquet' in import_args:
			parquet_file_paths = u.read_file_paths_by_extension(input_paths.get('parquet', input_paths.get('_')), ticker, '*.parquet')

			if parquet_file_paths:
				print(f'\tParquet files : {len(parquet_file_paths)}')
				df, min_date, max_date = process_dataframes(parquet_file_paths, ticker, 'parquet')
				date_info              = f'{min_date}_{len(parquet_file_paths)}_{max_date}'.replace('-', '')
				write_files(ticker, df, date_info, export_args, output_paths)

			else:
				print(f'\tNo input Parquet files were found')
				return


main()
