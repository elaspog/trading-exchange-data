#!/usr/bin/env python3


import os
import sys
import glob
import duckdb
import argparse
import numpy as np
import polars as pl

REPO_ROOT_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(REPO_ROOT_DIRECTORY_PATH)

import data_config as dc
import utils as u


ALLOWED_TIMEFRAMES = set(['tick'] + [tf for tf in dc.OHLCV_TIMEFRAMES if u.timeframe_to_seconds(tf) <= 24*60*60])


def locate_ohlcv_databases(input_directory_path, database_prefixes):

	ohlcv_dir_paths = []
	for prefix in database_prefixes:
		matching_ohlcv_directories = [d for d in glob.glob(os.path.join(input_directory_path, prefix))]
		ohlcv_dir_paths.extend(matching_ohlcv_directories)

	return ohlcv_dir_paths


def get_table_names_to_query(timeframes):

	table_names_to_query = {}
	for timeframe in timeframes:
		table_name = None
		if timeframe != 'tick':
			table_name = f'aggr_{timeframe}'
		else:
			table_name = 'tick'
		table_names_to_query[timeframe] = table_name

	return table_names_to_query


def main():

	input_directory_path = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__AGGR_DB)

	parser = argparse.ArgumentParser(description='DuckDB OHLCV to CSV or Parquet OHLCV')
	parser.add_argument('-p', '--database_prefixes',
		nargs    = '+',
		required = True,
		type     = str,
		help     = 'Database Prefix'
	)
	parser.add_argument('-t', '--timeframes',
		nargs    = '+',
		type     = str,
		help     = f'TimeFrames: {ALLOWED_TIMEFRAMES}',
        default  = ALLOWED_TIMEFRAMES,
	)
	parser.add_argument('-i', '--input_directory_path',
		default = input_directory_path,
		type    = str,
		help    = 'Input OHLCV database directory path'
	)
	parser.add_argument('-o', '--output_directory_path',
		type    = str,
		help    = 'Output OHLCV file directory path'
	)
	parser.add_argument('-e', '--exports',
		nargs   = '+',
		default = [],
		type    = u.supported_file_formats,
		help    = f'Export output as any of the supported formats: {u.ALLOWED_FORMATS}'
	)

	args                               = parser.parse_args()
	output_formats                     = u.handle_formats_args(args.exports, 'parquet')
	export_args, output_directory_path = u.handle_output_args(
		args,
		repo_root_directory    = REPO_ROOT_DIRECTORY_PATH,
		base_data_directory    = dc.BASE_DIRECTORY__DATA,
		base_directory_csv     = dc.DIRECTORY_NAME__AGGR_CSV,
		base_directory_parquet = dc.DIRECTORY_NAME__AGGR_PARQUET,
	)

	if not u.file_exists(args.input_directory_path):
		print(f'Missing input directory: {args.input_directory_path}')
		return

	if unsupported_timeframes := set(args.timeframes) - ALLOWED_TIMEFRAMES:
		print(f'Unsupported timeframes: {unsupported_timeframes}')
		return

	database_files = locate_ohlcv_databases(args.input_directory_path, args.database_prefixes)
	if not database_files:
		print(f'No OHLCV database was found.')
		return

	table_names_to_query = get_table_names_to_query(args.timeframes)

	valid_db_files_to_process = []
	for database_file in database_files:

		tables_in_db = set([x[0] for x in duckdb.connect(database_file).execute(f"""
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = 'main'
		""").fetchall()])

		if unsupported_timeframes := set(table_names_to_query.values()) - tables_in_db:
			print(f'Missing timeframes {unsupported_timeframes} in database {database_file}')
			return

		file_name_parts = os.path.basename(database_file).split('.')
		if len(file_name_parts) != 3:
			print(f'Invalid database name format: {database_file}')
			return

		valid_db_files_to_process.append({
			'database_file'      : database_file,
			'output_file_prefix' : '.'.join(file_name_parts[:2]).upper(),
		})

	for export_format, is_allowed in export_args.items():
		if is_allowed:
			u.create_local_folder(output_directory_path[export_format])

	for idx, db in enumerate(valid_db_files_to_process, start=1):
		print(f'\n[{idx}/{len(valid_db_files_to_process)}] Processing {db["database_file"]}')

		for timeframe, table_name in table_names_to_query.items():

			tf_data = duckdb.connect(db['database_file']).execute(f"""FROM {table_name}""").pl().sort('datetime')
			infix   = db["output_file_prefix"]

			if export_args['csv']:
				dir_path_csv     = os.path.join(output_directory_path.get('csv', output_directory_path.get('_')), infix)
				u.create_local_folder(dir_path_csv)
				output_file_path = os.path.join(dir_path_csv, f'{infix}.{timeframe}.csv')
				tf_data.write_csv(output_file_path)
				print(f'\t{timeframe:<4} : {output_file_path}')

			if export_args['parquet']:
				dir_path_parquet = os.path.join(output_directory_path.get('parquet', output_directory_path.get('_')), infix)
				u.create_local_folder(dir_path_parquet)
				output_file_path = os.path.join(dir_path_parquet, f'{infix}.{timeframe}.parquet')
				tf_data.write_parquet(output_file_path)
				print(f'\t{timeframe:<4} : {output_file_path}')

if __name__ == '__main__':
	main()
