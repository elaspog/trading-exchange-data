#!/usr/bin/env python3


import os
import sys
import glob
import argparse
import polars as pl
from decimal import Decimal

REPO_ROOT_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(REPO_ROOT_DIRECTORY_PATH)

import data_config as dc
import utils as u


def main():

	default_input_directory_base  = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__TICK_CSV)
	default_output_directory_base = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__TICK_PARQUET)

	parser = argparse.ArgumentParser(description='ByBit tick data converter (from CSV to Parquet).')
	parser.add_argument('-t', '--tickers',
		nargs    = '+',
		required = True,
		type     = str,
		help     = 'Tickers'
	)
	parser.add_argument('-i', '--input_directory_path',
		default = default_input_directory_base,
		type    = str,
		help    = 'Unpacked tick data CSV directory path'
	)
	parser.add_argument('-o', '--output_directory_path',
		default = default_output_directory_base,
		type    = str,
		help    = 'Output tick data Parquet directory path'
	)

	args = parser.parse_args()

	print(f'input directory  : {args.input_directory_path}')
	print(f'output directory : {args.output_directory_path}')
	u.create_local_folder(args.output_directory_path)

	for ticker_idx, ticker in enumerate(args.tickers):
		print(f'[{ticker_idx+1}/{len(args.tickers)}] Processing {ticker=}.')

		csv_file_paths = u.read_file_paths_by_extension(args.input_directory_path, ticker, '*.csv')
		print(f'\tFound files: {len(csv_file_paths)}')

		for csv_file_path in csv_file_paths:
			parquet_directory_path = os.path.join(args.output_directory_path, ticker)
			parquet_file_path      = os.path.join(parquet_directory_path, os.path.basename(csv_file_path).replace('.csv', '.parquet'))

			u.create_local_folder(parquet_directory_path)

			df = pl.read_csv(csv_file_path, infer_schema=False)
			df.write_parquet(parquet_file_path)

			print(f'\tFile written: {parquet_file_path}')

main()
