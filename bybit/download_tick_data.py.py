#!/usr/bin/env python3


import urllib.request
import os
import re
import sys
import gzip
import time
import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

REPO_ROOT_DIRECTORY_PATH = os.path.commonpath([os.getcwd(), os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))])
LIBRARIES_DIRECTORY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../libs/python'))

sys.path.append(REPO_ROOT_DIRECTORY_PATH)
sys.path.append(LIBRARIES_DIRECTORY_PATH)

import data_config as dc
import file_utils as fu


BASE_URL     = 'https://public.bybit.com/trading/'
TEMPLATE     = '{symbol}/{symbol}{date}'
EXTENSION    = '.csv.gz'
DATE_PATTERN = r'\d{4}-\d{2}-\d{2}'


def get_csv_details(url):
	dir_response = requests.get(url)
	dir_soup     = BeautifulSoup(dir_response.text, 'html.parser')
	csvgz_urls   = dir_soup.find_all(href=re.compile('.csv.gz$'))
	return [{
		'date' : (hit.group(0) if (hit := re.search(DATE_PATTERN, link.text)) else None),
		'file' : link.text,
		'url'  : f'{url}{link.get("href")}',
	} for link in csvgz_urls]


def get_formatted_csv_file_path(csvgz_file_path):
	csv_date           = re.findall(DATE_PATTERN, csvgz_file_path)[0]
	csv_file_path_base = csvgz_file_path[:csvgz_file_path.rfind(csv_date)]
	return f'{csv_file_path_base}.{csv_date}.csv'


def download_csvgz_file(url, local_path):
	time.sleep(0.1)
	with urllib.request.urlopen(url) as response, open(local_path, 'wb') as out_file:
		data = response.read()
		out_file.write(data)


def unpack_csvgz_to_csv(csvgz_file_path, csv_file_path):
	if fu.file_exists(csv_file_path):
		os.remove(csv_file_path)

	if csvgz_file_path.endswith('.gz'):
		with gzip.open(csvgz_file_path, 'rb') as f_in:
			with open(csv_file_path, 'wb') as f_out:
				f_out.write(f_in.read())
		os.remove(csvgz_file_path)
	else:
		os.rename(csvgz_file_path, csv_file_path)


def get_prev_day(start_date):
	current_date = datetime.strptime(start_date, "%Y-%m-%d")
	while True:
		yield current_date.strftime("%Y-%m-%d")
		current_date -= timedelta(days=1)


def handle_download(symbol_folder_path, filename, url):

	csvgz_file_path = os.path.join(symbol_folder_path, filename)
	csv_file_path   = get_formatted_csv_file_path(csvgz_file_path)
	if not fu.file_exists(csv_file_path):
		download_csvgz_file(url, csvgz_file_path)
		unpack_csvgz_to_csv(csvgz_file_path, csv_file_path)
		print(f"Downloaded '{url}' --> '{csv_file_path}'")
	else:
		print(f"Skipped '{url}'. Already exists: '{csv_file_path}'")


def main():

	default_output_directory_base = os.path.join(REPO_ROOT_DIRECTORY_PATH, dc.BASE_DIRECTORY__DATA, dc.DIRECTORY_NAME__TICK_CSV)

	parser = argparse.ArgumentParser(
		description='ByBit tick data downloader.'
	)
	parser.add_argument('-s', '--symbols',
		nargs = '+',
		type  = str,
		help  = 'Symbols',
	)
	parser.add_argument('-o', '--output_directory_path',
		default = default_output_directory_base,
		type    = str,
		help    = 'Download directory path',
	)
	parser.add_argument('-b', '--backfill',
		action = 'store_true',
		help   = 'Backfill hidden',
	)
	parser.add_argument('-i', '--ignore_downloads_by_directory',
		type = str,
		help = 'Skips downloads by the dates of file names from the specified directory <TICKER>.<YYYY-MM-DD>.<format>.',
	)

	args     = parser.parse_args()
	response = requests.get(BASE_URL)
	soup     = BeautifulSoup(response.text, 'html.parser')
	links    = soup.find_all('a')

	symbols  = [link.get('href')[:-1] for link in links if link.get('href').endswith('/')]
	if args.symbols:
		symbols = [t for t in symbols if t in args.symbols]

	if symbols:
		fu.create_local_folder(args.output_directory_path)

	for symbol_idx, symbol in enumerate(symbols, start=1):

		dates_to_skip = set()
		if args.ignore_downloads_by_directory:

			skipping_directory = os.path.join(args.ignore_downloads_by_directory, symbol)
			if not os.path.isdir(skipping_directory):
				print('Directory not found:', skipping_directory)
				return

			dates_to_skip = {f.split('.')[1] for f in os.listdir(skipping_directory) if f.split('.')[0] == symbol}

		details  = get_csv_details(BASE_URL + symbol + '/')
		online   = len(details)
		offline  = len(dates_to_skip)
		min_date = min({d['date'] for d in details})
		max_date = max({d['date'] for d in details})
		details  = [d for d in details if d['date'] not in dates_to_skip]
		details.sort(key=lambda x: x['date'], reverse=True)

		print(f'[{symbol_idx}/{len(symbols)}] Processing: {symbol}\n\tOnline  : {online} files [{min_date}...{max_date}]\n\tOffline : {offline} files')

		if online == offline:
			print('\tDone.')
			continue

		symbol_folder_path  = os.path.join(args.output_directory_path, symbol)
		fu.create_local_folder(symbol_folder_path)

		for file_idx, detail in enumerate(details, start=1):
			print(f'\t[{file_idx}/{len(details)}] ', end='')
			handle_download(symbol_folder_path, detail['file'], detail['url'])

		if not args.backfill:
			continue

		tolerance = 10
		for hidden_date in get_prev_day(min_date):
			if tolerance <= 0:
				break
			try:
				print(f'\t[{hidden_date}] ', end='')
				filename = '{symbol}{date}{ext}'.format(symbol=symbol, date=hidden_date, ext=EXTENSION)
				url      = f'{BASE_URL}{TEMPLATE}{EXTENSION}'.format(symbol=symbol, date=hidden_date)
				handle_download(symbol_folder_path, filename, url)
			except:
				print('Not Found.')
				tolerance -= 1


if __name__ == '__main__':
	main()
