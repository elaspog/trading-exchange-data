
import os
import glob
import argparse


ALLOWED_FORMATS = ['csv', 'parquet']


def file_exists(local_path):
	return os.path.exists(local_path)


def create_local_folder(folder_path):
	if not os.path.exists(folder_path):
		os.makedirs(folder_path, exist_ok=True)


def read_file_paths_by_extension(folder_path, ticker, extension):
	return glob.glob(os.path.join(folder_path, ticker, extension))


def supported_file_formats(value):
	if value not in ALLOWED_FORMATS:
		raise argparse.ArgumentTypeError(f"The '{value}' is not a supported format. Valid formats are {', '.join(ALLOWED_FORMATS)}")
	return value


def parse_supported_file_format_arguments(
	supported_formats : set[str],
	args              : list,
	default_export    : str = None,
	key_prefix        : str = None,
):
	formats = {
		f: f in args for f in supported_formats
	}

	if default_export:
		if default_export not in formats.keys():
			raise ValueError(f'Possible values: {formats.keys()}')
		if all(value is False for value in formats.values()):
			formats[default_export] = True

	if key_prefix:
		formats = {f'{key_prefix}_{k}': v for k, v in formats.items()}

	return formats
