
import os
import glob
import argparse


ALLOWED_FORMATS = ['csv', 'parquet']


class PreconditionError(Exception):

	def __init__(self, message: str):
		super().__init__(message)
		self.message = message

	def __str__(self):
		return self.message


def file_exists(local_path):
	return os.path.exists(local_path)


def create_local_folder(directory_path):
	if not os.path.exists(directory_path):
		os.makedirs(directory_path, exist_ok=True)


def read_file_paths_by_extension(directory_path, extension):
	return glob.glob(os.path.join(directory_path, extension))


def list_subdirectories_with_matching_prefix(path_to_directory, prefix):
	return [d for d in glob.glob(os.path.join(path_to_directory, prefix + '*/'))]


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


def handle_input_args(args, repo_root_directory, base_data_directory, base_directory_csv, base_directory_parquet):

	import_args = parse_supported_file_format_arguments(ALLOWED_FORMATS, args.formats, 'parquet')

	input_directory_paths = {}
	if args.input_directory_path:
		input_directory_paths['_']       = args.input_directory_path
	else:
		input_directory_paths['csv']     = os.path.join(repo_root_directory, base_data_directory, base_directory_csv)
		input_directory_paths['parquet'] = os.path.join(repo_root_directory, base_data_directory, base_directory_parquet)

	import_args = [extension for extension, is_allowed in import_args.items() if is_allowed]
	if len(import_args) > 1:
		raise PreconditionError(f'Only one import format is allowed: {" or ".join(ALLOWED_FORMATS)}')

	input_directory_paths     = {k: v for k, v in input_directory_paths.items() if k in import_args or k == '_'}
	missing_input_directories = [input_directory_path for extension, input_directory_path in input_directory_paths.items() if not file_exists(input_directory_path)]
	if missing_input_directories:
		raise PreconditionError(f'Input directories are missing:\n\t{"\n\t".join(missing_input_directories)}')

	return import_args, input_directory_paths


def handle_output_args(args, repo_root_directory, base_data_directory, base_directory_csv, base_directory_parquet):

	export_args = parse_supported_file_format_arguments(ALLOWED_FORMATS, args.exports, 'parquet')

	output_directory_paths = {}
	if args.output_directory_path:
		output_directory_paths['_']       = args.output_directory_path
	else:
		output_directory_paths['csv']     = os.path.join(repo_root_directory, base_data_directory, base_directory_csv)
		output_directory_paths['parquet'] = os.path.join(repo_root_directory, base_data_directory, base_directory_parquet)

	return export_args, output_directory_paths
