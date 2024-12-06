
import os


def file_exists(local_path):
	return os.path.exists(local_path)


def create_local_folder(folder_path):
	if not os.path.exists(folder_path):
		os.makedirs(folder_path, exist_ok=True)

