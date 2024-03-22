#!/usr/bin/env python3
import os
import json
import argparse
import logging

from pathlib import Path
import pysftp
from io import StringIO
import paramiko

logger = logging.getLogger("tap-sftp-files")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    with open(path) as f:
        return json.load(f)

def get_files_to_download(sftp, remote_dir, search_patterns, exact_directory=False):
    files_to_download = []
    with sftp.cd(remote_dir):
        for item in sftp.listdir_attr():
            remote_path = os.path.join(remote_dir, item.filename)
            match_found = False

            for pattern in search_patterns:
                if item.filename.startswith(pattern):
                    if not exact_directory or (exact_directory and sftp.isfile(item.filename)):
                        files_to_download.append(item.filename)
                        match_found = True  # Set flag to True if match is found
            
            # If no match is found and exact_directory is True, check if it's a directory and add it
            if match_found and sftp.isfile(item.filename):
                files_to_download.append(item.filename)

    if files_to_download:            
        logger.info(f"Found following directories to download {files_to_download}")
    return files_to_download

def download_files(sftp, remote_dir, local_dir, files_to_download):
    # Download the selected files
    if len(files_to_download)>0:
        for file in files_to_download:
            # Create local directory if it doesn't exist
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            remote_path = os.path.join(remote_dir, file)
            local_path = os.path.join(local_dir, file)
            logger.info(f"Downloading file {remote_path}: {local_path}")
            sftp.get(remote_path, local_path)

def recursive_download(sftp, remote_dir, local_dir, search_patterns, exact_directory=False):
    files_to_download = get_files_to_download(sftp, remote_dir, search_patterns, exact_directory)
    download_files(sftp, remote_dir, local_dir, files_to_download)
    if not exact_directory:
        directories_to_download = [filename for filename in sftp.listdir(remote_dir) if not sftp.isfile(os.path.join(remote_dir, filename))]
        for directory in directories_to_download:
            remote_path = os.path.join(remote_dir, directory)
            local_path = os.path.join(local_dir, directory)
            logger.info(f"Recursive search for files in {remote_path}")
            recursive_download(sftp, remote_path, local_path, search_patterns, exact_directory)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args


def download(args):
    logger.debug(f"Downloading data...")
    config = args.config
    host = config['host']
    port = config.get('port', "")
    remote_path = config.get('path_prefix')
    remote_files = config.get('files')
    target_dir = config['target_dir']

    connection_config = {
        'username': config['username'],
    }

    if config.get('password'):
        connection_config['password'] = config['password']
    elif config.get("private_key"):
        private_key_path = f"{os.getcwd()}/key.pem"
        with open(private_key_path, "w") as f:
            f.write(config.get("private_key"))

        connection_config['private_key'] = private_key_path

    if port:
        connection_config['port'] = int(port)
    if config.get("tables"):
        with pysftp.Connection(host, **connection_config) as sftp:
            recursive_download(sftp, remote_path, target_dir, config.get("tables"), exact_directory=config.get("exact_directory",False))

    elif remote_files:
        with pysftp.Connection(host, **connection_config) as sftp:
            for file in remote_files:
                target = f"{target_dir}/{file.split('/')[-1]}"
                logger.info(f"Downloading: data from {file} -> {target}")
                sftp.get(file, target)
    elif remote_path:
        # Establish connection to SFTP server
        with pysftp.Connection(host, **connection_config) as sftp:
            logger.info(f"Downloading: data from {remote_path} -> {target_dir}")
            # Copy all files in remote_path to target_dir
            sftp.get_r(remote_path, target_dir)
    else:
        raise Exception("One of the parameters path_prefix or files must be defined.")

    logger.info(f"Data downloaded.")

def main():
    # Parse command line arguments
    args = parse_args()

    # Download the data
    download(args)


if __name__ == "__main__":
    main()
