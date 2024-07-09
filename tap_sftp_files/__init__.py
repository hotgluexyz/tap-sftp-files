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

    if remote_files:
        with pysftp.Connection(host, **connection_config) as sftp:
            for file in remote_files:
                target = f"{target_dir}/{file.split('/')[-1]}"
                logger.info(f"Downloading: data from {file} -> {target}")
                sftp.get(file, target)
    elif remote_path:
        # Establish connection to SFTP server
        with pysftp.Connection(host, **connection_config) as sftp:
            logger.info(f"Downloading: data from {remote_path} -> {target_dir}")
            if config.get("recursive_clone", False):
                with sftp.cd(remote_path):
                    # Copy all files in remote_path to target_dir
                    sftp.get_r(".", target_dir)
            elif config.get("exact_directory", False):
                sftp.get_d(remote_path, target_dir)
            else:
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
