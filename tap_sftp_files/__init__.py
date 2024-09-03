#!/usr/bin/env python3
import os
import json
import argparse
import logging

from pathlib import Path
import pysftp
from io import StringIO
import paramiko
import hashlib

logger = logging.getLogger("tap-sftp-files")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    if not os.path.exists(path):
        return dict()

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

    parser.add_argument(
        '-s', '--state',
        help='State file',
        required=False)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = load_json(args.state)

    return args


def calculate_md5(file_path):
    """Calculate the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def sftp_remove(sftp_conn, delete_after_sync=False, remote_file=None, remote_path=None):
    if not delete_after_sync:
        return

    try:
        if remote_file:
            logger.info(f"Removing: remote file {remote_file}")
            sftp_conn.remove(remote_file)
        elif remote_path:
            logger.info(f"Removing: remote path {remote_path}")
            sftp_conn.rmdir(remote_path)
    except:
        logger.exception("Error removing files")


def download(args):
    logger.debug(f"Downloading data...")
    config = args.config
    host = config['host']
    port = config.get('port', "")
    remote_path = config.get('path_prefix')
    remote_files = config.get('files')
    target_dir = config['target_dir']
    delete_after_sync = config.get('delete_after_sync', False)

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
                sftp_remove(sftp, delete_after_sync, remote_file=file)
    elif remote_path:
        # Establish connection to SFTP server
        with pysftp.Connection(host, **connection_config) as sftp:
            logger.info(f"Downloading: data from {remote_path} -> {target_dir}")
            if config.get("recursive_clone", False):
                with sftp.cd(remote_path):
                    # Copy all files in remote_path to target_dir
                    sftp.get_r(".", target_dir)
                sftp_remove(sftp, delete_after_sync, remote_path=remote_path)
            elif config.get("exact_directory", False):
                sftp.get_d(remote_path, target_dir)
                sftp_remove(sftp, delete_after_sync, remote_path=remote_path)
            else:
                # Copy all files in remote_path to target_dir
                sftp.get_r(remote_path, target_dir)
                sftp_remove(sftp, delete_after_sync, remote_path=remote_path)
    else:
        raise Exception("One of the parameters path_prefix or files must be defined.")

    if config.get('incremental_mode'):
        state = args.state or dict()

        # Need to walk the entire target_dir and create the md5
        logger.info(target_dir)
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                remote_file_path = local_file_path.replace(target_dir, remote_path, 1)
                file_hash = calculate_md5(local_file_path)

                # NOTE: We need to delete this file, it's already been synced
                if file_hash == state.get(remote_file_path):
                    os.remove(local_file_path)

                state[remote_file_path] = file_hash

        # Write the updated state
        json.dump(state, open(args.state_path, "w"), indent=4)

    logger.info(f"Data downloaded.")


def main():
    # Parse command line arguments
    args = parse_args()

    # Download the data
    download(args)


if __name__ == "__main__":
    main()
