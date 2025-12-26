#!/usr/bin/env python3
import os
import json
import argparse
import logging
import time
import stat
import hashlib
from contextlib import contextmanager

from tap_sftp_files.client import SFTPConnection

logger = logging.getLogger("tap-sftp-files")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    if not os.path.exists(path):
        return dict()

    with open(path) as f:
        return json.load(f)


def isdir(sftp, path):
    """Check if a remote path is a directory."""
    try:
        return stat.S_ISDIR(sftp.stat(path).st_mode)
    except:
        return False


@contextmanager
def cd(sftp, path):
    """Context manager to change directory."""
    original_path = sftp.getcwd()
    try:
        sftp.chdir(path)
        yield
    finally:
        sftp.chdir(original_path)


def get_r(sftp, remote_path, local_path, current_file_count=0, max_file_count=None):
    """Recursively download files and directories."""
    if max_file_count and current_file_count >= max_file_count:
        return current_file_count
    
    try:
        attrs = sftp.stat(remote_path)
        if stat.S_ISDIR(attrs.st_mode):
            # Create local directory
            os.makedirs(local_path, exist_ok=True)
            # List and download contents
            for item in sftp.listdir(remote_path):
                remote_item = os.path.join(remote_path, item).replace('\\', '/')
                local_item = os.path.join(local_path, item)
                current_file_count = get_r(sftp, remote_item, local_item, current_file_count, max_file_count)
                if max_file_count and current_file_count >= max_file_count:
                    break
        else:
            # Download file
            if max_file_count and current_file_count >= max_file_count:
                return current_file_count
            sftp.get(remote_path, local_path)
            current_file_count += 1
    except Exception as e:
        logger.warning(f"Error downloading {remote_path}: {e}")
    
    return current_file_count


def get_d(sftp, remote_path, local_path, current_file_count=0, max_file_count=None):
    """Download directory structure exactly."""
    if max_file_count and current_file_count >= max_file_count:
        return current_file_count
    
    try:
        attrs = sftp.stat(remote_path)
        if stat.S_ISDIR(attrs.st_mode):
            # Create local directory
            os.makedirs(local_path, exist_ok=True)
            # List and download contents
            for item in sftp.listdir(remote_path):
                remote_item = os.path.join(remote_path, item).replace('\\', '/')
                local_item = os.path.join(local_path, item)
                current_file_count = get_d(sftp, remote_item, local_item, current_file_count, max_file_count)
                if max_file_count and current_file_count >= max_file_count:
                    break
    except Exception as e:
        logger.warning(f"Error downloading {remote_path}: {e}")
    
    return current_file_count


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


def rm(sftp_conn, remote_path, removed_file_count=0, max_file_count=None):
    files = sftp_conn.listdir(remote_path)

    for f in files:
        if max_file_count and removed_file_count >= max_file_count:
            break
        filepath = os.path.join(remote_path, f)

        if isdir(sftp_conn, filepath):
            removed_file_count = rm(sftp_conn, filepath, removed_file_count, max_file_count)
        else:
            sftp_conn.remove(filepath)
            removed_file_count += 1

    return removed_file_count


def sftp_remove(sftp_conn, delete_after_sync=False, remote_file=None, remote_path=None, removed_file_count=0, max_file_count=None):
    if not delete_after_sync:
        return

    try:
        if remote_file and ((max_file_count and removed_file_count < max_file_count) or not max_file_count):
            logger.info(f"Removing: remote file {remote_file}")
            sftp_conn.remove(remote_file)
            removed_file_count += 1
        elif remote_path:
            logger.info(f"Removing: remote path {remote_path}")
            removed_file_count = rm(sftp_conn, remote_path, removed_file_count, max_file_count)
    except:
        logger.exception("Error removing files")

    return removed_file_count


def download(args):
    logger.debug(f"Downloading data...")
    config = args.config
    remote_path = config.get('path_prefix')
    remote_files = config.get('files')
    target_dir = config['target_dir']
    delete_after_sync = config.get('delete_after_sync', False)
    incremental_mode = config.get('incremental_mode')
    max_file_count = config.get('max_file_count')
    removed_file_count = 0
    current_file_count = 0

    connection_config = {
        'host': config['host'],
        'username': config['username'],
        'port': config.get('port'),
        'password': config.get('password'),
        'private_key_file': config.get('private_key_file'),
        'private_key': config.get('private_key'),
    }

    if max_file_count:
        try:
            max_file_count = int(max_file_count)
        except Exception as exc:
            raise Exception(f"max_file_count must be an integer not {max_file_count}") from exc
    
    # Debug SFTP connection logging (if enabled in config)
    if config.get('sftp_debug_logging', False):
        # initialize sftp connection with paramiko to get the current working directory and list the contents of the root directory
        with SFTPConnection(**connection_config) as sftp_conn:
            sftp = sftp_conn.sftp
            try:
                cwd = sftp.getcwd()
                logger.info(f"[SFTP Debug] Current working directory: {cwd}")
            except Exception as e:
                logger.warning(f"[SFTP Debug] Could not get current working directory: {e}")

            try:
                logger.info("[SFTP Debug] Listing contents of cwd ('.'):")
                entries = sftp.listdir_attr(".")
                for e in entries:
                    kind = "DIR" if stat.S_ISDIR(e.st_mode) else "FILE"
                    mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e.st_mtime))
                    logger.info(f"[SFTP Debug] - {kind:4} {e.filename:20} "
                                f"size={e.st_size} modified={mtime}")
            except Exception as e:
                logger.warning(f"[SFTP Debug] Could not list contents of cwd ('.'): {e}")

            try:
                logger.info("[SFTP Debug] Attempting to list root directory ('/'):")
                entries = sftp.listdir_attr("/")
                for e in entries:
                    kind = "DIR" if stat.S_ISDIR(e.st_mode) else "FILE"
                    logger.info(f"[SFTP Debug] - {kind:4} {e.filename}")
            except Exception as e:
                logger.warning(f"[SFTP Debug] Could not list root directory ('/'): {e}")
        # end of logs

    if remote_files:
        with SFTPConnection(**connection_config) as sftp_conn:
            sftp = sftp_conn.sftp
            for file in remote_files:
                if max_file_count and current_file_count >= max_file_count:
                    break
                target = f"{target_dir}/{file.split('/')[-1]}"
                logger.info(f"Downloading: data from {file} -> {target}")
                if not isdir(sftp, file):
                    sftp.get(file, target)
                    current_file_count += 1
                if not incremental_mode:
                    removed_file_count = sftp_remove(sftp, delete_after_sync, remote_file=file, removed_file_count=removed_file_count, max_file_count=max_file_count)
    elif remote_path:
        # Establish connection to SFTP server
        with SFTPConnection(**connection_config) as sftp_conn:
            sftp = sftp_conn.sftp
            logger.info(f"Downloading: data from {remote_path} -> {target_dir}")
            if config.get("recursive_clone", False):
                with cd(sftp, remote_path):
                    # Copy all files in remote_path to target_dir
                    current_file_count = get_r(sftp, ".", target_dir, current_file_count, max_file_count)
            elif config.get("exact_directory", False):
                current_file_count = get_d(sftp, remote_path, target_dir, current_file_count, max_file_count)
            else:
                # Copy all files in remote_path to target_dir
                current_file_count = get_r(sftp, remote_path, target_dir, current_file_count, max_file_count)

            if not incremental_mode:
                removed_file_count = sftp_remove(sftp, delete_after_sync, remote_path=remote_path, removed_file_count=removed_file_count, max_file_count=max_file_count)
    else:
        raise Exception("One of the parameters path_prefix or files must be defined.")

    if incremental_mode:
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
                else:
                    # If it's not already been synced, delete from remote
                    with SFTPConnection(**connection_config) as sftp_conn:
                        sftp = sftp_conn.sftp
                        removed_file_count = sftp_remove(sftp, delete_after_sync, remote_file=remote_file_path, removed_file_count=removed_file_count, max_file_count=max_file_count)

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
