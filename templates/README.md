# Configuration Template for tap-sftp-files

This directory contains a template configuration file for the `tap-sftp-files` tool.

## Quick Start

1. Copy `config.json` to your desired location (e.g., `config.json`)
2. Fill in the required fields
3. Run the tool: `tap-sftp-files -c config.json [-s state.json]`

## Configuration Options

### Required Fields

- **`host`** (string): The SFTP server hostname or IP address
  - Example: `"sftp.example.com"` or `"192.168.1.100"`

- **`username`** (string): The username for SFTP authentication
  - Example: `"myuser"`

- **`target_dir`** (string): Local directory where files will be downloaded
  - Example: `"./downloads"` or `"/path/to/local/directory"`

### Required (One of the following)

You must provide either `path_prefix` OR `files` (but not necessarily both):

- **`path_prefix`** (string, optional): Remote directory path to download from
  - Example: `"/remote/path/to/files"`
  - If specified, all files in this directory will be downloaded

- **`files`** (array of strings, optional): List of specific remote file paths to download
  - Example: `["/remote/path/file1.txt", "/remote/path/file2.csv"]`
  - If specified, only these files will be downloaded

### Optional Fields

#### Connection Settings

- **`port`** (integer, optional): SFTP server port number
  - Default: `22` (standard SFTP port)
  - Example: `2222`

#### Authentication

You must provide either `password` OR `private_key` OR `private_key_file` (but not both):

- **`password`** (string, optional): Password for password-based authentication
  - Example: `"your_password"`

- **`private_key`** (string, optional): Private key content for key-based authentication
  - Should be the full private key content including headers
  - Example: `"-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----"`

- **`private_key_file`** (string, optional): Path to private key file for key-based authentication
  - Supports RSA, DSS, ECDSA, and Ed25519 key types
  - Example: `"path/to/private_key"`

#### Download Behavior

- **`recursive_clone`** (boolean, optional): If `true`, changes to the remote directory before downloading
  - Default: `false`
  - When `true`, uses `sftp.cd()` to change to `path_prefix` before downloading
  - Only applies when using `path_prefix` mode

- **`exact_directory`** (boolean, optional): If `true`, downloads the remote directory contents (non-recursive)
  - Default: `false`
  - Only applies when using `path_prefix` mode

#### File Management

- **`delete_after_sync`** (boolean, optional): If `true`, deletes files from remote SFTP server after successful download
  - Default: `false`
  - **Warning**: Use with caution! This permanently deletes files from the remote server

- **`max_file_count`** (integer, optional): Maximum number of files to download/delete
  - Default: `null` (no limit)
  - Example: `100`
  - Useful for testing or limiting the scope of operations

#### Advanced Options

- **`incremental_mode`** (boolean, optional): If `true`, enables incremental sync mode
  - Default: `false`
  - When enabled:
    - Requires a state file (`-s` or `--state` argument)
    - Tracks MD5 hashes of downloaded files
    - Skips files that haven't changed since last sync
    - Deletes local files that match previous state
    - Updates state file with new file hashes

- **`sftp_debug_logging`** (boolean, optional): If `true`, enables detailed SFTP connection debugging
  - Default: `false`
  - When enabled, logs:
    - Current working directory
    - Contents of current directory
    - Contents of root directory
  - Useful for troubleshooting connection issues

## Usage Examples

### Basic Download (Single Directory)

```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "password": "mypassword",
  "target_dir": "./downloads",
  "path_prefix": "/data/exports"
}
```

### Download Specific Files

```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----",
  "target_dir": "./downloads",
  "files": [
    "/data/file1.csv",
    "/data/file2.json"
  ]
}
```

### Incremental Sync with State Tracking

```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "password": "mypassword",
  "target_dir": "./downloads",
  "path_prefix": "/data/exports",
  "incremental_mode": true
}
```

Run with state file:
```bash
tap-sftp-files -c config.json -s state.json
```

### Download and Delete After Sync

```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "password": "mypassword",
  "target_dir": "./downloads",
  "path_prefix": "/data/exports",
  "delete_after_sync": true,
  "max_file_count": 100
}
```

### Debug Mode

```json
{
  "host": "sftp.example.com",
  "username": "myuser",
  "password": "mypassword",
  "target_dir": "./downloads",
  "path_prefix": "/data/exports",
  "sftp_debug_logging": true
}
```

## Command Line Usage

```bash
# Basic usage
tap-sftp-files -c config.json

# With state file (required for incremental_mode)
tap-sftp-files -c config.json -s state.json
```

## Notes

- When using `incremental_mode`, you **must** provide a state file using the `-s` or `--state` argument
- The state file stores MD5 hashes of downloaded files to track changes
- If `delete_after_sync` is enabled, files are deleted from the remote server after successful download
- `max_file_count` limits both downloads and deletions when `delete_after_sync` is enabled
- `recursive_clone` and `exact_directory` only apply when using `path_prefix` mode (not with `files`)

## Security Considerations

- **Never commit** your actual `config.json` file with real credentials to version control
- Store passwords and private keys securely (consider using environment variables or secret management)
- Use private key authentication when possible instead of passwords
