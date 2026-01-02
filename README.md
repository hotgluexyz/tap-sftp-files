# tap-sftp-files

`tap-sftp-files` is a small Python CLI for downloading files from an SFTP server to a local directory. It supports downloading a directory (`path_prefix`) or an explicit list of files (`files`), with optional incremental syncing and optional remote cleanup.

### Local development setup

Use `pyenv` to install Python 3.10 and create an isolated virtualenv in `.venv`:

```bash
pyenv install 3.10.13
pyenv local 3.10.13

python -m venv .venv
source .venv/bin/activate
```

Install the package in editable mode:

```bash
pip install -e .
```

### Usage

```bash
# Basic usage
tap-sftp-files -c config.json

# With state file (required when incremental_mode=true)
tap-sftp-files -c config.json -s state.json
```

### Configuration

- **Config reference**: see `templates/README.md`
- **Config template**: start from `templates/config.json`

### Notes

- **Incremental mode**: when `incremental_mode` is enabled, you must pass a state file via `-s/--state`.
- **Remote deletion**: if `delete_after_sync` is enabled, files are deleted from the remote server after successful download (use with care).


