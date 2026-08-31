#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-sftp-files',
    version='0.0.3',
    description='hotglue tap for importing files from SFTP',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.14',
    ],
    python_requires='>=3.7',
    py_modules=['tap_sftp_files'],
    install_requires=[
        'backoff==2.2.1',
        'paramiko>=2.12,<3.5; python_version < "3.8"',
        'paramiko>=3.5.1,<4; python_version >= "3.8" and python_version < "3.9"',
        'paramiko>=4,<5; python_version >= "3.9"'
    ],
    entry_points='''
        [console_scripts]
        tap-sftp-files=tap_sftp_files:main
    ''',
    packages=['tap_sftp_files']
)
