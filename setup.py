#!/usr/bin/env python

from setuptools import setup

setup(
    name='tap-sftp-files',
    version='0.0.2',
    description='hotglue tap for importing files from SFTP',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_sftp_files'],
    install_requires=[
        'argparse==1.4.0',
        'pysftp==0.2.8'
    ],
    entry_points='''
        [console_scripts]
        tap-sftp-files=tap_sftp_files:main
    ''',
    packages=['tap_sftp_files']
)
