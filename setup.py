#!/usr/bin/env python
from setuptools import setup
import sys

VERSION = "0.0.5"

COMMANDS = ['agilefuse']

classifiers = [
	'Development Status :: 3 - Alpha',
	'Intended Audience :: Developers',
	'License :: OSI Approved :: BSD License',
	'Operating System :: OS Independent',
	'Operating System :: MacOS',
	'Operating System :: POSIX :: Linux',
	'Environment :: MacOS X',
	'Programming Language :: Python',
	'Programming Language :: Python :: 2.7',
	'Topic :: System :: Filesystems',
]

long_desc = """agilefuse is a module and command that provides a FUSE abstraction of the Agile Cloud Storage, commonly known as a 'mounted' network filesystem on a native filesystem.  It leverages the AgileCLU python module and associated configuration files.

At present, it provides a read-only filesystem.  It has been tested on Linux and Mac OS X (and can be configured to show-up in Finder and the Desktop)."""

extra = {}

setup(install_requires=['AgileCLU','fusepy','pylibmc','pycurl'],
	name='agilefuse',
	version=VERSION,
	description='Agile Cloud Storage FUSE Mount',
	long_description=long_desc,
	license = 'BSD',
	author = 'Wylie Swanson',
	author_email = 'wylie@pingzero.net',
	url='https://github.com/wylieswanson/agilefuse',
	download_url='https://github.com/wylieswanson/agilefuse',
	platforms = ['any'],
	packages = ['agilefuse','agilefuse.commands'],
	scripts = ['agilefuse/commands/%s' % command for command in COMMANDS],
	classifiers=classifiers,
	**extra
	)
