#!/usr/bin/env python

from __future__ import print_function

import sys

from setuptools import setup

if sys.version_info < (3, 6):
    print("Python versions prior to 3.6 are not supported for pip installed byzer-python-client.",
          file=sys.stderr)
    sys.exit(-1)

try:
    exec(open('version.py').read())
except IOError:
    print("Failed to load byzer-python-client version file for packaging.",
          file=sys.stderr)
    sys.exit(-1)

VERSION = __version__

setup(
    name='byzer_python_client',
    version=VERSION,
    description='Builder to build&execute Byzer-lang script',
    long_description="Builder to build&execute Byzer-lang script",
    author='WilliamZhu',
    author_email='allwefantasy@gmail.com',
    url='https://github.com/allwefantasy/byzer-client-sdk',
    packages=['tech',
              'tech.mlsql',
              'tech.mlsql.byzer_client_sdk',
              'tech.mlsql.byzer_client_sdk.python_lang',
              'tech.mlsql.byzer_client_sdk.python_lang.generator'
              ],
    include_package_data=True,
    license='http://www.apache.org/licenses/LICENSE-2.0',
    install_requires=[
        'click>=6.7',
        'jinja2>=3.0.0',
        'requests'
    ],
    entry_points='''
        [console_scripts]
        mlsql_plugin_tool=tech.mlsql.plugin.tool.Plugin:main
    ''',
    setup_requires=['pypandoc'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy']
)
