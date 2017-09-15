# *-* coding: utf-8 -*-
from setuptools import setup, find_packages
from os.path import join, dirname

VERSION_FILE = open(join(dirname(__file__), 'VERSION'))
__versionstr__ = VERSION_FILE.read().strip()
VERSION_FILE.close()

REQUIREMENTS = open(join(dirname(__file__), 'requirements.txt'))
INSTALL_REQUIRES = REQUIREMENTS.read().split('\n')
REQUIREMENTS.close()

f = open(join(dirname(__file__), 'DESCRIPTION'))
long_description = f.read().strip()
f.close()

CONSOLE_SCRIPTS = [
    'toflerdb-server=toflerdb.apiserver:run_apiserver',
    'toflerdb-dbinit=toflerdb.scripts.bootstrap:run_bootstrap',
    'toflerdb-run-sample=toflerdb.scripts.sample:run_sample',
    'toflerdb-generate-config=toflerdb.scripts.sampleconfig:run',
]


setup(
    name='toflerdb',
    version=__versionstr__,
    description='ToflerDB',
    long_description=long_description,
    url='http://www.toflerdb.com',
    author='Team Tofler',
    author_email='devteam@tofler.in',
    license='GNU AGPLv3',
    package_dir={'': './'},
    packages=find_packages('./'),
    install_requires=INSTALL_REQUIRES,
    dependency_links=[
        'http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip#md5=6d42998cfec6e85b902d4ffa5a35ce86'
    ],
    include_package_data=True,
    package_data={'toflerdb': [
        'resources/data/*', 'resources/dbpatches/*']
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2.7',
        'Operating System :: OS Independent'
    ],
    entry_points={
        'console_scripts': CONSOLE_SCRIPTS,
    }
    )
