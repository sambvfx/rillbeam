"""
Beam testing
"""
import os
import re
from setuptools import setup, find_packages

PKG_NAME = 'rillbeam'


_dirname = os.path.abspath(os.path.dirname(__file__))


def read(*paths):
    with open(os.path.join(_dirname, *paths)) as f:
        return f.read()


def version():
    """
    Sources version from the __init__.py so we don't have to maintain the
    value in two places.
    """
    regex = re.compile(r'__version__ = \'([0-9.]+)\'')
    for line in read(PKG_NAME, '__init__.py').split('\n'):
        match = regex.match(line)
        if match:
            return match.groups()[0]


def requirements():
    """
    Sources install_requires from requirements.txt so we don't have to maintain
    in two places.
    """
    results = []
    for line in read('requirements.txt').split('\n'):
        line = line.strip()
        if line.startswith('#'):
            continue
        results.append(line.split(' ')[0].split('#')[0])
    return results


setup(
    name=PKG_NAME,
    version=version(),
    description=__doc__,
    long_description=read('README.md'),
    author='Sam Bourne',
    packages=find_packages(),
    # FIXME: including apache_beam in requirements causes flink to fail with:
    #  Received exit code 1 for command 'docker inspect -f {{.State.Running}} <uuid>'. stderr: Error: No such object: <uuid>
    # install_requires=requirements(),
    install_requires=['typing'],
    extras_require={
        'tests': [
            'pytest',
        ],
    }
)
