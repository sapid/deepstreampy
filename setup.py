import os
from setuptools import setup, find_packages

def requirements(filename='requirements.txt'):
    with open(filename) as f:
        requirements = f.read().splitlines()
        return requirements

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='deepstreampy-twisted',
      version='0.2.0',
      author='Will Crawford',
      description='A deepstream.io client for Twisted.',
      license='MIT',
      url='https://www.github.com/sapid/deepstreampy-twisted',
      packages=find_packages(),
      install_requires=requirements(),
      long_description=read('README.md'),
      test_suite='tests')
