#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-pinterest-ads',
      version='0.0.7',
      description='Singer.io tap for extracting data from the Pinterest-ads v5.0 API',
      author='singer-io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_pinterest_ads'],
      install_requires=[
          'backoff==1.8.0',
          'singer-sdk==0.3.14'
      ],
      entry_points='''
          [console_scripts]
          tap-pinterest-ads=tap_pinterest_ads.tap:TapPinterestAds.cli
      ''',
      packages=find_packages(),
      package_data={
          'tap_pinterest_ads': [
              'schemas/*.json',
              'schemas/shared/*.json',
              'tests/*.py'
          ]
      })
