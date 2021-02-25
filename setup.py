#!/usr/bin/env python
import setuptools
from distutils.core import setup

setup(name='fastapi_crud_orm_connector',
      version='0.1.0',
      description='Simple methods for fast prototyping crud operations connecting fastapi with datasets, such as sqlalchemy and pandas',
      author='Leonardo Christino',
      author_email='leomilho@gmail.com',
      url='https://github.com/christinoleo/fastapi_crud_orm_connector/',
      packages=setuptools.find_packages(),
      license='MIT',
      long_description=open('README.md').read(),
      long_description_content_type="text/markdown",
      )
