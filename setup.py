#!/usr/bin/env python

from distutils.core import setup

setup(name='django_hana',
      version='1.0',
      description='SAP HANA backend for Django 1.4',
      author='Kapil Ratnani',
      author_email='kapil.ratnani@iiitb.net',
      url='https://github.com/kapilratnani/django_hana',
      packages=['django_hana'],
      requires = ['django (>= 1.4)'],
     )
