#!/usr/bin/env python

from distutils.core import setup


setup(
    name='dreque',
    version='0.3.0',
    description='Persistent job queueing library using Redis inspired by Resque',
    author='Samuel Stauffer',
    author_email='samuel@lefora.com',
    url='http://github.com/samuel/dreque',
    packages=['dreque'],
    requires=["redis"],
    install_requires=["redis"],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
