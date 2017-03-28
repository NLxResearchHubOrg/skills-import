#!/usr/bin/env python

from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

setup(
    name='Skills Import Private',
    version='0.1.0',
    description='Importers from private datasets',
    author="Center for Data Science and Public Policy",
    author_email='datascifellows@gmail.com',
    url='https://github.com/workforce-data-initiative/skills-import-private',
    packages=['skills_import_private'],
    include_package_data=True,
    install_requires=[],
    license="MIT license",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
)
