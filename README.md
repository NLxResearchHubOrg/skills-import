# Skills Import Private

Utilities for importing private skills/jobs datasets and converting them to a common schema.

## Adding an importer

Add a class under skills_import_private somewhere, and then add it to the list of importers at skills_import_private/__init__.py
Also, please add a test! Subclass a test from https://github.com/workforce-data-initiative/skills-utils/blob/master/tests/test_job_posting_import.py to make sure that your import matches the expected schema.

## Running tests

This repository uses py.test. Just run `py.test`
