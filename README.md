# Original code
Please see branch `original-code` for the original code. This version includes some improvements and removes some features.

# How to use
## Get a database up
Please see the additional documentation if you don't know how to do this.

## Extract a flat file
Open `sync_nlx` and set the variables correctly.
```
pipenv run sync_nlx.py
```

The original code compresses and pushes the flat files to S3, this version does not. It can be easily copy and pasted to add that functionality by going to the `original-code` branch.

Compress your flat file (gz preferably) and send it whereever you need to.

If instead you want to create the schema.org files continue to the next step without compressing.

## Conversion to schema.org
Set the correct variables in `nlx.py` and run
```
pipenv run nlx.py
```
