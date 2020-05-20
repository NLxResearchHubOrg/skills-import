# Skills Import

## How to set this repo up

Please see [this documentation](https://docs.google.com/document/d/1_Bz1zeICXzgwQC_N637CQIyxHeRwMuSV5FG7GLDO9IM/edit?usp=sharing) for indepth technical notes on how to setup this repo.

## How to use

### Extract from database to a flat file (CSV)

1. Open `sync_nlx` and set the variables throughout the file correctly.

1. Run the script to scrap the database,

    ```
    pipenv run sync_nlx.py
    ```

    The original code compresses and pushes the flat files to S3, this version does not. It can be easily copy and pasted to add that functionality by going to the `original-code` branch.

1. Compress your flat file (gz preferably) and send it wherever you need to.

    If instead you want to create the schema.org files continue to the next step without compressing.

### Convert flat file (CSV) to schema.org JobPosting

1. Set the correct variables in `nlx.py`

1. Run the application

    ```
    pipenv run nlx.py
    ```

## Troubleshooting

Some organizations may have trouble opening the csv files produced.

In that case you can try ASCII encoding the file and setting the row delimieter to CRLF with the following commands,

```bash
cat 2018.csv | iconv -c -t ASCII//IGNORE > 2018_ascii_dos.csv
unix2dos 2018_ascii_dos.csv
```

## Original code

Please see branch `original-code` for the original code. This version includes some improvements and removes some features.
