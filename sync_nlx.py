import pymssql
from skills_utils.time import datetime_to_year_quarter
from skills_utils.s3 import upload
import unicodecsv as csv
import gzip
from os import getenv

filehandles = {}
writers = {}
s3_keys = {}


def filename(quarter_string):
    return '/mnt/sqltransfer/{}.gz'.format(quarter_string)


def output_writer(row, cursor_desc):
    year, _ = datetime_to_year_quarter(row['dateacquired'])
    year_string = str(year)
    if year_string not in writers:
        filehandles[year_string] = \
            gzip.open(filename(year_string), 'ab')
        writers[year_string] = \
            csv.DictWriter(filehandles[year_string], fieldnames=cursor_desc)
        writers[year_string].writeheader()

    return writers[year_string]


QUERY = """
SELECT
j.generatedJobId,
[jobID],
j.city,
j.state,
j.zipcode,
j.country,
[createddate],
[dateacquired],
[description],
[expired],
[expiryDate],
[fedcontractor],
[fein],
[lastUpdatedDate],
[link],
[sourceState],
[title],
onet.value as onet_code,
naics.value as naics_code,
mined.value as min_education,
expr.value as experience,
lic.value as license,
train.value as training,
positions.maximum as maxPositions,
duration.maximum as jobDuration,
hoursperweek.maximum as weeklyHours,
shift.maximum as shift,
salary.minimum as minSalary,
salary.maximum as maxSalary,
salary.unit as salaryUnit,
a.company,
addr.city as companyCity,
addr.state as companyState,
addr.zipcode as companyZipcode,
addr.country as companyCountry,
addr.line1 as companyAddress
FROM [LMI-Oct2015].[dbo].[Job] j
LEFT JOIN Classification onet ON (
    j.generatedJobId = onet.generatedJobId and onet.type = 'ONET'
)
LEFT JOIN Classification naics ON (
    j.generatedJobId = naics.generatedJobId and naics.type = 'NAICS'
)
LEFT JOIN Requirement mined on (
    j.generatedJobId = mined.generatedJobId and mined.type = 'mineducation'
)
LEFT JOIN Requirement expr on (
    j.generatedJobId = expr.generatedJobId and expr.type = 'experience'
)
LEFT JOIN Requirement lic on (
    j.generatedJobId = lic.generatedJobId and lic.type = 'license'
)
LEFT JOIN Requirement train on (
    j.generatedJobId = train.generatedJobId and train.type = 'training'
)
LEFT JOIN Parameter positions on (
    j.generatedJobId = positions.generatedJobId and parameterType = 'positions'
)
LEFT JOIN Parameter duration on (
    j.generatedJobId = duration.generatedJobId
    and duration.parameterType = 'duration'
)
LEFT JOIN Parameter hoursperweek on (
    j.generatedJobId = hoursperweek.generatedJobId
    and hoursperweek.parameterType = 'hoursperweek'
)
LEFT JOIN Parameter shift on (
    j.generatedJobId = shift.generatedJobId
    and shift.parameterType = 'shift'
)
LEFT JOIN Parameter salary on (
    j.generatedJobId = salary.generatedJobId
    and salary.parameterType = 'salary'
)
LEFT JOIN [Application] a on (j.generatedJobId = a.generatedJobId)
LEFT JOIN [Address] addr on (a.applicationId = addr.applicationId)
WHERE j.generatedJobId >= {min_pk} and j.generatedJobId < {max_pk}
"""


def get_batch(cursor, min_pk, batch_size):
    bound_query = QUERY.format(min_pk=min_pk, max_pk=min_pk+batch_size)
    cursor.execute(bound_query)
    cursor_desc = dict(
        (field[0], field[0])
        for field in cursor.description
    )

    i = 0
    for row in cursor:
        i += 1
        if i % 10000 == 0:
            print(i)
        output_writer(row, cursor_desc).writerow(row)


def run(s3_conn, output_s3_prefix):
    try:
        connection = pymssql.connect(
            server=getenv('PYMSSQL_SERVER'),
            user=getenv('PYMSSQL_WINDOWS_USER').replace("\\", "\\\\"),
            password=getenv('PYMSSQL_WINDOWS_PASSWORD'),
            database=getenv('PYMSSQL_DATABASE'),
        )
        cursor = connection.cursor(as_dict=True)
        max_pk = 42770863
        batch_size = 1000000
        for min_pk in range(0, max_pk, batch_size):
            get_batch(cursor, min_pk, batch_size)
            print('Done with min pk %s', min_pk)
        print('Now uploading files to s3')
        for year, fh in filehandles.items():
            upload(
                s3_conn,
                filename(year),
                '{}/{}.gz'.format(output_s3_prefix, year)
            )
    finally:
        for fh in filehandles.values():
            fh.close()
