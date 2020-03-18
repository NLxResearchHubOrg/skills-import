import pymssql
from skills_utils.time import datetime_to_year_quarter
from skills_utils.s3 import upload
import unicodecsv as csv
import gzip
from os import getenv
import _mssql
import time 
import pyodbc
from datetime import datetime
import os.path


filehandles = {}
writers = {}
start_time = datetime.now()
database = "" # Do not modify here, modify in run function


def print_elapsed_time():
    global start_time
    duration = datetime.now() - start_time
    duration_in_s = duration.total_seconds()
    hours = divmod(duration_in_s, 3600)[0]  
    mins = divmod(duration_in_s, 60)[0]
    print('Elapsed: {} hours / {} mins'.format(hours, mins))


def filename(quarter_string):
    return 'C:/Output/1/{}.csv'.format(quarter_string)


def output_writer(row, cursor_desc):
    #print(row)
    global writers
    global filehandles
    year, _ = datetime_to_year_quarter(row.dateacquired)
    year_string = str(year)
    if year_string not in writers:
        if os.path.isfile(filename(year_string)):
            new_file = False
        else:
            new_file = True
            
        filehandles[year_string] = open(filename(year_string), 'ab')
            
        writers[year_string] = \
            csv.DictWriter(filehandles[year_string], fieldnames=cursor_desc)

        if new_file:
            print('writing header')
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
FROM [{database_name}].[dbo].[Job] j
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


def row_to_dict(row):
    return dict(zip([t[0] for t in row.cursor_description], row))

# These global variables will attempt to restore the query state
# if this encounters a disconnect
i = 0  
total = 0


def get_batch(cursor, min_pk, batch_size):
    nonlocal database
    bound_query = QUERY.format(
        database_name=database,
        min_pk=min_pk,
        max_pk=min_pk+batch_size)
    cursor.execute(bound_query)
    cursor_desc = dict(
        (field[0], field[0])
        for field in cursor.description
    )

    # Restore cursor state if we dropped connection
    global i
    global total
    temp = i
    while temp != 0:
        cursor.fetchone()
        temp-=1
        
    for row in cursor:
        i += 1
        total += 1
        if i % 100000 == 0:
            print(f'{total:,} -- batch procesing {i:,}')
        output_writer(row, cursor_desc).writerow(row_to_dict(row))
    
    i = 0


def close_files():
    global filehandles
    global writers
    for fh in filehandles.values():
        fh.close()
        
    filehandles = {}
    writers = {}


def run():
    max_pk = 52602456
    batch_size = 1000000
    disconnect = False
    for min_pk in range(0, max_pk, batch_size):
        while True:
            try:
                print('Time: ', str(datetime.now()))
                print_elapsed_time()
                nonlocal database
                    
                server = 'EC2AMAZ-U6JE5SD'
                database = 'LMI'
                username = 'pytest' 
                password = '' 
                connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

                print('1')
                cursor = connection.cursor()
                print('2')
                get_batch(cursor, min_pk, batch_size)
                print('3')
                print('Done with min pk', min_pk)

                break

            except pyodbc.OperationalError as e:
                print('caught a disconnect')
                print(e)
                try:
                    cursor.close()
                except:
                    pass
                
                try:
                    connection.close()
                except:
                    pass
                
            except Exception as e:    
                print(e)
            
            finally:
                close_files()

run()