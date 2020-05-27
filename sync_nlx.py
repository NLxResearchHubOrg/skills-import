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
s3_keys = {}
start_time = datetime.now()

def print_elapsed_time():
    global start_time
    duration = datetime.now() - start_time
    duration_in_s = duration.total_seconds()
    hours = divmod(duration_in_s, 3600)[0]  
    mins = divmod(duration_in_s, 60)[0]
    print('Elapsed: {} hours / {} mins'.format(hours, mins))

def filename(quarter_string): # TODO create folder if not exist
    return 'C:/Output/1/{}_sanitized_oct2015.csv'.format(quarter_string)


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


SANITIZED_QUERY = """
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
[lastUpdatedDate],
[sourceState],
[title],
[fileName],
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
addr.city as companyCity,
addr.state as companyState,
addr.zipcode as companyZipcode,
addr.country as companyCountry
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
WHERE [dateacquired] BETWEEN Convert(datetime, '2017-01-01' ) AND Convert(datetime, '2018-12-31 23:59:59')
AND j.generatedJobId >= {min_pk} and j.generatedJobId < {max_pk}
"""

UNSANITIZED_QUERY = """
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
[fileName].
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
FROM [LMI-April2018].[dbo].[Job] j
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
WHERE [dateacquired] BETWEEN Convert(datetime, '2020-01-01' ) AND Convert(datetime, '2020-12-31 23:59:59')
AND j.generatedJobId >= {min_pk} and j.generatedJobId < {max_pk}
"""


def row_to_dict(row):
    return dict(zip([t[0] for t in row.cursor_description], row))

i = 0
total = 0

def get_batch(cursor, min_pk, batch_size):
    bound_query = SANITIZED_QUERY.format(min_pk=min_pk, max_pk=min_pk+batch_size) # TODO add db
    cursor.execute(bound_query)
    cursor_desc = dict(
        (field[0], field[0])
        for field in cursor.description
    )

    global i
    global total
    #items = cursor.fetchall()
    #print(items)
    temp = i
    while temp != 0:
        cursor.fetchone()
        temp-=1
        
    for row in cursor: # this is what takes forever
        #print(row)
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
    #batch_size = 10
    disconnect = False
    for min_pk in range(0, max_pk, batch_size):
        while True:
            try:
                print('yea top of try')
                print('now: ', str(datetime.now()))
                print_elapsed_time()
                if disconnect:
                    #conn = pymssql.connect(server="EC2AMAZ-VG341JF")
                    #time.sleep(60)
                    pass
                    
                #conn = pymssql.connect(server="EC2AMAZ-VG341JF")
                server = 'EC2AMAZ-AAAAA' 
                database = 'LMI-Oct2015' 
                username = '' 
                password = ''
                connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

                # connection = pymssql.connect(
                    # server='EC2AMAZ-VG341JF',
                    # user='pytest',
                    # password='',
                    # database='LMI-April2018',
                    # login_timeout=3
                # )
                print('1')
                cursor = connection.cursor()
                print('2')
                get_batch(cursor, min_pk, batch_size)
                print('3')
                print('Done with min pk', min_pk)

                # close_files()
                # connection.close()
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
                
        #print('Now uploading files to s3')
        #for year, fh in filehandles.items():
        #    upload(
        #        s3_conn,
        #        filename(year),
        #        '{}/{}.gz'.format(output_s3_prefix, year)
        #    )
    # except _mssql.MssqlDriverException:
        # print("A MSSQLDriverException has been caught.")

    # except _mssql.MssqlDatabaseException as e:
        # print("A MSSQLDatabaseException has been caught.")
        # print('Number = ',e.number)
        # print('Severity = ',e.severity)
        # print('State = ',e.state)
        # print('Message = ',e.message)
        
    # except Exception as e:
        # try:
            # if int(str(e)[1:6]) == 20047:
                # print('caught a disconnect')
        # except ValueError:
            # pass
            
        # raise e
        
    # finally:
        # print('closing files...')
        # close_files()
            
        # print('closed files')
        # print('closing connection...')
        # connection.close()
        # print('connection closed.')

run()
