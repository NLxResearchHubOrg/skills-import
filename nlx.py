import gzip
import logging
import unicodecsv as csv
from datetime import datetime
import s3fs
import os
import json
import sys
import csv

#from airflow.hooks import S3Hook
#from skills_ml.job_postings.aggregate.dataset_transform import DatasetStatsCounter


class NLXTransformer(object):
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, s3_prefix, temp_file_path=None):
        #self.s3_prefix = s3_prefix
        #self.temp_file_path = temp_file_path or 'tmp'
        pass

    def transformed_postings(self, year, stats_counter=None):
        for posting in self.raw_postings(year):
            if not _has_all_required_fields(posting):
                continue
            transformed = self._transform(posting)
            transformed['id'] = '{}_{}'.format(
                #self.partner_id,
                '12345',
                self._id(posting)
            )
            if stats_counter:
                stats_counter.track(
                    input_document=posting,
                    output_document=transformed
                )
            yield transformed

    def raw_postings(self, year):
        """Iterate through NLX postings for the given year.

        NLX postings are written in CSV form to a gzipped file,
        and stored on s3.
        """
        logging.info("Finding raw NLX postings for %s", year)
        #s3_path = '{}/{}.gz'.format(self.s3_prefix, year)
        #local_path = '{}/{}.gz'.format(self.temp_file_path, year)
        local_path = 'C:/Users/Public/Documents/{}.gz'.format(year)
        logging.info(local_path)
        #try:
         #   s3 = s3fs.S3FileSystem()
        #    s3.get(s3_path, local_path)
        #except Exception as e:
        #    logging.warning('No source file found at %s, skipping extraction', s3_path)
        #    return
        in_file = 0
        with gzip.open(local_path, 'rt', encoding="cp437") as gzfile:
            reader = csv.DictReader(gzfile)
            try:
                for posting in reader:
                    in_file += 1
                    if in_file % 10000 == 0:
                        logging.info(year, in_file)
                    yield posting
            except EOFError:
                logging.error('EOF error was reached')
            except Exception as e:
                logging.error(e)
            logging.info('%s total in file', in_file)

    def _id(self, document):
        return document['jobID']

    def _has_all_required_fields(self, document):
        required_fields = [
            'datePosted',
            'title',
            'jobLocation',
            # 'hiringOrganization',
            'description'
        ]

        for field in required_fields:
            if field not in document.keys():
                return False
        
        return True

    def _transform(self, document):
        transformed = {
            "@context": "http://schema.org",
            "@type": "JobPosting",
        }
        basic_mappings = {
            'title': 'title',
            'description': 'description',
            'educationRequirements': 'min_education',
            'experienceRequirements': 'experience',
            'qualifications': 'license',
            'skills': 'training',
            'onet_soc_code': 'onet_code',
            'industry': 'naics_code',
        }
        for target_key, source_key in basic_mappings.items():
            transformed[target_key] = document.get(source_key, '')

        start = datetime.strptime(document['dateacquired'], self.DATE_FORMAT)
        transformed['datePosted'] = start.date().year
        if 'city' in document or 'state' in document:
            transformed['jobLocation'] = {
                '@type': 'Place',
                'address': {
                    '@type': 'PostalAddress',
                    'addressLocality': document.get('city', ''),
                    'addressRegion': document.get('state', ''),
                }
            }
        if 'minSalary' in document or 'maxSalary' in document:
            transformed['baseSalary'] = {
                '@type': 'MonetaryAmount',
                'minValue': document.get('minSalary', ''),
                'maxValue': document.get('maxSalary', ''),
                'salaryFrequency': document.get('salaryUnit', '')  # not standard! but we need it!
            }
        if 'maxPositions' in document:
            transformed['numPositions'] = document['maxPositions'] # should be an int // change numPositions to totalJobOpenings
        return transformed


if __name__ == '__main__':

    # maxInt = sys.maxsize

    # while True:
        # # decrease the maxInt value by factor 10 
        # # as long as the OverflowError occurs.

        # try:
            # csv.field_size_limit(maxInt)
            # break
        # except OverflowError:
            # maxInt = int(maxInt/10)
    import ctypes
    csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
    
    transformer = NLXTransformer(
        s3_prefix='open-skills-private/NLX_extracted',
        temp_file_path='/mnt/sqltransfer'
    )
    logging.basicConfig(level=logging.INFO)
    
    #logging.info('max csv size set to {}'.format(maxInt))
    #for year in ('2003', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2016', '2017', '2018'):
    #for year in ('2003', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013'):
    #for year in ('2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013'):
    for year in range(2003, 2020):
        year = str(year)
        #stats_counter = DatasetStatsCounter(
        #    quarter=year,
        #    dataset_id='NLX'
        #)
        # check if file exists
        if not os.path.exists('C:/Users/Public/Documents/{}.gz'.format(year)):
            logging.info('File for year {} does not exist, skipping'.format(year))
            continue
        logging.info('Processing year %s', year)
        for posting in transformer.transformed_postings(year):
            try:
                partitioned_key = posting['id'][-4:]
            except Exception as e:
                logging.warning('No partition key available! Choosing fallback')
                partitioned_key = '-1'
            #partitioned_file = '/mnt/sqltransfer/{}/{}.txt'.format(year, partitioned_key)
            #os.makedirs('/mnt/sqltransfer/{}/'.format(year), exist_ok=True)
            partitioned_file = 'C:/Users/Public/Documents/sqltransfer/{}/{}.txt'.format(year, partitioned_key)
            os.makedirs('C:/Users/Public/Documents/sqltransfer/{}/'.format(year), exist_ok=True)
            with open(partitioned_file, 'a') as f:
                f.write(json.dumps(posting))
                f.write('\n')
        logging.info('Done with year %s, saving stats', year)

        #stats_counter.save(
        #    s3_conn=S3Hook().get_conn(),
        #    s3_prefix='open-skills-private/job_posting_stats'
        #)
