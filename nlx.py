import gzip
import logging
import unicodecsv as csv
from datetime import datetime
import s3fs
import os
import json
from airflow.hooks import S3Hook
from skills_ml.job_postings.aggregate.dataset_transform import DatasetStatsCounter


class NLXTransformer(object):
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, s3_prefix, temp_file_path=None):
        self.s3_prefix = s3_prefix
        self.temp_file_path = temp_file_path or 'tmp'

    def transformed_postings(self, year, stats_counter=None):
        for posting in self.raw_postings(year):
            transformed = self._transform(posting)
            transformed['id'] = '{}_{}'.format(
                self.partner_id,
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
        s3_path = '{}/{}.gz'.format(self.s3_prefix, year)
        local_path = '{}/{}.gz'.format(self.temp_file_path, year)
        try:
            s3 = s3fs.S3FileSystem()
            s3.get(s3_path, local_path)
        except Exception as e:
            logging.warning('No source file found at %s, skipping extraction', s3_path)
            return
        in_file = 0
        with gzip.open(local_path) as gzfile:
            reader = csv.DictReader(gzfile)
            for posting in reader:
                in_file += 1
                yield posting
            logging.info('%s total in file', in_file)

    def _id(self, document):
        return document['jobID']

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
            transformed['numPositions'] = document['maxPositions']
        return transformed


if __name__ == '__main__':
    transformer = NLXTransformer(
        s3_prefix='open-skills-private/NLX_extracted',
        temp_file_path='/mnt/sqltransfer'
    )
    #for year in ('2003', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2016', '2017', '2018'):
    for year in ('2003', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013'):
        stats_counter = DatasetStatsCounter(
            quarter=year,
            dataset_id='NLX'
        )
        logging.info('Processing year %s', year)
        for posting in transformer.transformed_postings(year):
            try:
                partitioned_key = posting['id'][-4:]
            except Exception as e:
                logging.warning('No partition key available! Choosing fallback')
                partitioned_key = '-1'
            partitioned_file = '/mnt/sqltransfer/{}/{}.txt'.format(year, partitioned_key)
            os.makedirs('/mnt/sqltransfer/{}/'.format(year), exist_ok=True)
            with open(partitioned_file, 'ab') as f:
                f.write(json.dumps(posting))
                f.write('\n')
        logging.info('Done with year %s, saving stats', year)

        stats_counter.save(
            s3_conn=S3Hook().get_conn(),
            s3_prefix='open-skills-private/job_posting_stats'
        )
