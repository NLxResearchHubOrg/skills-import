import csv
from datetime import datetime
import logging
import tempfile

from skills_utils.io import stream_json_file
from skills_utils.job_posting_import import JobPostingImportBase
from skills_utils.time import overlaps, quarter_to_daterange


def log_download_progress(num_bytes, obj_size):
    logging.info('%s bytes transferred out of %s total', num_bytes, obj_size)


class CareerBuilderTransformer(JobPostingImportBase):
    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, bucket_name=None, prefix=None, **kwargs):
        super(CareerBuilderTransformer, self).__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.soc_code_lookup = self._create_soc_code_lookup()

    def _create_soc_code_lookup(self):
        with self.onet_cache.ensure_file('Occupation Data.txt') as title_filename:
            lookup = {}
            with open(title_filename) as f:
                reader = csv.reader(f, delimiter='\t')
                next(reader)
                for row in reader:
                    lookup[row[1]] = row[0]
        return lookup

    def _iter_postings(self, quarter):
        logging.info("Finding CareerBuilder postings for %s", quarter)
        quarter_start, quarter_end = quarter_to_daterange(quarter)
        bucket = self.s3_conn.get_bucket(self.bucket_name)
        keylist = list(bucket.list(prefix=self.prefix, delimiter=''))
        for key in keylist:
            in_file = 0
            overlapping = 0
            logging.info('Processing key %s', key.name)
            with tempfile.NamedTemporaryFile() as local_file:
                key.get_contents_to_file(local_file, cb=log_download_progress)
                logging.info('Downloaded key %s for processing', key.name)
                local_file.seek(0)
                for posting in stream_json_file(local_file):
                    in_file += 1
                    listing_start = datetime.strptime(
                        posting['created'],
                        self.DATE_FORMAT
                    )
                    listing_end = datetime.strptime(
                        posting['modified'],
                        self.DATE_FORMAT
                    )
                    if overlaps(
                        listing_start.date(),
                        listing_end.date(),
                        quarter_start,
                        quarter_end
                    ):
                        overlapping += 1
                        yield posting
                logging.info(
                    '%s overlapping out of %s total in file',
                    overlapping,
                    in_file
                )

    def _id(self, document):
        return document['hashdid']

    def _transform(self, document):
        transformed = {
            "@context": "http://schema.org",
            "@type": "JobPosting",
        }
        basic_mappings = {
            'title': 'jobtitle',
            'description': 'jobdesc',
            'educationRequirements': 'reqdegree',
            'employmentType': 'employmenttype',
            'experienceRequirements': 'jobreq',
            'incentiveCompensation': 'payother',
            'qualifications': 'jobreq',
            'alternateName': 'carotenetitle',
            'occupationalCategory': 'onettitle',
        }
        for target_key, source_key in basic_mappings.items():
            transformed[target_key] = document.get(source_key)

        start = datetime.strptime(document['created'], self.DATE_FORMAT)
        end = datetime.strptime(document['modified'], self.DATE_FORMAT)
        transformed['datePosted'] = start.date().isoformat()
        transformed['validThrough'] = end.isoformat()
        transformed['jobLocation'] = {
            '@type': 'Place',
            'address': {
                '@type': 'PostalAddress',
                'addressLocality': document['cityname'],
                'addressRegion': document['statename'],
            }
        }
        transformed['baseSalary'] = {
            '@type': 'MonetaryAmount',
            'minValue': document.get('paybasel'),
            'maxValue': document.get('paybaseh')
        }
        transformed['skills'] = ', '.join([
            document.get('firstcategory'),
            document.get('secondcategory'),
            document.get('thirdcategory')
        ])
        transformed['industry'] = ', '.join([
            document.get('firstindustry'),
            document.get('secondindustry'),
            document.get('thirdindustry')
        ])
        transformed['onet_soc_code'] = self.soc_code_lookup.get(
            document.get('onettitle')
        )
        return transformed
