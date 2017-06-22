import gzip
import logging
import unicodecsv as csv
from datetime import datetime

from skills_utils.job_posting_import import JobPostingImportBase
from skills_utils.time import overlaps, quarter_to_daterange
from skills_utils.s3 import download


class NLXTransformer(JobPostingImportBase):
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, s3_prefix, temp_file_path, **kwargs):
        super(NLXTransformer, self).__init__(**kwargs)
        self.s3_prefix = s3_prefix
        self.temp_file_path = temp_file_path

    def _iter_postings(self, quarter):
        """Iterate through NLX postings for the given quarter.

        NLX postings are written in CSV form to a gzipped file,
        and stored on s3.
        """
        logging.info("Finding NLX postings for %s", quarter)
        quarter_start, quarter_end = quarter_to_daterange(quarter)
        s3_path = '{}/{}.gz'.format(self.s3_prefix, quarter)
        local_path = '{}/{}.gz'.format(self.temp_file_path, quarter)

        download(self.s3_conn, local_path, s3_path)
        in_file = 0
        overlapping = 0
        with gzip.open(local_path) as gzfile:
            reader = csv.DictReader(gzfile)
            for posting in reader:
                in_file += 1
                # sanity check: make sure the quarter in the posting matches
                # if the sync worked correctly this should be 100% though
                listing_time = datetime.strptime(
                    posting['dateacquired'],
                    self.DATE_FORMAT
                )
                if overlaps(
                    listing_time.date(),
                    listing_time.date(),
                    quarter_start,
                    quarter_end
                ):
                    overlapping += 1
                    yield posting
            logging.info(
                '%s matched out of %s total in file',
                overlapping,
                in_file
            )

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
        transformed['datePosted'] = start.date().isoformat()
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
                'maxValue': document.get('maxSalary', '')
            }
        if 'maxPositions' in document:
            transformed['numPositions'] = document['maxPositions']
        return transformed
