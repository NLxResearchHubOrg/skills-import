"""
Test the CareerBuilder ETL transformer
"""
from skills_utils.testing import ImporterTest
from career_builder import CareerBuilderTransformer

from moto import mock_s3
import boto

from io import StringIO
import contextlib
import csv
import json
import tempfile

OCCUPATION_CONTENT = [
    ['O*NET-SOC Code', 'Title', 'Description'],
    ['11-1011.00', 'Chief Executives', 'Not important'],
    ['11-1011.03', 'Chief Sustainability Officers', 'Not important'],
    ['11-1021.00', 'General and Operations Managers', 'Not important'],
    ['11-1031.00', 'Legislators', 'Not important'],
]


class MockOnetCache(object):
    @contextlib.contextmanager
    def ensure_file(self, dataset):
        fake_data_lookup = {
            'Occupation Data.txt': OCCUPATION_CONTENT,
        }
        tf = tempfile.NamedTemporaryFile(delete=False)
        with open(tf.name, 'w') as write_stream:
            writer = csv.writer(write_stream, delimiter='\t')
            for row in fake_data_lookup[dataset]:
                writer.writerow(row)

        yield tf.name

        tf.close()


@mock_s3
class CareerBuilderTest(ImporterTest):
    transformer_class = CareerBuilderTransformer
    sample_input_document = {
        "hashdid": -260666572,
        "jobtitle": "IT Risk and Assurance Senior",
        "created": "2014-03-06",
        "modified": "2014-04-06",
        "firstcategory": "Information Technology",
        "secondcategory": "Accounting",
        "thirdcategory": "Customer Service",
        "firstindustry": "Accounting - Finance",
        "secondindustry": "Consulting",
        "thirdindustry": "Services - Corporate B2B",
        "employmenttype": "Full-Time",
        "reqtravel": "Not Specified",
        "reqdegree": "Graduate Degree",
        "paybaseh": 75000.0,
        "paybasel": 65000.0,
        "paybonus": 0.0,
        "paycom": 0.0,
        "totalpay": 75000.0,
        "payper": "Year",
        "payother": "$5,000 bouns potential",
        "textpay": "$65,000 - $75,000\/Year",
        "productlevel": "City",
        "cityname": "Pittsburgh",
        "statename": "PA",
        "countryname": "US",
        "rn_location": "US-PA-Pittsburgh",
        "management": False,
        "screener": False,
        "jobskin": True,
        "joblogo": False,
        "talentnetwork": False,
        "startupflag": False,
        "relocate": False,
        "applyurl": False,
        "jobdesc": "information technology - risk - ERP auditing - assessment - public accounting A large, international public accounting firm is searching for an IT Risk and Assurance Senior.\u00a0 The IT Risk and Assurance Senior will develop program timelines and assessments in order to mitigate potential risks for the business processes dependent on IT.\u00a0 The IT Risk and Assurance Senior will lead and advise clients to maximum efficiency and minimal risk in their information systems.",
        "jobreq": "An ideal candidate for the IT Risk and Assurance Senior position will have: ERP auditing experience 1-8 years in public accounting or internal audit a 4-year degree in accounting\/finance or computer science\/information systems Experience in SAP, Oracle, data analytics, data security, and privacy is a plus for the IT Risk and Assurance Senior.",
        "carotenetitle": "Risk Manager",
        "onettitle": "Legislators"
    }

    def setUp(self):
        self.connection = boto.connect_s3()
        self.bucket_name = 'test'
        self.prefix = 'akey'
        self.transformer = self.transformer_class(
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            partner_id='CB',
            onet_cache=MockOnetCache(),
            s3_conn=self.connection,
        )

    def test_iterate(self):
        """Test that records from all files are properly returned or excluded
        according to the given date range"""
        bucket = self.connection.create_bucket(self.bucket_name)
        mock_data = {
            'fileone': [
                {'created': '2014-12-15', 'modified': '2015-01-15'},
                {'created': '2014-11-15', 'modified': '2014-12-15'},
                {'created': '2015-01-15', 'modified': '2015-02-15'},
            ],
            'filetwo': [
                {'created': '2014-12-15', 'modified': '2015-01-15'},
                {'created': '2014-11-15', 'modified': '2014-12-15'},
                {'created': '2015-01-15', 'modified': '2015-02-15'},
            ]
        }

        for keyname, rows in mock_data.items():
            key = boto.s3.key.Key(
                bucket=bucket,
                name='{}/{}'.format(self.prefix, keyname)
            )
            stream = StringIO()
            for row in rows:
                stream.write(json.dumps(row))
                stream.write('\n')
            stream.seek(0)
            key.set_contents_from_file(stream)

        postings = [
            posting
            for posting in self.transformer._iter_postings(quarter='2015Q1')
        ]
        assert len(postings) == 4

    def test_transform(self):
        """Test that the required fields are properly mapped
        from the input data"""
        transformed = self.transformer._transform(self.sample_input_document)
        assert transformed['title'] == 'IT Risk and Assurance Senior'
        assert transformed['jobLocation']['address']['addressLocality'] == 'Pittsburgh'
        assert transformed['jobLocation']['address']['addressRegion'] == 'PA'
        assert transformed['datePosted'] == '2014-03-06'
        assert transformed['validThrough'] == '2014-04-06T00:00:00'
        assert transformed['onet_soc_code'] == '11-1031.00'
        assert 'A large, international public' in transformed['description']
