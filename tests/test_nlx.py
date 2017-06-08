from moto import mock_s3
import boto
from skills_utils.testing import ImporterTest
from skills_utils.s3 import split_s3_path
from nlx import NLXTransformer

from io import BytesIO
import tempfile
import gzip


@mock_s3
class NLXImporterTest(ImporterTest):
    transformer_class = NLXTransformer
    sample_input_document = {
        'jobDuration': '',
        'zipcode': '17837',
        'license': '',
        'description': """Auto req ID 37632BR\n  \n**Posting Title**\n  \n Teller\n  \n**Business Unit**\n  \n Community Banking Div Group\n  \n**Address**\n  \n Lewisburg, PA \\- 6901 West Branch Highway\n  \n**Language Fluency**\n  \n English\n  \n**Full\\-Time/Part\\-Time**\n  \n Full time\n  \n**Regular/Temporary**\n  \n Regular\n  \n**Shift**\n  \n 1st shift\n  \n**Scheduled Weekly Hours**\n  \n 40\n  \n**Primary Purpose**\n  \n Participate in daily operational function of branch Teller being responsible for delivery of superior quality service while adhering to corporate, regulatory, and audit guidelines\\. Provide timely and efficient completion of client transactions while maintaining accurate records and thorough proper handling of all monies assigned\\. Proactively participate in the sales/quality referral process of the branch as directed by management\\.\n  \n**Job Description**\n  \n**Essential Duties and Responsibilities:**\n  \nThe following is a summary of the essential job functions for this job\\. Other duties may be performed, both major and minor, which are not mentioned below\\. Specific activities may change from time to time\\.\n  \n1\\. Provide professional client service, which includes but is not limited to: performing accurate transactions, greeting the client, smiling, using the client's name during the transaction, and thanking each client for his or her business\\. Refer clients to other branch personnel as needed\\.\n  \n2\\. Perform the basic transactions of a paying and receiving teller such as accepting deposits and loan payments, verifying cash and endorsements, cashing checks within limits and obtaining further authorization when necessary, issuing money orders, cashiers checks, and redeeming savings bonds\\.\n  \n3\\. Prepare individual daily balance of teller cash transactions as well as other reports as necessary\\.\n  \n4\\. Perform more complex transactions \\(with assistance as necessary\\) such as:\n  \na\\. Coupon Collection\n  \nb\\. Issuing Official Checks/Money Orders\n  \nc\\. Large Commercial Deposits\n  \nd\\. Close Out Transactions\n  \ne\\. Cash Advances\n  \n5\\. At the discretion and direction of the supervisor, responsible for collecting his or her own cash items\\.\n  \n6\\. Follow all operating procedures as outlined in Branch Operations Manual \\(BOM\\)\\.\n  \n7\\. Handle proportionate volume of work based on branch demands\\.\n  \n**Required Skills and Competencies:**\n  \nThe requirements listed below are representative of the knowledge skill and or ability required\\. Reasonable accommodations may be made to enable individuals with disabilities to perform essential functions\\.\n  \n1\\. High School diploma or equivalent\n  \n2\\. Ability to complete teller training in required time frame\n  \n3\\. Demonstrated ability to read, follow written instructions and accurately complete written reports\n  \n4\\. Good interpersonal skills\n  \n5\\. Ability to use office machines and perform basic mathematical functions\n  \n6\\. Demonstrated ability to deliver good client service and provide team support\n  \n7\\. Ability to complete Bank training program for Teller\n  \n8\\. Willingness to travel to accommodate temporary staffing needs as required\n  \n9\\. Capability to lift a minimum of 30 lbs\n  \n10\\. Ability to speak fluent English\n  \n**BB&T is an Equal Opportunity Employer and considers all qualified applicants regardless of race, gender, color, religion, national origin, age, sexual orientation, gender identity, disability, veteran status or other classification protected by law\\.**""",
        'jobID': '256621',
        'dateacquired': '2003-01-24 00:00:00',
        'minSalary': '',
        'fedcontractor': '1',
        'expiryDate': '2015-11-17 00:00:00',
        'salaryUnit': '',
        'naics_code': '',
        'companyAddress': '',
        'maxPositions': '',
        'companyZipcode': '',
        'experience': '',
        'title': 'Teller',
        'city': 'Lewisburg',
        'shift': '',
        'maxSalary': '',
        'country': 'US',
        'createddate': '2015-11-16 00:00:00',
        'fein': '',
        'onet_code': '43-3071.00',
        'companyState': '',
        'weeklyHours': '',
        'state': 'PA',
        'sourceState': '',
        'expired': '1',
        'company': '',
        'training': '',
        'companyCountry': '',
        'companyCity': '',
        'link': 'http://my.jobs/24a4c8de278a4ea492b8027be6d8cfe01087',
        'min_education': '',
        'lastUpdatedDate': '2015-11-16 00:00:00',
        'generatedJobId': '1860650'
    }

    def setUp(self):
        self.connection = boto.connect_s3()
        self.s3_prefix = 'test-bucket/akey/'
        self.temp_dir = tempfile.TemporaryDirectory()
        self.transformer = self.transformer_class(
            s3_prefix=self.s3_prefix,
            temp_file_path=self.temp_dir.name,
            partner_id='NLX',
            s3_conn=self.connection,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_iterate(self):
        """Test that records from all files are properly returned or excluded
        according to the given date range

        This is explicitly testing edge cases; under normal operation
        each file will just contain records for the specified quarter 
        """
        bucket_name, prefix = split_s3_path(self.s3_prefix)
        bucket = self.connection.create_bucket(bucket_name)
        mock_data = {
            '2014Q4.gz': [
                {'dateacquired': '2014-12-15 00:00:00'},
                {'dateacquired': '2014-11-15 00:00:00'},
                {'dateacquired': '2015-01-15 00:00:00'},
            ],
            '2015Q1.gz': [
                {'dateacquired': '2014-12-15 00:00:00'},
                {'dateacquired': '2014-11-15 00:00:00'},
                {'dateacquired': '2015-01-15 00:00:00'},
            ]
        }

        for keyname, rows in mock_data.items():
            key = boto.s3.key.Key(
                bucket=bucket,
                name='{}/{}'.format(prefix, keyname)
            )
            stream = BytesIO()
            gzipfile = gzip.GzipFile(fileobj=stream, mode='w')
            gzipfile.write(b'dateacquired\n')
            for row in rows:
                gzipfile.write(row['dateacquired'].encode('utf-8'))
                gzipfile.write(b'\n')
            gzipfile.close()
            stream.seek(0)
            key.set_contents_from_file(stream)

        self.assert_num_postings_for_quarter('2015Q1', 1)
        self.assert_num_postings_for_quarter('2014Q4', 2)

    def assert_num_postings_for_quarter(self, quarter, expected):
        postings = [
            posting
            for posting in self.transformer._iter_postings(quarter=quarter)
        ]
        assert len(postings) == expected

    def test_transform(self):
        """Test that the required fields are properly mapped
        from the input data"""
        transformed = self.transformer._transform(self.sample_input_document)
        assert transformed['title'] == 'Teller'
        assert transformed['jobLocation']['address']['addressLocality'] == 'Lewisburg'
        assert transformed['jobLocation']['address']['addressRegion'] == 'PA'
        assert transformed['datePosted'] == '2003-01-24'
        assert transformed['onet_soc_code'] == '43-3071.00'
        assert 'Proactively participate in the sales/quality referral process of the branch as directed by management' in transformed['description']
