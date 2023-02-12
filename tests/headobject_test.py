import unittest
import subprocess as sp
import botocore
import botocore.session

from tests.utility import *


class TestHeadObject(unittest.TestCase):

    def setUp(self):
        session = botocore.session.get_session()
        self.client = session.create_client("s3",
                                            use_ssl=False,
                                            endpoint_url="http://127.0.0.1:8080",  # normal networking stuff :p
                                            aws_access_key_id="no",
                                            aws_secret_access_key="heck")
        set_access("", 'own', recursive=True)
        mkdir("", access_level="own")

    def tearDown(self):
        set_access("", "own", recursive=True)
        # remove_file(self.client, "", recursive=True)
        self.client.close()

    def test_permission(self):
        "Test permission support in headobject"
        # Create file that cannot be read by the current user
        self.client.put_object(Bucket="wow", Key="test/Hi", Body=b"Hello")
        set_access("Hi", "null")
        # Read the object :)
        self.assertRaises(Exception,  # This is very much not a good thing, but
                          # but botocore is rather not ideal for this.
                          lambda: print(self.client.head_object(Key="test/Hi", Bucket="wow")))

    def test_head_succeeds(self):
        set_access("", "own", recursive=True)
        self.client.put_object(Bucket="wow", Key="test/hi", Body=b"Hello")
        self.client.head_object(Bucket="wow", Key="test/hi")

    def test_head_fails(self):
        self.assertRaises(botocore.exceptions.ClientError, lambda: self.client.head_object(Bucket="wow", Key="test/hi2"))

