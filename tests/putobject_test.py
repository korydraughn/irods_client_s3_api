import unittest
import subprocess as sp
import botocore
import botocore.session

from tests.utility import *


class PutObject_Test(unittest.TestCase):

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
        remove_file(self.client, '', recursive=True)
        self.client.close()

    def test_put_succeeds(self):
        """
        Tests that that putobject works properly in the successful case.
        """

        OBJECT_PATH = "something"
        OBJECT_KEY = "test/something"
        self.client.put_object(
            Key=OBJECT_KEY, Body=b"this should succeed", Bucket="wow")
        self.assertEqual(read_file(self.client, OBJECT_KEY)["Body"].read(),
                         b"this should succeed")
        remove_file(self.client, OBJECT_PATH)

    def test_put_fails(self):
        """
        Tests that that putobject works properly in the failure case.
        """

        # Check if the permissions of the directory are properly honored. Namely,
        # it should return a 403 status code if it cannot write.
        OBJECT_PATH = "something"
        OBJECT_KEY = "test/something"
        set_access('', 'read_metadata', recursive=True)
        self.assertRaises(Exception,
                          lambda: self.client.put_object(
                              Key=OBJECT_KEY, Body=b"this should fail", Bucket="wow"))
        set_access('', 'own', recursive=True)
        remove_file(self.client, OBJECT_PATH)

        # This is overwriting a file directly that the user cannot overwrite. 
        # It should provide a 403 status code
        touch_file(OBJECT_PATH, access_level='read_metadata')
        self.assertRaises(Exception, lambda: self.client.put_object(
            Key=OBJECT_KEY, Body=b"this should fail", Bucket='wow'))
        set_access(OBJECT_PATH, "own")
        remove_file(self.client, OBJECT_PATH)
