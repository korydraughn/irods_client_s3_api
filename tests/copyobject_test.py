import unittest
import subprocess as sp
import botocore
import botocore.session

from tests.utility import *


class CopyObject_Test(unittest.TestCase):
    def setUp(self):
        session = botocore.session.get_session()
        self.client = session.create_client("s3",
                                            use_ssl=False,
                                            endpoint_url="http://127.0.0.1:8080",  # normal networking stuff :p
                                            aws_access_key_id="no",
                                            aws_secret_access_key="heck")
        set_access("", 'own', recursive=True)
        mkdir("", access_level="own")

    def test_copy_succeeds(self):
        """
        Tests that that putobject works properly in the successful case.
        """

        OBJECT_PATH = "something"
        OBJECT_KEY = "test/something"
        OBJECT_KEY2 = "test/something3"
        self.client.put_object(
            Key=OBJECT_KEY, Body=b"this should succeed", Bucket="wow")
        self.assertEqual(read_file(self.client, OBJECT_KEY)["Body"].read(),
                         b"this should succeed")
        self.client.copy_object(CopySource=OBJECT_KEY, Bucket="wow", Key=OBJECT_KEY2)
        self.assertEqual(read_file(self.client, OBJECT_KEY2)["Body"].read(),
                         b"this should succeed")
        remove_file(self.client, OBJECT_PATH)
        remove_file(self.client, OBJECT_PATH + '3')
