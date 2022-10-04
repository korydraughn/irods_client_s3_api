import unittest
import subprocess as sp
import botocore
import botocore.session

from tests.utility import *


class CopyObject_Test(unittest.TestCase):
    def setUp(self):
        session = botocore.session.get_session()
        # To enable logging of the signature steps, you can uncomment this line
        # session.set_stream_logger('botocore.auth', botocore.logging.DEBUG)
        self.client = session.create_client("s3",
                                            use_ssl=False,
                                            endpoint_url="http://127.0.0.1:8080",  # normal networking stuff :p
                                            aws_access_key_id="no",
                                            aws_secret_access_key="heck")
        set_access("", 'own', recursive=True)
        mkdir("", access_level="own")

    def tearDown(self) -> None:
        self.client.close()
        # set_access("", "own", recursive=True)
        # remove_file(self.client, "", recursive=True)

    def test_copy_succeeds(self):
        """
        Tests that copyobject works properly in the successful case.
        """

        OBJECT_PATH = "something"
        OBJECT_PATH2 = OBJECT_PATH + "3"
        OBJECT_KEY = "test/" + OBJECT_PATH
        OBJECT_KEY2 = "test/" + OBJECT_PATH2
        self.client.put_object(
            Key=OBJECT_KEY, Body=b"this should succeed", Bucket="wow")
        self.assertEqual(read_file(self.client, OBJECT_KEY)["Body"].read(),
                         b"this should succeed")
        self.client.copy_object(CopySource={"Bucket": "wow", "Key": OBJECT_KEY}, Bucket="wow", Key=OBJECT_KEY2)
        self.assertEqual(read_file(self.client, OBJECT_KEY2)["Body"].read(),
                         b"this should succeed")
        remove_file(self.client, OBJECT_PATH)
        remove_file(self.client, OBJECT_PATH + '3')

    def test_permissions(self):
        OBJECT_PATH = "something"
        OBJECT_PATH2 = OBJECT_PATH + "3"
        OBJECT_KEY = "test/" + OBJECT_PATH
        OBJECT_KEY2 = "test/" + OBJECT_PATH2
        remove_file(self.client, OBJECT_PATH2)
        self.client.put_object(
            Key=OBJECT_KEY, Body=b"this should *not* succeed", Bucket="wow")
        set_access(OBJECT_PATH, "null")
        self.assertRaises(Exception,
                          lambda: self.client.copy_object(CopySource={"Bucket": "wow", "Key": OBJECT_KEY}, Bucket="wow",
                                                          Key=OBJECT_KEY2))
        set_access("", "own", recursive=True)
        remove_file(self.client, OBJECT_PATH)
        remove_file(self.client, OBJECT_PATH + '3')
