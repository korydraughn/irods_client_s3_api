import unittest
import subprocess as sp
import botocore
import botocore.session

from tests.utility import *

# A lot of this will need to be reworked so that it can be bootstrapped effectively.
# Also the "test/hi" and "hi" disparity is already getting old, so I
# should do something about that.

class TestGetObject(unittest.TestCase):
    def setUp(self):
        session = botocore.session.get_session()

        self.client = session.create_client("s3",
                                            use_ssl=False,
                                            endpoint_url="http://127.0.0.1:8080",
                                            aws_access_key_id="no",
                                            aws_secret_access_key="heck")
    
    def tearDown(self):
        self.client.close()

    def test_permission(self):
        "Test permission support in getobject"
        # Create file that cannot be read by the current user
        touch_file("Hi")
        set_access("Hi", "read_metadata")
        # Read the object :)
        self.assertRaises(Exception,
                          lambda: print(read_file(self.client, "test/Hi")))

        # After claiming the object, it should be able to be read
        set_access("Hi", "own")
        read_file(self.client, "test/Hi")
        remove_file(self.client, 'Hi')
