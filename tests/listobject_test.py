from tests.utility import *
from unittest import *
import botocore
import botocore.session


class ListObject_Test(TestCase):
    def setUp(self):
        mkdir("")
        mkdir("a-first-test")
        mkdir("b-second-test")

        for i in range(10):
            touch_file(f"a-first-test/{i}")
            touch_file(f"b-second-test/{i}")

        session = botocore.session.get_session()
        self.client = session.create_client("s3",
                                            use_ssl=False,
                                            endpoint_url="http://127.0.0.1:8080",  # normal networking stuff :p
                                            aws_access_key_id="no",
                                            aws_secret_access_key="heck")
        set_access("", 'own',recursive=True)
        mkdir("", access_level="own")

    def tearDown(self):
        remove_file(self.client, "", recursive=True)

    def test_list(self):
        x = self.client.list_objects_v2(Bucket="wow", Prefix='test')
        print(x)
        self.assertEqual(len(x['Contents']), 20, "All items found")
