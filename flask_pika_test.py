# import unittest2
import unittest2 as unittest
from MockLogger import MockLogger
from flask_pika import Fpika
from flask_pika import unix_time_millis_now
# import pika


class TestFlaskPika(unittest.TestCase):

    def failover_params(self):
        return {'host': 'testing.hostfail.com',
                'username': 'ianthomasmccarthy',
                'password': 'ianthomasmccarthy',
                'port': 5672,
                'virtual_host': '/'}

    def params(self):
        return {'host': 'testing.host.com',
                'username': 'ianthomasmccarthy',
                'password': 'ianthomasmccarthy',
                'port': 5672,
                'virtual_host': '/'}

    def setUp(self):
        self.fpika = Fpika(Logger=MockLogger())

    def test_sanity(self):
        self.assertEqual(4 + 4, 8)

    def test_object(self):
        self.assertTrue(self.fpika)

    def test_create_creds(self):
        params = self.params()
        new = self.fpika.create_creds(params)
        self.assertTrue(new['credentials'])

    def test_fail_over(self):
        self.fpika._pika_connection_params = self.params()
        self.fpika._pika_failover_connection_params = self.failover_params()
        result = self.fpika._fail_over("testing")
        test = "Successfully Failed Over"
        self.assertEqual(test, result)

    def test_check_tolerance(self):
        self.fpika.channel_broken_times = []
        for i in xrange(10):
            self.fpika.channel_broken_times.append(unix_time_millis_now() - (i*10))
        self.fpika.tolerance = 5
        self.fpika.tolerance_interval = 60
        self.assertTrue(self.fpika.check_tolerance())
        self.assertEqual(len(self.fpika.channel_broken_times), self.fpika.tolerance)
