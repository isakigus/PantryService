import os
import unittest

exe = os.system

class TestPantry(unittest.TestCase):
    def test_set_one_node(self):
        exe('python ../counter_services.py')
        exe('../toopython shopper.py --port 8888 --host 127.0.0.1 --verb PUT --uri /store/doc1/1 --data 'jejejejlou'')

