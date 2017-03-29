#!/usr/bin/python
import unittest
import mtods

class MaTestCases(unittest.TestCase):
    def test_startup(self):
        self.assertEqual(True, True)

def run():
    unittest.main()

if __name__ == '__main__':
    run()
