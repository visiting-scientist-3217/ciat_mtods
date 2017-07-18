#!/work/opt/python-2.7.9/bin/python
'''Script to reset the chado schema; asuming the existence of "chado.dump".'''
import unittest_helper
try:
    p = unittest_helper.PostgreRestorer()
except Exception as e:
    raw_input('{}, try again <Return>'.format(e))
    p = unittest_helper.PostgreRestorer()
p.dumpfile = 'chado.dump'
p.restore()
