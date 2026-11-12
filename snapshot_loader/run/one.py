"""Run one or more dotted test paths quickly, e.g.
python -I run/one.py tests.test_acceptance.LogStreamTests
"""
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

names = sys.argv[1:]
loader = unittest.TestLoader()
suite = unittest.TestSuite()
for name in names:
    suite.addTests(loader.loadTestsFromName(name))
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
