"""Unit tests for schedule.py"""
import datetime
import functools
import mock
import unittest

from context import interface
import luigi
from luigi.contrib.simulate import RunAnywayTarget
import time

class BaseTest(luigi.ExternalTask):
    param= luigi.Parameter()

    def run(self):
        print('param: {0}'.format(self.param))
        print('sleeping for 10 seconds')
        time.sleep(10)
        print('done')
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


class BadTest(BaseTest):

    def run(self):
        print('param: {0}'.format(self.param))
        print('sleeping for 10 seconds')
        time.sleep(10)
        print('done')
        if 1 / 0:
          self.output().done()


class BadTestWithDependency(BadTest):

    def requires(self):
        return BaseTest(param=self.param)


class TestWithBadDependency(BaseTest):

    def requires(self):
        return BadTest(param=self.param)


class InterfaceTests(unittest.TestCase):

    def test_success(self):
       result = interface.build([BaseTest(param='Testing...123...')],local_scheduler=True)
       assert result['success'] == True

    def test_failure(self):
       result = interface.build([BadTest(param='Testing...123...')],local_scheduler=True)
       assert result['success'] == False

    def test_failed_dependency(self):
       result = interface.build([BadTestWithDependency(param='Testing...123...')],local_scheduler=True)
       assert result['success'] == False
       result = interface.build([TestWithBadDependency(param='Testing...123...')],local_scheduler=True)
       assert result['success'] == False




if __name__ == "__main__":
    unittest.main()