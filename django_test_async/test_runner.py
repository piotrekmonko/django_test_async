from django.test.runner import DiscoverRunner
from django.utils.unittest.runner import TextTestRunner
from django.utils.unittest.suite import TestSuite
from cStringIO import StringIO
import sys


class AsyncRunner(DiscoverRunner):

    def create_suite(self, test):
        ts = TestSuite()
        ts.addTest(test)
        return ts

    def get_suite_list(self, test_labels, extra_tests, **kwargs):
        one_big_suite = self.build_suite(test_labels, extra_tests)
        # collect a suite of tests into an iterable of single test suites
        return [self.create_suite(x) for x in one_big_suite]

    def run_async_test(self, suite):
        self.setup_test_environment()
        old_config = self.setup_databases()
        result = self.run_suite(suite)
        self.teardown_databases(old_config)
        self.teardown_test_environment()
        return self.suite_result(suite, result)

    def run_suite(self, suite, **kwargs):
        stream = sys.stderr if self.verbosity and self.verbosity > 1 else StringIO()
        result = TextTestRunner(
            stream=stream,
            verbosity=self.verbosity,
            failfast=self.failfast
        ).run(suite)
        state = 'OK ' * result.testsRun
        if result.skipped:
            state = 'SKIPPED ' * len(result.skipped)
        if result.failures:
            state = 'F' * len(result.failures)
        if result.errors:
            state = 'E' * len(result.errors)
        print '{} {}\033[K'.format(state, suite._tests[0].id())
        return result