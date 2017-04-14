from django.test.runner import DiscoverRunner
from django.utils.unittest.runner import TextTestRunner
from django.utils.unittest.suite import TestSuite
from cStringIO import StringIO


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
        for s in suite._tests:
            print s.id(), '\033[K'
        return TextTestRunner(
            stream=StringIO(),
            verbosity=self.verbosity,
            failfast=self.failfast
        ).run(suite)