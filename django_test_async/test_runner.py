from Queue import Empty
import traceback
from billiard import Process
from django.test.runner import DiscoverRunner
from django.utils.unittest.runner import TextTestRunner
from django.utils.unittest.suite import TestSuite
from cStringIO import StringIO
import sys
import shutil
import os
from django_test_async.const import COLOR_SHIFT, STOPBIT
from unittest import loader, TestCase


class _FailedTest(TestCase):
    _testMethodName = None

    def __init__(self, method_name, exception):
        self._exception = exception
        super(_FailedTest, self).__init__(method_name)

    def __getattr__(self, name):
        if name != self._testMethodName:
            return super(_FailedTest, self).__getattr__(name)

        def testFailure():
            raise self._exception
        return testFailure


def _make_failed_import_test(name, suiteClass):
    message = 'Failed to import test module: %s' % name
    test = _FailedTest(name, ImportError(message))
    return suiteClass((test,))


def _make_failed_load_tests(name, exception, suiteClass):
    test = _FailedTest(name, exception)
    return suiteClass((test,))


# patch testLoader to make it's exceptions pickable
def patch_loader():
    loader._make_failed_load_tests = _make_failed_load_tests
    loader._make_failed_import_test = _make_failed_import_test

patch_loader()


class AsyncRunner(DiscoverRunner):

    def create_suite(self, test):
        ts = TestSuite()
        ts.addTest(test)
        ts.sid = ts._tests[0].id()
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

    def ppp(self, s):
        try:
            from blessings import Terminal
            term = Terminal()
            print term.color(self.cons_id + COLOR_SHIFT)(u'{}\033[K'.format(s))
        except:
            print u'{} {}\033[K'.format(self.cons_id, s)

    def run_suite(self, suite, consumer_id, **kwargs):
        self.cons_id = consumer_id
        stream = sys.stderr if self.verbosity and self.verbosity > 1 else StringIO()
        self.ppp(u' > {}'.format(suite._tests[0].id()))
        result = TextTestRunner(
            stream=stream,
            verbosity=self.verbosity,
            failfast=self.failfast
        ).run(suite)
        state = 'OK'
        if result.skipped:
            state = 'SKIPPED'
        if result.failures:
            state = 'F' * len(result.failures)
        if result.errors:
            state = 'E' * len(result.errors)
        self.ppp(u'{} {}'.format(state, suite._tests[0].id()))
        return result


class Consumer(Process):
    runner = None
    old_config = None
    settings = None
    sysout = None
    ROOT = None
    BASEDIR = None
    interactive = False

    def consumer_id(self):
        return int(self.name.split('-')[1])

    def boolshitpid(self):
        return self.pid or self.consumer_id()

    def pre_run(self):
        self.sysout = sys.stdout
        self.interactive = not bool(self.pid)
        self.q_suites = self._kwargs['suites']
        self.q_results = self._kwargs['result']
        self.max_tasks = self._kwargs['max_tasks']
        self.ROOT = os.path.join(os.environ.get('XDG_RUNTIME_DIR'), 'test_async')
        if not os.path.exists(self.ROOT):
            os.mkdir(self.ROOT)
        self.BASEDIR = os.path.join(self.ROOT, 'pid_%s' % self.pid or 'mainprocess')
        if not os.path.exists(self.BASEDIR):
            os.mkdir(self.BASEDIR)

        self.apply_patches()

        # prepare environment and databases, abort tests on any errors
        from django_test_async.test_runner import AsyncRunner
        try:
            self.runner = AsyncRunner(**self._kwargs['opts'])
            self.runner.interactive = False
            self.runner.setup_test_environment()
            self.old_config = self.runner.setup_databases()
        except Exception as e:
            self.log(traceback.format_exc())
            a = ('error setting up databases', e.message)
            if self.interactive:
                import ipdb; ipdb.set_trace()
            if 'south' in self.settings.INSTALLED_APPS:
                a += (' - perhaps try without south?',)
            self.log(*a)
            self.cleanup()
            raise e

    def run_one(self):
        try:
            suite = self.q_suites.get(timeout=0.2)
        except Empty:
            sys.exit(0)
        # intercept and report testrunner-uncaught exceptions
        try:
            result = self.runner.run_suite(suite, self.consumer_id())
        except:
            result = {
                'testsRun': 1,
                'skipped': 0,
                'errors': unicode(traceback.format_exc()),
                'failures': '',
            }
        self.q_results.put((
            self.boolshitpid(),
            suite.sid,
            {
                'run': result.testsRun,
                'skipped': len(result.skipped),
                'errors': unicode(result.errors[0][1]) if result.errors and result.errors[0] else '',
                'failures': unicode(result.failures[0][1]) if result.failures and result.failures[0] else '',
            }
        ))

    def run(self):
        self.pre_run()
        # consume suites queue, update results queue
        tasks = 0
        try:
            while True:
                self.run_one()
                tasks += 1
                if self.max_tasks and tasks >= self.max_tasks:
                    break
            self.cleanup()
        except Exception as e:
            self.q_results.put((
                self.boolshitpid(),
                self.name,
                unicode(traceback.format_exc()),
            ))
            self.cleanup()
            raise
        sys.exit(0)

    def apply_patches(self):
        # patch testLoader to make it pickable
        patch_loader()

        # alter settings
        from django.conf import settings

        DD = settings.DATABASES.dict
        for db in DD:
            if 'transaction_hooks' in DD[db]['ENGINE']:
                settings.DATABASES[db]['ENGINE'] = 'transaction_hooks.backends.sqlite3'
            else:
                settings.DATABASES[db]['ENGINE'] = 'django.db.backends.sqlite3'
            settings.DATABASES[db]['TEST_NAME'] = os.path.join(
                self.BASEDIR,
                'db_{}_{}'.format(
                    self.boolshitpid(),
                    DD[db].get('TEST_NAME', 'notestname')
                )
            )

        for cc in settings.CACHES:
            settings.CACHES[cc]['KEY_PREFIX'] = 'pid_{}_{}'.format(
                self.boolshitpid(),
                settings.CACHES[cc].get('KEY_PREFIX', 'no_cache_prefix'))
            if 'FileBased' in settings.CACHES[cc]['BACKEND']:
                settings.CACHES[cc]['LOCATION'] = os.path.join(self.BASEDIR, 'cache', cc)

        settings.WORK_DIR = os.path.join(self.BASEDIR, 'work_%s' % self.boolshitpid())
        if not os.path.exists(settings.WORK_DIR):
            os.mkdir(settings.WORK_DIR)
        settings.NFS_SHARED_DIR = os.path.join(settings.WORK_DIR, 'shared')
        if not os.path.exists(settings.NFS_SHARED_DIR):
            os.mkdir(settings.NFS_SHARED_DIR)
        settings.TENANT_DIR = os.path.join(settings.WORK_DIR, 'tenant')
        if not os.path.exists(settings.TENANT_DIR):
            os.mkdir(settings.TENANT_DIR)
        self.settings = settings

    def cleanup(self):
        try:
            if self.old_config:
                self.runner.teardown_databases(self.old_config)
            self.runner.teardown_test_environment()
            shutil.rmtree(self.BASEDIR, ignore_errors=True)
        except Exception as e:
            self.log('cleanup error:', e)
        # mark STOPBIT task as processed too
        self._kwargs['result'].put((self.pid, STOPBIT))

    def log(self, *args):
        print('{}[{}] {}\033[K'.format(self.name, self.pid, ' '.join(map(unicode, args))))
        # self.sysout.write('{}[{}] {}\033[K\n'.format(self.name, self.pid, ' '.join(map(unicode, args))))
        # self.sysout.flush()