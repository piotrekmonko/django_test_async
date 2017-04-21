# coding: utf-8
import traceback
import os
from optparse import make_option
from Queue import Empty
import logging
from billiard import Process, JoinableQueue, cpu_count, log_to_stderr
from clint.textui.progress import Bar
from django.core.management.base import BaseCommand
import sys
from unittest import loader, TestCase
import time
import shutil


mpl = log_to_stderr()
mpl.setLevel(logging.INFO)

STOPBIT = 0xffff


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


# patch testLoader to make it pickable
loader._make_failed_load_tests = _make_failed_load_tests
loader._make_failed_import_test = _make_failed_import_test


class Consumer(Process):
    runner = None
    old_config = None
    settings = None
    sysout = None
    ROOT = None
    BASEDIR = None
    interactive = False

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
        suite = self.q_suites.get(False)
        # intercept and report testrunner-uncaught exceptions
        try:
            result = self.runner.run_suite(suite)
        except:
            result = unicode(traceback.format_exc())
        else:
            # unwrap StringIO if it's that
            if hasattr(result.stream.stream, 'getvalue'):
                result.stream = result.stream.stream.getvalue()
        self.q_results.put((
            self.pid,
            result
        ))
        self.q_suites.task_done()

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
                self.pid,
                e.message
            ))
            if not self.interactive:
                self.join(3)
            self.cleanup()
            raise

    def apply_patches(self):
        # patch testLoader to make it pickable
        loader._make_failed_load_tests = _make_failed_load_tests
        loader._make_failed_import_test = _make_failed_import_test

        # alter settings
        from django.conf import settings
        # if 'south' in settings.INSTALLED_APPS:
        #     settings.INSTALLED_APPS.remove('south')

        dd = settings.DATABASES.dict
        for db in dd:
            settings.DATABASES[db]['ENGINE'] = 'django.db.backends.sqlite3'
            settings.DATABASES[db]['TEST_NAME'] = os.path.join(
                self.BASEDIR,
                'db_{}_{}'.format(
                    self.pid or 'mainprocess',
                    dd[db].get('TEST_NAME', 'notestname')
                )
            )

        for cc in settings.CACHES:
            settings.CACHES[cc]['KEY_PREFIX'] = 'pid_{}_{}'.format(
                self.pid,
                settings.CACHES[cc].get('KEY_PREFIX', 'no_cache_prefix'))
            if 'FileBased' in settings.CACHES[cc]['BACKEND']:
                settings.CACHES[cc]['LOCATION'] = os.path.join(self.BASEDIR, 'cache', cc)

        settings.WORK_DIR = os.path.join(self.BASEDIR, 'work_%s' % self.pid)
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


class Command(BaseCommand):
    option_list = (
        make_option('-v', '--verbosity', action='store', dest='verbosity', default='1',
            type='choice', choices=map(str, range(6)),
            help='Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output'),
        make_option('--settings',
            help='The Python path to a settings module, e.g. "myproject.settings.main". If this isn\'t provided, the DJANGO_SETTINGS_MODULE environment variable will be used.'),
        make_option('--pythonpath',
            help='A directory to add to the Python path, e.g. "/home/djangoprojects/myproject".'),
        make_option('--traceback', action='store_true',
            help='Raise on exception'),
        make_option('--failfast',
            action='store_true', dest='failfast', default=False,
            help='Tells Django to stop running the test suite after first '
                 'failed test.'),
        # make_option('--liveserver',
        #     action='store', dest='liveserver', default=None,
        #     help='Overrides the default address where the live server (used '
        #          'with LiveServerTestCase) is expected to run from. The '
        #          'default value is localhost:8081.'),
        make_option('--processes', '-p', action='store', type='int', dest='processes', default=None,
            help='Use this many processes. Defaults to system cpu count.'),
        make_option('--tasks', '-t', action='store', type='int', dest='max_tasks', default=None,
            help='Kill worker after this many processed tasks.'),
    )
    help = ('Discover and run tests in the specified modules or the current directory.')
    args = '[path.to.modulename|path.to.modulename.TestCase|path.to.modulename.TestCase.test_method]...'

    requires_model_validation = False
    workers = []
    max_procs = 0
    max_tasks = 0
    tasks_revived = 0
    max_tasks_revived = 4

    def __init__(self):
        self.test_runner = None
        super(Command, self).__init__()

    def log(self, level, *args):
        if level <= self.verbosity:
            print('{}\033[K\r'.format(' '.join(map(unicode, args))))
            # sys.stdout.flush()

    def execute(self, *args, **options):
        if int(options['verbosity']) > 0:
            # ensure that deprecation warnings are displayed during testing
            # the following state is assumed:
            # logging.capturewarnings is true
            # a "default" level warnings filter has been added for
            # DeprecationWarning. See django.conf.LazySettings._configure_logging
            logger = logging.getLogger('py.warnings')
            handler = logging.StreamHandler()
            logger.addHandler(handler)
        super(Command, self).execute(*args, **options)
        if int(options['verbosity']) > 0:
            # remove the testing-specific handler
            logger.removeHandler(handler)

    def killall(self):
        self.log(2, 'Killing workers:')
        for i in self.workers:
            if i.is_alive():
                self.log(2, i.name)
                i.terminate()
        for i in self.workers:
            if i.is_alive():
                self.log(2, 'Joining', i.name)
                i.join(1)

    def launch_consumer(self):
        ww = Consumer(kwargs=self.pargs)
        self.workers.append(ww)
        if self.max_procs > 1:
            ww.start()
        self.tests_done[ww.pid] = dict((('run', 0), ('skipped', 0), ('errors', list()), ('failures', list())))

    def consumer(self, pid):
        return dict(((x.pid, x) for x in self.workers)).get(pid, None)

    def topup_consumers(self):
        if self.alive() < self.max_procs:
            # some worker died, check if there are many tasks left, revive if yes
            if self.suites_queue.qsize() > self.max_procs * (self.max_tasks or 40):
                if self.tasks_revived < self.max_tasks_revived:
                    self.tasks_revived += 1
                    self.launch_consumer()

    def retire_consumers(self):
        self.log(3, 'Max tasks', self.max_tasks)
        if not self.max_tasks:
            # if no need to retire then just check if all are running
            self.topup_consumers()
            return
        alive = self.alive()
        # dont launch new consumers if tasks left can be shared among running consumers
        self.log(3, 'Tasks left / queue:', alive * self.max_tasks, self.suites_queue.qsize())
        if alive * self.max_tasks > self.suites_queue.qsize():
            return
        # dont launch new consumers if that would exceed allowed processes
        self.log(3, 'Procs alive / max procs:', alive, self.max_procs)
        if alive >= self.max_procs:
            return
        self.log(3, 'Launching consumer')
        self.launch_consumer()

    def alive(self):
        return sum(map(lambda x: x.is_alive(), self.workers))

    def worker_status_display(self, (pid, v)):
        return '{}{}-{}E'.format(
            str(v['run']),
            ('A' if self.consumer(pid).is_alive() else 'D'),
            len(v['errors']) + len(v['failures'])
        )

    def handle(self, *test_labels, **options):
        # from django.conf import settings
        # if 'south' in settings.INSTALLED_APPS:
        #     settings.INSTALLED_APPS.remove('south')
        from django_test_async.test_runner import AsyncRunner

        self.verbosity = options['verbosity'] = int(options.get('verbosity'))
        self.max_procs = int(options.pop('processes', None) or cpu_count())
        self.max_tasks = options.pop('max_tasks')
        failfast = options.pop('failfast')

        if options.get('liveserver') is not None:
            os.environ['DJANGO_LIVE_TEST_SERVER_ADDRESS'] = options['liveserver']
            del options['liveserver']

        self.suites_queue = JoinableQueue()
        result_queue = JoinableQueue()
        self.tests_done = {}

        # put suites in processing queue
        suites = AsyncRunner(**options).get_suite_list(test_labels, None)
        self.suites = dict(((x.sid, (x, None)) for x in suites))
        for k, (v, x) in self.suites:
            self.suites_queue.put(v)

        if not len(suites):
            print 'No tests discovered. Are your tests on PYTHONPATH?'
            sys.exit(1)

        total = len(suites)
        if self.max_procs > total:
            self.max_procs = total
        print 'Starting %s tests in %s processes%s' % \
              (total, self.max_procs, ' restarted every %s tasks.' % self.max_tasks if self.max_tasks else '.')

        self.pargs = {
            'opts': options,
            'length': len(suites),
            'suites': self.suites_queue,
            'result': result_queue,
            'max_tasks': self.max_tasks,
        }

        # single-threaded run
        for i in range(self.max_procs):
            self.launch_consumer()

        if self.max_procs <= 1:
            self.workers[0].pre_run()

        with Bar(label='      ', width=32, expected_size=total) as bar:
            finished = done = skipped = errors = iterations = 0
            pid = cmd = None
            processing = []
            alive = 1
            try:
                # while done == 0 or alive:
                while alive:
                    iterations += 1
                    alive = self.alive()
                    qsize = self.suites_queue.qsize()
                    if self.max_procs <= 1:
                        self.workers[0].run_one()
                        alive = qsize
                    if done == 0:
                        s = ',|\'|'
                        bar.label = ' {}  {} tests queued, {} workers starting up, {} are up'.format(
                            s[iterations % 4],
                            qsize,
                            self.max_procs,
                            alive)
                        sys.stderr.write('{}\r'.format(bar.label))
                        sys.stderr.flush()
                    else:
                        s = '\X/X'
                        bar.label = '  {} {} left, {} running {} [{}]  '.format(
                            s[iterations % 4],
                            qsize,
                            total - (done + qsize),  # unaccounted for/tests in progress
                            '({} skipped)'.format(skipped) if skipped else '',
                            '|'.join(map(self.worker_status_display, self.tests_done.items())))
                        bar.show(done + skipped)
                    # quit if no workers are alive but there were some tests processed
                    # if (not alive and total and done) or ((done or skipped) and done + skipped >= total):
                    #     self.log(2, '>>> No live workers left!')
                    #     raise KeyboardInterrupt('No workers alive')

                    try:
                        pid, cmd = result_queue.get(timeout=0.2)
                        if pid and pid not in processing:
                            processing.append(pid)
                    except Empty:
                        self.log(5, '>>> skipped a bit', pid, cmd, alive, done, total)
                        continue

                    if cmd == STOPBIT:
                        self.log(4, 'Got STOPBIT')
                        for i in self.workers:
                            if i.pid == pid:
                                i.join(10)
                        finished += 1
                    elif cmd:
                        if not hasattr(cmd, 'failures'):
                            import ipdb; ipdb.set_trace()
                        done += cmd.testsRun
                        skipped += len(cmd.skipped)
                        errors += len(cmd.errors) + len(cmd.failures)
                        self.tests_done[pid]['run'] += cmd.testsRun
                        self.tests_done[pid]['skipped'] += len(cmd.skipped)
                        self.tests_done[pid]['errors'].extend(cmd.errors)
                        self.tests_done[pid]['failures'].extend(cmd.failures)
                    if failfast and errors:
                        self.killall()
                    self.retire_consumers()
            except KeyboardInterrupt:
                print 'Stopping'
            except:
                self.log(5, 'An exception')
                self.killall()
                raise
            finally:
                bar.show(done + skipped)

        self.killall()

        total_tests = 0
        total_skipped = 0
        total_errs = []
        total_fails = []
        self.log(1, '=' * 70)
        for i, j in enumerate(self.workers):
            self.log(1, 'Worker', i, 'pid', j.pid, 'finished with ', \
                self.tests_done[j.pid]['run'], 'tests, ', \
                len(self.tests_done[j.pid]['errors']), 'errors and ', \
                len(self.tests_done[j.pid]['failures']), 'failures.')
            total_tests += self.tests_done[j.pid]['run']
            total_skipped += self.tests_done[j.pid]['skipped']
            total_errs.extend(self.tests_done[j.pid]['errors'])
            total_fails.extend(self.tests_done[j.pid]['failures'])

        for k, f in zip(('FAILURE:', 'ERROR:'), (total_fails, total_errs)):
            if f:
                for testsuite, traceback in f:
                    self.log(1, '-' * 70)
                    self.log(1, k)
                    self.log(1, testsuite)
                    self.log(1, traceback)

        self.log(1, '-' * 70)
        self.log(1, 'Tests found:', total)
        self.log(1, 'Tests processed:', total_tests + total_skipped)
        self.log(1, 'Tests run:', total_tests)
        self.log(1, 'Tests skipped:', total_skipped)
        self.log(1, 'Errors:', len(total_errs))
        self.log(1, 'Failures:', len(total_fails))
        self.log(1, '=' * 70)
        sys.exit(0)