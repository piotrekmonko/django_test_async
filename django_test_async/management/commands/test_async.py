import logging
from billiard import Process, JoinableQueue, cpu_count
from clint.textui.progress import Bar
import os
from optparse import make_option

from django.core.management.base import BaseCommand


STOPBIT = 0xffff


class Consumer(Process):
    runner = None
    old_config = None

    def run(self):
        q_suites = self._kwargs['suites']
        q_results = self._kwargs['result']

        from django.conf import settings
        if 'south' in settings.INSTALLED_APPS:
            settings.INSTALLED_APPS.remove('south')

        dd = settings.DATABASES.dict
        for db in dd:
            settings.DATABASES[db]['ENGINE'] = 'django.db.backends.sqlite3'
            settings.DATABASES[db]['TEST_NAME'] = '{}/pid_{}_{}'.format(
                os.environ.get('XDG_RUNTIME_DIR'),
                self.pid,
                dd[db].get('TEST_NAME', 'no_testname'))

        for cc in settings.CACHES:
            settings.CACHES[cc]['KEY_PREFIX'] = 'pid_{}_{}'.format(
                self.pid,
                settings.CACHES[cc].get('KEY_PREFIX', 'no_cache_prefix'))

        # prepare environment and databases, abort tests on any errors
        from django_test_async.test_runner import AsyncRunner
        try:
            self.runner = AsyncRunner(**self._kwargs['opts'])
            self.runner.interactive = False
            self.runner.setup_test_environment()
            self.old_config = self.runner.setup_databases()
        except:
            if self._popen:
                self.terminate()
            raise

        # consume suites queue, update results queue
        try:
            for suite in iter(q_suites.get, STOPBIT):
                result = self.runner.run_suite(suite)
                q_results.put((
                    self.pid,
                    result
                    # self.runner.suite_result(suite, result)
                ))
                q_suites.task_done()
            # mark STOPBIT task as processed too
            q_suites.task_done()
            self.runner.teardown_databases(self.old_config)
            self.runner.teardown_test_environment()
            q_results.put((self.pid, STOPBIT))
        except:
            self.runner.teardown_databases(self.old_config)
            self.runner.teardown_test_environment()
            q_results.put((self.pid, STOPBIT))
            raise


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--failfast',
            action='store_true', dest='failfast', default=False,
            help='Tells Django to stop running the test suite after first '
                 'failed test.'),
        make_option('--liveserver',
            action='store', dest='liveserver', default=None,
            help='Overrides the default address where the live server (used '
                 'with LiveServerTestCase) is expected to run from. The '
                 'default value is localhost:8081.'),
        make_option('--processes', '-p', action='store', type='int', dest='processes', default=None,
            help='Use this many processes. Defaults to system cpu count.'),
    )
    help = ('Discover and run tests in the specified modules or the current directory.')
    args = '[path.to.modulename|path.to.modulename.TestCase|path.to.modulename.TestCase.test_method]...'

    requires_model_validation = False

    def __init__(self):
        self.test_runner = None
        super(Command, self).__init__()

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

    def handle(self, *test_labels, **options):
        from django.conf import settings
        if 'south' in settings.INSTALLED_APPS:
            settings.INSTALLED_APPS.remove('south')
        from django_test_async.test_runner import AsyncRunner

        options['verbosity'] = int(options.get('verbosity'))
        procs = int(options.pop('processes', None) or cpu_count())

        if options.get('liveserver') is not None:
            os.environ['DJANGO_LIVE_TEST_SERVER_ADDRESS'] = options['liveserver']
            del options['liveserver']

        suites_queue = JoinableQueue()
        result_queue = JoinableQueue()
        workers = []
        tests_done = {}

        # put suites in processing queue
        suites = AsyncRunner(**options).get_suite_list(test_labels, None)
        for s in suites:
            suites_queue.put(s)

        total = len(suites)
        if procs > total:
            procs = total
        print 'Starting %s tests in %s processes.' % (total, procs)

        pargs = {
            'opts': options,
            'length': len(suites),
            'suites': suites_queue,
            'result': result_queue,
        }
        # single-threaded run
        if procs == 0:
            worker = Consumer(kwargs=pargs)
            worker.run()
            for p in iter(result_queue):
                print p
            return

        # start workers
        for i in range(procs):
            ww = Consumer(kwargs=pargs)
            workers.append(ww)
            ww.start()
            # null-terminate queue and wait for workers to join
            suites_queue.put(STOPBIT)
            tests_done[ww.pid] = dict((('run', 0), ('errs', list()), ('fails', list())))

        with Bar(label='TESTING', width=42, expected_size=total) as bar:
            finished = 0
            done = 1
            try:
                while True:
                    bar.show(done)
                    pid, cmd = result_queue.get()
                    if cmd == STOPBIT:
                        print 'Got STOPBIT'
                        finished += 1
                    else:
                        done += 1
                        tests_done[pid]['run'] += cmd.testsRun
                        tests_done[pid]['errs'].extend(cmd.errors)
                        tests_done[pid]['fails'].extend(cmd.failures)
                    if finished == procs:
                        print 'Got ALL STOPBITS'
                        break
            except:
                print 'Killing workers'
                # kill all workers if any or main process failed
                for i in workers:
                    i.terminate()
                raise

        total_tests = 0
        total_errs = []
        total_fails = []
        print '=' * 70
        for i, j in enumerate(workers):
            print 'Worker', i, 'pid', j.pid, 'finished with ', tests_done[j.pid]['run'], 'tests, ', \
                len(tests_done[j.pid]['errs']), 'errors and ', len(tests_done[j.pid]['fails']), 'failures.'
            j.join(5)
            total_tests += tests_done[j.pid]['run']
            total_errs.extend(tests_done[j.pid]['errs'])
            total_fails.extend(tests_done[j.pid]['fails'])

        if total_fails:
            print '-' * 70
            for i in total_fails:
                print i

        if total_errs:
            print '-' * 70
            for i in total_errs:
                print i

        print '-' * 70
        print 'Tests run:', total_tests
        print 'Errors:', len(total_errs)
        print 'Failures:', len(total_fails)
        print '=' * 70
