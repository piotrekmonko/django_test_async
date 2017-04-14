import os
from optparse import make_option
from Queue import Empty
import logging
from multiprocessing import Process, JoinableQueue, cpu_count
from clint.textui.progress import Bar
from django.core.management.base import BaseCommand
import sys


STOPBIT = 0xffff


class Consumer(Process):
    runner = None
    old_config = None
    settings = None

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

        settings.WORK_DIR = os.path.join(os.environ.get('XDG_RUNTIME_DIR'), 'work_%s' % self.pid)
        if not os.path.exists(settings.WORK_DIR):
            os.mkdir(settings.WORK_DIR)
        settings.NFS_SHARED_DIR = os.path.join(settings.WORK_DIR, 'shared')
        if not os.path.exists(settings.NFS_SHARED_DIR):
            os.mkdir(settings.NFS_SHARED_DIR)
        self.settings = settings

        # prepare environment and databases, abort tests on any errors
        from django_test_async.test_runner import AsyncRunner
        try:
            self.runner = AsyncRunner(**self._kwargs['opts'])
            self.runner.interactive = False
            self.runner.setup_test_environment()
            self.old_config = self.runner.setup_databases()
        except:
            print 'Error setting up databases'
            if 'south' in settings.INSTALLED_APPS:
                print 'Perhaps try without south?'
            self.cleanup()
            raise

        # consume suites queue, update results queue
        try:
            for suite in iter(q_suites.get, STOPBIT):
                result = self.runner.run_suite(suite)
                result.stream = result.stream.stream.getvalue()
                q_results.put((
                    self.pid,
                    result
                ))
                q_suites.task_done()
            self.cleanup()
        except:
            self.cleanup()
            raise

    def cleanup(self):
        # mark STOPBIT task as processed too
        self.runner.teardown_databases(self.old_config)
        self.runner.teardown_test_environment()
        os.rmdir(self.settings.NFS_SHARED_DIR)
        os.rmdir(self.settings.WORK_DIR)
        for db in self.settings.DATABASES:
            if os.path.exists(self.settings[db]['TEST_NAME']):
                os.remove(self.settings[db]['TEST_NAME'])
        self._kwargs['result'].put((self.pid, STOPBIT))


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
    workers = []

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

    def killall(self):
        # join() doesn't work at all
        # print 'Gracefully stopping workers:'
        # # kill all workers if any or main process failed
        # for i in self.workers:
        #     print i.name
        #     i.join(5)
        print 'Killing workers:'
        for i in self.workers:
            if i.is_alive():
                print i.name
                i.terminate()
        for i in self.workers:
            if i.is_alive():
                print 'Joining', i.name
                i.join(1)

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
        if procs <= 1:
            worker = Consumer(kwargs=pargs)
            worker.run()
            for p in iter(result_queue):
                print p
            return

        # start workers
        for i in range(procs):
            ww = Consumer(kwargs=pargs)
            self.workers.append(ww)
            ww.start()
            # null-terminate queue and wait for workers to join
            suites_queue.put(STOPBIT)
            tests_done[ww.pid] = dict((('run', 0), ('errs', list()), ('fails', list())))
        suites_queue.close()

        with Bar(label='      ', width=42, expected_size=total) as bar:
            finished = done = iterations = 0
            s = '|/-\\'
            pid = cmd = None
            try:
                while done < total:
                    iterations += 1
                    alive = sum(map(lambda x: x.is_alive(), self.workers))
                    if done == 0:
                        bar.label = '  %s  %s  ' % ('.' * procs, s[iterations % 4])
                    else:
                        bar.label = '  %s%s  %s  ' % ('R' * alive, 'S' * (procs - alive), s[iterations % 4])
                    bar.show(done)
                    if not alive:
                        print '>>> No live workers left!'
                        raise KeyboardInterrupt('No workers alive')
                    try:
                        pid, cmd = result_queue.get(timeout=0.1)
                    except Empty:
                        # print '>>> skipped a bit', pid, cmd
                        continue
                    if cmd == STOPBIT:
                        print 'Got STOPBIT'
                        for i in self.workers:
                            if i.pid == pid:
                                i.join(1)
                        finished += 1
                    else:
                        done += cmd.testsRun
                        tests_done[pid]['run'] += cmd.testsRun
                        tests_done[pid]['errs'].extend(cmd.errors)
                        tests_done[pid]['fails'].extend(cmd.failures)
                    if finished == procs:
                        print 'Got ALL STOPBITS'
                        break
                    bar.show(done)
            except KeyboardInterrupt:
                print 'Stopping'
            except:
                print 'An exception'
                self.killall()
                raise

        self.killall()

        total_tests = 0
        total_errs = []
        total_fails = []
        print '=' * 70
        for i, j in enumerate(self.workers):
            print 'Worker', i, 'pid', j.pid, 'finished with ', tests_done[j.pid]['run'], 'tests, ', \
                len(tests_done[j.pid]['errs']), 'errors and ', len(tests_done[j.pid]['fails']), 'failures.'
            total_tests += tests_done[j.pid]['run']
            total_errs.extend(tests_done[j.pid]['errs'])
            total_fails.extend(tests_done[j.pid]['fails'])

        for f in (total_fails, total_errs):
            if f:
                for testsuite, traceback in f:
                    print '-' * 70
                    print testsuite
                    print traceback

        print '-' * 70
        print 'Tests run:', total_tests
        print 'Errors:', len(total_errs)
        print 'Failures:', len(total_fails)
        print '=' * 70
        sys.exit(0)