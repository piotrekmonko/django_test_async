# coding: utf-8
import os
from optparse import make_option
from Queue import Empty
import logging
from billiard import cpu_count, log_to_stderr, Queue
from clint.textui.progress import Bar
from django.core.management.base import BaseCommand
import sys
import time
from django_test_async.const import STOPBIT, colorized
from django_test_async.test_runner import Consumer


mpl = log_to_stderr()
mpl.setLevel(logging.INFO)


class Command(BaseCommand):
    option_list = (
        make_option('-v', '--verbosity', action='store', dest='verbosity', default='1',
            type='choice', choices=map(str, range(6)),
            help='Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output'),
        make_option('--settings',
            help='The Python path to a settings module, e.g. "myproject.settings.main". If this isn\'t provided, '
                 'the DJANGO_SETTINGS_MODULE environment variable will be used.'),
        make_option('--pythonpath',
            help='A directory to add to the Python path, e.g. "/home/djangoprojects/myproject".'),
        make_option('--traceback', action='store_true',
            help='Raise on exception'),
        make_option('--failfast',
            action='store_true', dest='failfast', default=False,
            help='Tells Django to stop running the test suite after first '
                 'failed test.'),
        make_option('--debug',
            action='store_true', dest='debug', default=False,
            help='Run verbose and single-threaded. Drop to shell on exception.'),
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
    help = 'Discover and run tests asynchronously in the specified modules or the current directory.'
    args = '[path.to.modulename|path.to.modulename.TestCase|path.to.modulename.TestCase.test_method]...'

    requires_model_validation = False
    can_import_settings = False
    leave_locale_alone = True

    workers = []
    max_procs = 0
    max_tasks = 0
    verbosity = 0
    tasks_revived = 0
    max_tasks_revived = 4
    term = None
    pargs = None
    suites = None
    tests_done = None
    suites_queue = None
    global_errors = None

    def __init__(self):
        self.test_runner = None
        super(Command, self).__init__()

    def log(self, level, *args):
        if level <= self.verbosity:
            print('{}\033[K\r'.format(' '.join(map(unicode, args))))
            # sys.stdout.flush()

    def execute(self, *args, **options):
        try:
            from blessings import Terminal
            self.term = Terminal()
            self.worker_status_display = self._worker_status_colorized
        except:
            pass
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
        self.tests_done[ww.boolshitpid()] = dict((('run', 0), ('skipped', 0), ('errors', 0), ('failures', 0)))

    def consumer(self, pid):
        dic_workers = dict(((x.boolshitpid(), x) for x in self.workers))
        return dic_workers.get(pid, None)

    def topup_consumers(self):
        if self.alive() < self.max_procs:
            # some worker died, check if there are many tasks left, revive if yes
            if self.suites_queue.qsize() > self.max_procs * (self.max_tasks or 40):
                if self.tasks_revived < self.max_tasks_revived:
                    self.tasks_revived += 1
                    self.launch_consumer()

    def retire_consumers(self):
        self.log(3, 'Max tasks', self.max_tasks)
        if not self.max_tasks and self.max_procs > 1:
            # if no need to retire then just check if all are running
            self.topup_consumers()
            return
        if not self.max_tasks:
            return
        alive = self.alive()
        # dont launch new consumers if tasks left can be shared among running consumers
        self.log(3, 'Tasks left / queue:', alive * self.max_tasks or 0, self.suites_queue.qsize())
        if alive * self.max_tasks > self.suites_queue.qsize():
            return
        # dont launch new consumers if that would exceed allowed processes
        self.log(3, 'Procs alive / max procs:', alive, self.max_procs)
        if alive >= self.max_procs:
            return
        self.log(3, 'Launching consumer')
        self.launch_consumer()

    def suites_waiting(self):
        return filter(lambda x: x[1] is None, self.suites.values())

    def suites_processed(self):
        return filter(lambda x: x[1] is not None, self.suites.values())

    def suites_errors(self):
        return filter(lambda (x, y): y['errors'] or y['failures'], self.suites.values())

    def alive(self):
        return sum(map(lambda x: x.is_alive(), self.workers))

    def _worker_status_colorized(self, (pid, v)):
        cons = self.consumer(pid)
        alive = cons.is_alive()
        ss = '{}{}-{}S-{}E'.format(
            'A' if alive else 'D',
            v['run'],
            v['skipped'],
            v['errors'] + v['failures']
        )
        return colorized(cons.consumer_id(), ss)

    def worker_status_display(self, (pid, v)):
        return '{}{}s{}-{}E'.format(
            ('A' if self.consumer(pid).is_alive() else 'D'),
            v['run'],
            v['skipped'],
            v['errors'] + v['failures']
        )

    def push_waiting_suites(self):
        for (v, x) in self.suites_waiting()[:10]:
            self.log(1, 'Repushing a test:', unicode(v))
            self.suites_queue.put(v)

    def handle(self, *test_labels, **options):
        # from django.conf import settings
        # if 'south' in settings.INSTALLED_APPS:
        #     settings.INSTALLED_APPS.remove('south')
        from django_test_async.test_runner import AsyncRunner

        self.verbosity = options['verbosity'] = int(options.get('verbosity'))
        self.max_procs = int(options.pop('processes', None) or cpu_count())
        self.max_tasks = options.pop('max_tasks')
        self.debug = options['debug']
        if self.debug:
            self.max_procs = 1
            failfast = options['failfast'] = True
            if self.verbosity == 1:
                self.verbosity = options['verbosity'] = 6
        else:
            failfast = options.pop('failfast')

        if options.get('liveserver') is not None:
            os.environ['DJANGO_LIVE_TEST_SERVER_ADDRESS'] = options['liveserver']
            del options['liveserver']

        self.suites_queue = Queue()
        result_queue = Queue()
        self.tests_done = {}
        self.global_errors = {}

        # put suites in processing queue
        suites = AsyncRunner(**options).get_suite_list(test_labels, None)
        self.suites = dict(((x.sid, list((x, None))) for x in suites))
        for k, (v, x) in self.suites.items():
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
            p_done = skipped = errors = iterations = 0
            pid = None
            alive = 1
            try:
                # while done == 0 or alive:
                # while alive or (self.max_procs == 1 and self.suites_queue.qsize() or result_queue.qsize()):
                while alive or (self.suites_queue.qsize() or result_queue.qsize()):
                    iterations += 1
                    alive = self.alive()
                    qsize = self.suites_queue.qsize()
                    rsize = result_queue.qsize()
                    p_waiting = len(self.suites_waiting())
                    p_done = len(self.suites_processed())
                    for w in self.workers:
                        if not w.is_alive() and w.pid:
                            self.log(2, 'joining worker', w.boolshitpid())
                            w.join(0.1)
                    if not p_waiting and not rsize:
                        self.log(1, 'quiting: no waiting and no rsize')
                        break
                    # if not rsize and not qsize and p_waiting:
                    #     self.push_waiting_suites()
                    if self.max_procs <= 1:
                        self.workers[0].run_one()
                        alive = qsize
                    if p_done == 0:
                        s = ',|\'|'
                        bar.label = ' {} {} queued, {} workers starting up, {} are up, result_queue is {}.'.format(
                            s[iterations % 4],
                            qsize,
                            self.max_procs,
                            alive,
                            result_queue.qsize())
                        sys.stderr.write('{}\r'.format(bar.label))
                        sys.stderr.flush()
                    else:
                        s = '\X/X'
                        bar.label = '  {MARK} {AA}/{BB} T{TOTL} W{WAIT} D{DONE} S{SKIP} P{PRCS} [{FLGS}] '.format(
                            MARK=s[iterations % 4],
                            AA=self.suites_queue.qsize(),
                            BB=result_queue.qsize(),
                            TOTL=len(self.suites.keys()),  # unaccounted for/tests in progress
                            WAIT=p_waiting,
                            DONE=p_done,
                            SKIP=skipped,
                            PRCS=p_waiting - qsize,
                            FLGS='|'.join(map(self.worker_status_display, self.tests_done.items())))
                        bar.show(p_done)
                    # quit if no workers are alive but there were some tests processed
                    # if (not alive and total and done) or ((done or skipped) and done + skipped >= total):
                    #     self.log(2, '>>> No live workers left!')
                    #     raise KeyboardInterrupt('No workers alive')

                    try:
                        pid = test_id = test_result = response_error = None
                        response_body = result_queue.get(timeout=0.2)
                        if type(response_body) is not tuple:
                            print response_body
                            raise Exception('Uncomprehensible response from consumer')
                        if len(response_body) == 2:
                            pid, response_error = response_body
                        elif len(response_body) == 3:
                            pid, test_id, test_result = response_body
                    except Empty:
                        self.log(5, '>>> skipped a bit', pid, alive, p_done, total)
                        time.sleep(0.01)
                        continue

                    if test_id == STOPBIT:
                        self.log(4, 'Got STOPBIT')
                        for i in self.workers:
                            if i.boolshitpid() == pid:
                                i.join(10)
                    elif test_id and test_result:
                        if test_id in self.suites:
                            self.suites[test_id][1] = test_result
                        else:
                            if pid not in self.global_errors:
                                self.global_errors[pid] = []
                            self.global_errors[pid].append(test_result)
                        try:
                            skipped += test_result['skipped']
                        except TypeError:
                            import ipdb; ipdb.set_trace()

                        self.tests_done[pid]['run'] += 1
                        self.tests_done[pid]['skipped'] += test_result['skipped']
                        if test_result['errors']:
                            self.tests_done[pid]['errors'] += 1
                        if test_result['failures']:
                            self.tests_done[pid]['failures'] += 1
                    elif response_error:
                        self.tests_done[pid]['run'] += 1
                        self.tests_done[pid]['errors'] += 1
                        if pid not in self.global_errors:
                            self.global_errors[pid] = []
                        self.global_errors[pid].append(response_error)
                    if failfast and (test_result['errors'] or test_result['failures']):
                        self.log(1, 'quiting: failfast on errors')
                        self.killall()
                    self.retire_consumers()
            except KeyboardInterrupt:
                print 'Stopping'
            except:
                self.log(1, 'quitiing: an exception')
                self.killall()
                raise
            finally:
                bar.show(p_done)

        self.killall()

        total_tests = 0
        total_skipped = 0
        total_errs = 0
        total_fails = 0
        self.log(1, '=' * 70)
        for i, j in enumerate(self.workers):
            bid = j.boolshitpid()
            self.log(1,
                'Worker', i, 'pid', j.pid, 'finished with ',
                self.tests_done[bid]['run'], 'tests, ',
                self.tests_done[bid]['errors'], 'errors and ',
                self.tests_done[bid]['failures'], 'failures.')
            total_tests += self.tests_done[bid]['run']
            total_skipped += self.tests_done[bid]['skipped']
            total_errs += self.tests_done[bid]['errors']
            total_fails += self.tests_done[bid]['failures']

        if not self.debug:
            for tst, statedict in self.suites_errors():
                if statedict['errors']:
                    self.log(1, '=' * 70)
                    self.log(1, 'ERROR:', tst.sid)
                    self.log(1, tst._tests[0].shortDescription())
                    self.log(1, '-' * 70)
                    self.log(1, statedict['errors'])
                    self.log(1, 'ERROR', tst.sid)
                if statedict['failures']:
                    self.log(1, '=' * 70)
                    self.log(1, 'FAILURE:', tst.sid)
                    self.log(1, tst._tests[0].shortDescription())
                    self.log(1, '-' * 70)
                    self.log(1, statedict['failures'])
                    self.log(1, 'FAILURE:', tst.sid)

        still_waiting = self.suites_waiting()
        if len(still_waiting):
            self.log(1, 'Still waiting for', len(still_waiting), 'tests')

        for name, errlist in self.global_errors.items():
            self.log(1, 'GLOBALERROR:', name)
            for ee in errlist:
                self.log(1, ee)
                self.log(1, '-' * 70)

        self.log(1, '=' * 70)
        self.log(1, 'Tests found:', total)
        self.log(1, 'Tests run:', total_tests)
        self.log(1, 'Tests skipped:', total_skipped)
        self.log(1, 'Errors:', total_errs)
        self.log(1, 'Failures:', total_fails)
        self.log(1, '=' * 70)
        sys.exit(0)