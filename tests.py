import logging
from multiprocessing import Process
import os
import signal
import tempfile
import time
import unittest

from dreque import Dreque, DrequeWorker
import tests


def test_func(tempfile, text, delay=0, fail=False):
    if delay:
        time.sleep(delay)
    if fail:
        with open(tempfile, "a") as fp:
            fp.write(text)
        raise Exception("")
    with open(tempfile, "wb") as fp:
        fp.write(text.encode())


class TestDreque(unittest.TestCase):

    db = None

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.tempfile = tempfile.mkstemp()[1]
        self.dreque = Dreque(db=self.db)
        self.dreque.redis.flushdb()
        self.queue = "test"

    def tearDown(self):
        os.unlink(self.tempfile)

    def _get_output(self):
        with open(self.tempfile, "rb") as fp:
            return fp.read()

    def testSimple(self):
        self.dreque.push("test", "foo")
        self.assertEqual(self.dreque.size("test"), 1)
        self.assertEqual(self.dreque.pop("test"), "foo")
        self.assertEqual(self.dreque.pop("test"), None)
        self.assertEqual(self.dreque.size("test"), 0)

    def testFunction(self):
        self.dreque.enqueue("test", tests.test_func, "positional")
        self.assertEqual(self.dreque.dequeue(["test"]),
                             dict(queue="test", func="tests.test_func",
                                  args=["positional"], kwargs={}, retries_left=5))
        self.dreque.enqueue("test", tests.test_func, keyword="argument")
        self.assertEqual(self.dreque.dequeue(["test"]),
                             dict(queue="test", func="tests.test_func",
                                  args=[], kwargs={'keyword':"argument"},
                                  retries_left=5))

    def testPositionalWorker(self):
        self.dreque.enqueue("test", tests.test_func, self.tempfile, "worker_test")
        worker = DrequeWorker(["test"], db=self.db)
        worker.work(0)
        self.assertEqual(self._get_output(), b"worker_test")
        self.assertEqual(self.dreque.stats.get("processed"), 1)

    def testKeywordWorker(self):
        import tests
        self.dreque.enqueue("test", tests.test_func, tempfile=self.tempfile,
                            text="worker_test")
        worker = DrequeWorker(["test"], db=self.db)
        worker.work(0)
        self.assertEqual(self._get_output(), b"worker_test")

    def testDelayedJob(self):
        self.dreque.enqueue("test", tests.test_func, val="worker_test", _delay=1)
        self.assertEqual(self.dreque.dequeue("test"), None)
        time.sleep(1.5)
        self.assertEqual(self.dreque.dequeue(["test"]),
                             dict(queue="test", func="tests.test_func",
                                  args=[], kwargs={'val':"worker_test"},
                                  retries_left=5))

    def testGracefulShutdown(self):
        def worker():
            w = DrequeWorker(["test"], db=self.db)
            w.work(0)

        self.dreque.enqueue("test", tests.test_func,
                            tempfile=self.tempfile, text="graceful", delay=2)
        worker_child = Process(target=worker, args=())
        worker_child.start()
        time.sleep(0.5)  # Make sure the worker has spawned a child already
        # worker_child.terminate()
        os.kill(worker_child.pid, signal.SIGQUIT)
        worker_child.join()
        self.assertEqual(worker_child.exitcode, 0)
        self.assertEqual(self._get_output(), b"graceful")

    def testForcedShutdown(self):
        def worker():
            logging.getLogger("dreque.worker").setLevel(logging.CRITICAL)
            w = DrequeWorker(["test"], db=self.db)
            w.work(0)

        self.dreque.enqueue("test", tests.test_func,
                            tempfile=self.tempfile, text="graceful", delay=2)
        worker_child = Process(target=worker, args=())
        worker_child.start()
        time.sleep(0.5)  # Make sure the worker has spawned a child already
        worker_child.terminate()
        worker_child.join()
        self.assertEqual(worker_child.exitcode, 0)
        self.assertEqual(self._get_output(), b"")

    def testRetries(self):
        def worker():
            logging.getLogger("dreque.worker").setLevel(logging.CRITICAL)
            w = DrequeWorker(["test"], db=self.db)
            w.work(0.1)

        self.dreque.enqueue("test", tests.test_func,
                            tempfile=self.tempfile, text=".", fail=True)
        worker_child = Process(target=worker, args=())
        worker_child.start()
        time.sleep(3)  # Give enough time for worker to grab and execute the job
        os.kill(worker_child.pid, signal.SIGQUIT)
        worker_child.join()
        self.assertEqual(worker_child.exitcode, 0)
        self.assertEqual(self._get_output(), b"..")


if __name__ == '__main__':
    unittest.main()
