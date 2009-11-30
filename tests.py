
import os
import tempfile
import time
import unittest
from dreque import Dreque, DrequeWorker

def test_func(tempfile, text, delay=0):
    if delay:
        time.sleep(delay)
    with open(tempfile, "wb") as fp:
        fp.write(text)

class TestDreque(unittest.TestCase):
    def setUp(self):
        import logging
        logging.basicConfig(level=logging.DEBUG)
        self.tempfile = tempfile.mkstemp()[1]
        self.dreque = Dreque("127.0.0.1")
        self.queue = "test"
        self.dreque.remove_queue(self.queue)

    def tearDown(self):
        os.unlink(self.tempfile)

    def _get_output(self):
        with open(self.tempfile, "rb") as fp:
            return fp.read()

    def testSimple(self):
        self.dreque.push("test", "foo")
        self.failUnlessEqual(self.dreque.pop("test"), "foo")
        self.failUnlessEqual(self.dreque.pop("test"), None)

    def testFunction(self):
        import tests
        self.dreque.enqueue("test", tests.test_func, "positional")
        self.failUnlessEqual(self.dreque.dequeue(["test"]), dict(queue="test", func="tests.test_func", args=["positional"], kwargs={}))
        self.dreque.enqueue("test", tests.test_func, keyword="argument")
        self.failUnlessEqual(self.dreque.dequeue(["test"]), dict(queue="test", func="tests.test_func", args=[], kwargs={'keyword':"argument"}))

    def testPositionalWorker(self):
        import tests
        self.dreque.enqueue("test", tests.test_func, self.tempfile, "worker_test")
        worker = DrequeWorker(["test"], "127.0.0.1", nofork=True)
        worker.work(0)
        self.failUnlessEqual(self._get_output(), "worker_test")

    def testKeywordWorker(self):
        import tests
        self.dreque.enqueue("test", tests.test_func, tempfile=self.tempfile, text="worker_test")
        worker = DrequeWorker(["test"], "127.0.0.1", nofork=True)
        worker.work(0)
        self.failUnlessEqual(self._get_output(), "worker_test")

    def testDelayedJob(self):
        import tests
        self.dreque.enqueue("test", tests.test_func, val="worker_test", _delay=1)
        self.failUnlessEqual(self.dreque.dequeue("test"), None)
        time.sleep(1.5)
        self.failUnlessEqual(self.dreque.dequeue(["test"]), dict(queue="test", func="tests.test_func", args=[], kwargs={'val':"worker_test"}))

    def testGracefulShutdown(self):
        import tests
        import signal
        from multiprocessing import Process
    
        def worker():
            w = DrequeWorker(["test"], "127.0.0.1")
            w.work(0)
    
        self.dreque.enqueue("test", tests.test_func, tempfile=self.tempfile, text="graceful", delay=2)
        worker_child = Process(target=worker, args=())
        worker_child.start()
        time.sleep(0.5) # Make sure the worker has spawned a child already
        # worker_child.terminate()
        os.kill(worker_child.pid, signal.SIGQUIT)
        worker_child.join()
        self.failUnlessEqual(worker_child.exitcode, 0)
        self.failUnlessEqual(self._get_output(), "graceful")

    def testForcedShutdown(self):
        import tests
        import signal
        from multiprocessing import Process
    
        def worker():
            w = DrequeWorker(["test"], "127.0.0.1")
            w.work(0)
    
        self.dreque.enqueue("test", tests.test_func, tempfile=self.tempfile, text="graceful", delay=2)
        worker_child = Process(target=worker, args=())
        worker_child.start()
        time.sleep(0.5) # Make sure the worker has spawned a child already
        worker_child.terminate()
        worker_child.join()
        self.failUnlessEqual(worker_child.exitcode, 0)
        self.failUnlessEqual(self._get_output(), "")

if __name__ == '__main__':
    unittest.main()
