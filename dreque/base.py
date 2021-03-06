import logging
import time
from redis import Redis, ResponseError

from dreque import serializer
from dreque.stats import StatsCollector


class Dreque(object):

    def __init__(self, server="127.0.0.1", db=None, key_prefix="dreque:",
                 serializer=serializer):
        self.log = logging.getLogger("dreque")

        if isinstance(server, (tuple, list)):
            host, port = server
            self.redis = Redis(server[0], server[1], db=db)
        elif isinstance(server, str):
            host = server
            port = 6379
            if ':' in server:
                host, port = server.split(':')
            self.redis = Redis(host, port, db=db)
        else:
            self.redis = server

        self.key_prefix = key_prefix
        self.watched_queues = set()
        self.stats = StatsCollector(self.redis, self.key_prefix)
        self.serializer = serializer

    # Low level

    def push(self, queue, item, delay=None):
        self.watch_queue(queue)

        if delay:
            delay = time.time() + delay
            self.redis.zadd(self._delayed_key(queue), self.encode(item), delay)
            self.log.debug(f"Added {delay:.4f} {item}")
        else:
            self.redis.lpush(self._queue_key(queue), self.encode(item))
            self.log.debug(f"Added {item}")

    def check_delayed(self, queue, num=100):
        """
        Check for available jobs in the delayed queue and move them to the
        live queue.
        """
        delayed_key = self._delayed_key(queue)
        queue_key = self._queue_key(queue)
        now = time.time()
        jobs = self.redis.zrangebyscore(delayed_key, 0, now, start=0, num=num)
        self.log.debug(f"Popped {len(jobs)} delayed jobs")
        for j in jobs:
            self.redis.zrem(delayed_key, j)
            self.redis.lpush(queue_key, j)

    def pop(self, queue):
        self.check_delayed(queue)
        msg = self.redis.rpop(self._queue_key(queue))
        return self.decode(msg) if msg else None

    def poppush(self, source_queue, dest_queue):
        msg = self.redis.rpoplpush(self._queue_key(source_queue),
                                   self._queue_key(dest_queue))
        return self.decode(msg) if msg else None

    def size(self, queue):
        return self.redis.llen(self._queue_key(queue))

    def peek(self, queue, start=0, count=1):
        return self.list_range(self._queue_key(queue), start, count)

    def list_range(self, key, start=0, count=1):
        if count == 1:
            return self.decode(self.redis.lindex(key, start))
        return [self.decode(x)
                for x in self.redis.lrange(key, start, start + count - 1)]

    # High level

    def enqueue(self, queue, func, *args, **kwargs):
        delay = kwargs.pop('_delay', None)
        max_retries = kwargs.pop('_max_retries', 5)
        if not isinstance(func, str):
            func = "%s.%s" % (func.__module__, func.__name__)
        self.push(queue,
                  {'func': func,
                   'args': args,
                   'kwargs': kwargs,
                   'retries_left': max_retries},
                  delay=delay)

    def dequeue(self, queues, worker_queue=None):
        for queue in queues:
            if worker_queue:
                msg = self.redis.rpoplpush(self._queue_key(queue),
                                           self._redis_key(worker_queue))
                if msg:
                    msg = self.decode(msg)
            else:
                msg = self.pop(queue)
            if msg:
                msg['queue'] = queue
                return msg

    # Queue methods

    def queues(self):
        return self.redis.smembers(self._queue_set_key())

    def remove_queue(self, queue):
        self.watched_queues.discard(queue)
        self.redis.srem(self._queue_set_key(), queue)
        self.redis.delete(self._queue_key(queue))
        self.redis.delete(self._delayed_key(queue))

    def watch_queue(self, queue):
        if queue not in self.watched_queues:
            self.watched_queues.add(queue)
            self.redis.sadd(self._queue_set_key(), queue)

    #

    def encode(self, value):
        return self.serializer.dumps(value)

    def decode(self, value):
        return self.serializer.loads(value)

    def _queue_key(self, queue):
        return self._redis_key("queue:" + queue)

    def _queue_set_key(self):
        return self._redis_key("queues")

    def _delayed_key(self, queue):
        return self._redis_key("delayed:" + queue)

    def _redis_key(self, key):
        return self.key_prefix + key
