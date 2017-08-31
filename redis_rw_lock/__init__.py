# Author: Swapnil Mahajan
import redis_lock


class RWLock:

    READ = 'R'
    WRITE = 'W'

    def __init__(self, redis_conn, name, mode, expire=None, ):
        assert mode in (self.READ, self.WRITE), 'Invalid mode.'
        assert name, 'Invalid name. Should not be empty'

        self.__mode = mode
        self.__read_switch = _LightSwitch(redis_conn, 'read_switch:{}'.format(name), expire=expire)
        self.__write_switch = _LightSwitch(redis_conn, 'write_switch:{}'.format(name), expire=expire)
        self.__no_readers = redis_lock.Lock(
            redis_conn, 'lock:no_readers:{}'.format(name), expire=expire)
        self.__no_writers = redis_lock.Lock(
            redis_conn, 'lock:no_writers:{}'.format(name), expire=expire)
        self.__readers_queue = redis_lock.Lock(
            redis_conn, 'lock:readers_queue:{}'.format(name), expire=expire)

    def __reader_acquire(self):
        self.__readers_queue.acquire()
        self.__no_readers.acquire()
        self.__read_switch.acquire(self.__no_writers)
        self.__no_readers.release()
        self.__readers_queue.release()

    def __reader_release(self):
        self.__read_switch.release(self.__no_writers)

    def __writer_acquire(self):
        self.__write_switch.acquire(self.__no_readers)
        self.__no_writers.acquire()

    def __writer_release(self):
        self.__no_writers.release()
        self.__write_switch.release(self.__no_readers)

    def acquire(self):
        if self.__mode == self.READ:
            return self.__reader_acquire()
        return self.__writer_acquire()

    def release(self):
        if self.__mode == self.READ:
            return self.__reader_release()
        return self.__writer_release()


class _LightSwitch:
    """An auxiliary "light switch"-like object. The first thread turns on the
    "switch", the last one turns it off."""
    def __init__(self, redis_conn, name, expire=None):
        self.__counter_name = 'lock:switch:counter:{}'.format(name)
        self.__name = name
        self.__redis_conn = redis_conn
        self.__redis_conn.set(self.__counter_name, 0)
        self.__mutex = redis_lock.Lock(
            redis_conn, 'lock:switch:{}'.format(name), expire=expire)

    def acquire(self, lock):
        self.__mutex.acquire()
        self.__redis_conn.incr(self.__counter_name)
        if int(self.__redis_conn.get(self.__counter_name)) == 1:
            lock.acquire()
        self.__mutex.release()

    def release(self, lock):
        self.__mutex.acquire()
        self.__redis_conn.decr(self.__counter_name)
        if int(self.__redis_conn.get(self.__counter_name)) == 0:
            lock.reset()
        self.__mutex.release()
