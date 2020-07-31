# Author: Swapnil Mahajan
import redis_lock
import logging


class RWLock:

    READ = 'R'
    WRITE = 'W'

    def __init__(self, redis_conn, name, mode, expire=30, auto_renew=False):
        assert mode in (self.READ, self.WRITE), 'Invalid mode.'
        assert name, 'Invalid name. Should not be empty'

        self.__mode = mode
        self.__read_switch = _LightSwitch(
            redis_conn, f'read_switch:{name}', expire=expire, auto_renew=auto_renew)
        self.__write_switch = _LightSwitch(
            redis_conn, f'write_switch:{name}', expire=expire, auto_renew=auto_renew)
        self.__no_readers = redis_lock.Lock(
            redis_conn, f'lock:no_readers:{name}', expire=expire, auto_renewal=auto_renew)
        self.__no_writers = redis_lock.Lock(
            redis_conn, f'lock:no_writers:{name}', expire=expire, auto_renewal=auto_renew)
        self.__readers_queue = redis_lock.Lock(
            redis_conn, f'lock:readers_queue:{name}', expire=expire, auto_renewal=auto_renew)

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

    def reset(self):
        self.__read_switch.reset()
        self.__write_switch.reset()
        self.__no_readers.reset()
        self.__no_readers.reset()
        self.__readers_queue.reset()


class _LightSwitch:
    """An auxiliary "light switch"-like object. The first thread turns on the
    "switch", the last one turns it off."""
    def __init__(self, redis_conn, name, expire=None, auto_renew=False):
        self.__counter_name = f'lock:switch:counter:{name}'
        self.__name = name
        self.__expire = expire
        self.__redis_conn = redis_conn
        self.__redis_conn.set(self.__counter_name, 0, nx=True, ex=self.__expire)
        counter_value = int(self.__redis_conn.get(self.__counter_name))
        logging.debug(f'Counter - Initial Value - {self.__counter_name}: {counter_value}')
        self.__mutex = redis_lock.Lock(
            redis_conn, f'lock:switch:{name}', expire=expire, auto_renewal=auto_renew)

    def acquire(self, lock):
        self.__mutex.acquire()
        self.__redis_conn.incr(self.__counter_name)
        if self.__expire:
            self.__redis_conn.expire(self.__counter_name, self.__expire)
        counter_value = int(self.__redis_conn.get(self.__counter_name))
        logging.debug(f'Counter {self.__counter_name}: {counter_value}')
        if counter_value == 1:
            lock.acquire()
        self.__mutex.release()

    def release(self, lock):
        self.__mutex.acquire()
        self.__redis_conn.decr(self.__counter_name)
        if self.__expire:
            self.__redis_conn.expire(self.__counter_name, self.__expire)
        counter_value = int(self.__redis_conn.get(self.__counter_name))
        logging.debug(f'Counter {self.__counter_name}: {counter_value}')
        if counter_value == 0:
            lock.reset()
        self.__mutex.release()

    def reset(self):
        self.__mutex.reset()
        self.__redis_conn.set(self.__counter_name, 0)
