import queue
import requests
import threading


class LockedQueue:
    # A queue and its guard lock.

    def __init__(self):
        # Initialize a queue and a lock.
        self.q = queue.Queue()
        self.lock = threading.Lock()

    def get(self):
        # Lock the queue and retrieve an item.  Returns None
        # if the queue is empty.
        self.lock.acquire()
        p = None
        if not self.q.empty():
            p = self.q.get()
            self.lock.release()
        else:
            self.lock.release()
        return p

    def put(self, p):
        # Lock the queue and put an item on it.
        self.lock.acquire()
        self.q.put(p)
        self.lock.release()
