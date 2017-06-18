"""MulThreading components module"""
import time

class ThreadManager(object):
    """Manages a group of threads"""
    def __init__(self, *threads):
        self.threads = threads

    def _monitor_one(self, thread):
        thread.join()

    def _monitor(self):
        """Monitors a group of threads
        If one of the monitored threads goes down, all the threads are stopped.
        """
        if len(self.threads) == 1:
            self._monitor_one(self.threads[0])
            return

        all_alive = True
        while all_alive:
            if not all([t.isAlive() for t in self.threads]):
                for thread in self.threads:
                    thread.stop()
                all_alive = False
            else:
                time.sleep(5)

    def _start_all(self):
        """Starts all threads in the thread group"""
        for thread in self.threads:
            thread.start()

    def start(self):
        """Starts the group of threads and monitors them"""
        self._start_all()
        self._monitor()
