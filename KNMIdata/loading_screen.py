import itertools
import threading
import time
import sys


class LoadingScreen():
    def __init__(self):
        self.done = False
        self.t = None

    def __animate(self, data_type):
        for c in itertools.cycle(['|', '/', '-', '\\']):
            if self.done:
                break

            sys.stdout.write(f'\rDownloading {data_type} data from KNMI' + c)
            sys.stdout.flush()
            time.sleep(0.2)

    def start(self, data_type):
        self.t = threading.Thread(target=self.__animate, args=[data_type])
        self.t.start()

    def stop(self):
        sys.stdout.flush()
        sys.stdout.write('\rSuccess\n')
        self.done = True

