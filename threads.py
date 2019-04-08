import traceback
import sys
from threading import Thread

def wthread(q, n):
  while True:
    task = q.get()
    try:
      task()
    except Exception as e:
      print('Exception in thread %s: %s' % (n, str(e)))
      traceback.print_exc(file=sys.stdout)
    q.task_done()

def start_thread(target=wthread, args=()):
  downloadworker = Thread(target=target, args=args)
  downloadworker.setDaemon(True)
  downloadworker.start()
