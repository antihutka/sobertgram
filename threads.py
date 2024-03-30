import traceback
import sys
import logging
from threading import Thread

logger = logging.getLogger(__name__)

def wthread(q, n):
  while True:
    task = q.get()
    try:
      task()
    except Exception as e:
      logger.error('Exception in thread %s: %s' % (n, str(e)))
      traceback.print_exc(file=sys.stdout)
    q.task_done()

def start_thread(target=wthread, args=()):
  downloadworker = Thread(target=target, args=args)
  downloadworker.setDaemon(True)
  downloadworker.start()
