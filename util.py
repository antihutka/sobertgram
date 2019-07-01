import logging
import random
import time
import asyncio

def retry(retry_count):
  def dec(f):
    def df(*args, **kwargs):
      fails = 0
      delay = 0
      while True:
        try:
          return f(*args, **kwargs)
        except:
          fails += 1
          if retry_count >= fails:
            logging.exception('Exception, retrying in %d seconds' % delay)
            time.sleep(delay)
            delay = delay * 2 + random.randint(1,2)
          else:
            logging.exception('Exception, giving up')
            raise
    return df
  return dec

def aretry(retry_count):
  def dec(f):
    async def df(*args, **kwargs):
      fails = 0
      delay = 0
      while True:
        try:
          return await f(*args, **kwargs)
        except:
          fails += 1
          if retry_count >= fails:
            logging.exception('Exception, retrying in %d seconds' % delay)
            await asyncio.sleep(delay)
            delay = delay * 2 + random.randint(1,2)
          else:
            logging.exception('Exception, giving up')
            raise
    return df
  return dec
