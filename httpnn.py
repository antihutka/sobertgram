import asyncio
import aiohttp
import logging
from util import aretry
from threading import Thread, Event

import random

class HTTPNN:
  def __init__(self, url, keyprefix):
    self.url = url
    self.keyprefix = keyprefix
    self.locks = {}

  def get_lock(self, key):
    if key not in self.locks:
      self.locks[key] = asyncio.Lock()
    return self.locks[key]

  async def queued_for_key(self, key):
    if self.get_lock(key)._waiters is None:
      return 0
    return len(self.get_lock(key)._waiters)

  @aretry(5)
  async def put_(self, key, message):
    async with self.client.post(self.url + "put", json={'key': self.keyprefix + ':' + key, 'text': message}) as response:
      assert response.status == 200
      rj = await response.json()

  async def put(self, key, message):
    async with self.get_lock(key):
      return await self.put_(key, message)

  @aretry(5)
  async def get_(self, key, bad_words):
    async with self.client.post(self.url + 'get', json={'key': self.keyprefix + ':' + key, 'bad_words': bad_words}) as response:
      assert response.status == 200
      rj = await response.json()
    return rj['text']

  async def get(self, key, bad_words = []):
    async with self.get_lock(key):
      return await self.get_(key, bad_words)

  def initialize2(self):
    self.client = aiohttp.ClientSession(timeout = aiohttp.ClientTimeout(1800))

  async def initialize(self):
    self.client = aiohttp.ClientSession(loop = self.loop, timeout = aiohttp.ClientTimeout(1800))

  async def consume_queue(self):
    while True:
      try:
        coro = await self.queue.get()
        await coro
      except Exception as e:
        logging.getLogger(__file__).exception('Exception in coro')
      self.queue.task_done()

  def run_from_thread(self, func, *args):
    assert self.queue
    def f():
      self.queue.put_nowait(asyncio.ensure_future(func(*args)))
      #print(self.queue.__dict__)
    self.loop.call_soon_threadsafe(f)

  def run_thread(self):
    evt = Event()
    def tgt():
      self.loop = asyncio.new_event_loop()
      asyncio.set_event_loop(self.loop)
      self.loop.run_until_complete(self.initialize())
      self.queue = asyncio.Queue()
      evt.set()
      self.loop.run_until_complete(self.consume_queue())
      logging.getLogger(__file__).critical('Returned from consume_queue()?')
    thread = Thread(target=tgt, args=())
    thread.daemon = True
    thread.start()
    evt.wait()
