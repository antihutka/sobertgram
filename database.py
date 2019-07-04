import MySQLdb
from weakref import WeakKeyDictionary

from configuration import Config

def get_dbcon():
  db = MySQLdb.connect(host=Config.get('Database', 'Host'), user=Config.get('Database', 'User'), passwd=Config.get('Database', 'Password'), db=Config.get('Database', 'Database'), charset='utf8')
  cur = db.cursor()
  cur.execute('SET NAMES utf8mb4')
  return db, cur

def dbcur_queryone(cur, query, args = (), default = None):
  cur.execute(query, args)
  row = cur.fetchone()
  if row and (row[0] is not None):
    return row[0]
  return default

cursor_oncommit = WeakKeyDictionary()

def with_cursor(infun):
  def outfun(*args, **kwargs):
    db = MySQLdb.connect(
      host=Config.get('Database', 'Host'),
      user=Config.get('Database', 'User'),
      passwd=Config.get('Database', 'Password'),
      db=Config.get('Database', 'Database'),
      charset='utf8')
    try:
      with db as cur:
        cur = db.cursor()
        cur.execute('SET NAMES utf8mb4')
        ret = infun(cur, *args, **kwargs)
        db.commit()
        if cur in cursor_oncommit:
          for act in cursor_oncommit[cur]:
            act[0][act[1]] = act[2]
        return ret
    finally:
      db.close()
  return outfun

def cache_on_commit(cursor, cachedict, cachekey, cacheval):
  if cursor not in cursor_oncommit:
    cursor_oncommit[cursor] = []
  cursor_oncommit[cursor].append((cachedict, cachekey, cacheval))
