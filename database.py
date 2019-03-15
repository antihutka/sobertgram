import MySQLdb

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
