import sys

from configuration import Config
from database import with_cursor
import options

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])

@with_cursor
def get_old_options(cursor):
  cursor.execute("SELECT convid, option, value FROM options")
  r = cursor.fetchall()
  return r

@with_cursor
def get_bad(cursor):
  cursor.execute("SELECT convid FROM bad_chats")
  return [x[0] for x in cursor.fetchall()]

@with_cursor
def get_hidden(cursor):
  cursor.execute("SELECT id FROM _hidden_chats")
  return [x[0] for x in cursor.fetchall()]

for (convid, optname, optvalue) in get_old_options():
  print("%d %s %s" % (convid, optname, repr(optvalue)))
  if optname in ['potato', 'group', 'talk_prob', 'yiff']:
    print('Skipping!')
    continue
  options.set_option(convid, optname, optvalue, user_only=False)

for convid in get_bad():
  print("bad: %d" % convid)
  options.set_option(convid, 'is_bad', '1', user_only=False)

for convid in get_hidden():
  print("hidden: %d" % convid)
  options.set_option(convid, 'is_hidden', '1', user_only=False)
