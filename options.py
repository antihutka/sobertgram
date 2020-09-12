from cachetools import cached, TTLCache
from cachetools.keys import hashkey

from database import with_cursor

user_options = ['sticker_prob', 'reply_prob', 'admin_only', 'silent_commands', 'send_as_reply']

option_types = {
  'sticker_prob': float,
  'reply_prob': float,
  'admin_only': int,
  'silent_commands': int,
  'send_as_reply': int,
  'is_bad': int,
  'is_hidden': int,
  'blacklisted': int
}

default_user = {
  'sticker_prob': 0.9,
  'reply_prob': 1.0,
  'admin_only': 0,
  'silent_commands': 0,
  'send_as_reply': 1,
  'blacklisted': 0,
  'is_bad': 0,
  'is_hidden': 0
}

default_group = {
  'sticker_prob': 0.0,
  'reply_prob': 0.02,
  'admin_only': 0,
  'silent_commands': 0,
  'send_as_reply': 1,
  'blacklisted': 0,
  'is_bad': 0,
  'is_hidden': 0
}

user_options = ['sticker_prob', 'reply_prob', 'admin_only', 'silent_commands', 'send_as_reply']

optioncache = TTLCache(1024, 15*60)

@cached(optioncache)
@with_cursor
def get_all_options(cursor, convid):
  ret = {}
  cursor.execute('SELECT * FROM options2 WHERE convid=%s', (convid,))
  r = cursor.fetchone()
  if r:
    for (desc, val) in zip(cursor.description, r):
      if val is not None:
        ret[desc[0]] = val
  return ret

def get_option(convid, option_name):
  opts = get_all_options(convid)
  if option_name in opts:
    return opts[option_name]
  if convid < 0:
    return default_group[option_name]
  return default_user[option_name]

class OptionError(Exception):
  pass

@with_cursor
def set_option_db(cursor, convid, option_name, value_parsed):
  cursor.execute('INSERT INTO options2 (convid, ' + option_name + ') VALUES (%s,%s) ON DUPLICATE KEY UPDATE ' + option_name + ' = %s', (convid, value_parsed, value_parsed))

def set_option(convid, option_name, value, user_only = True):
  if ((option_name not in option_types) or
      (user_only and option_name not in user_options)
     ):
    raise OptionError("Unknown option: %s" % repr(option_name))
  try:
    value_parsed = option_types[option_name](value)
  except ValueError:
    raise OptionError("Can't parse value %s as %s" % (repr(option_name), option_types[option_name].__name__))
  current_value = get_option(convid, option_name)
  if current_value == value_parsed:
    print('value matches')
    return
  optioncache.pop(hashkey(convid), None)
  set_option_db(convid, option_name, value_parsed)
