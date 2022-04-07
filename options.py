from cachetools import cached, TTLCache
from cachetools.keys import hashkey
from collections import namedtuple

from database import with_cursor

ChatOption = namedtuple('ChatOption', 'name type settable default_user default_group visible description')
options_list = [
  ChatOption(name='sticker_prob',     type=float, settable=True,  default_user=0.9, default_group=0.0,  visible=True,  description='Probability of converting a single emoji message to a random corresponding sticker. (possibly NSFW)'),
  ChatOption(name='reply_prob',       type=float, settable=True,  default_user=1.0, default_group=0.02, visible=True,  description='Probability of replying to any text message.'),
  ChatOption(name='admin_only',       type=int,   settable=True,  default_user=0,   default_group=0,    visible=True,  description='Limit setting options and badwords to admins when set to 1, owners when set to 2.'),
  ChatOption(name='admin_only_pq',    type=int,   settable=True,  default_user=0,   default_group=0,    visible=True,  description='Only allow admins to use /pq.'),
  ChatOption(name='silent_commands',  type=int,   settable=True,  default_user=0,   default_group=0,    visible=True,  description='Suppress replies to all commands.'),
  ChatOption(name='send_as_reply',    type=int,   settable=True,  default_user=1,   default_group=1,    visible=True,  description='Send messages as replies. 0 - never, 1 - when there are multiple messages, 2 - always.'),
  ChatOption(name='filter_username',  type=int,   settable=True,  default_user=0,   default_group=0,    visible=True,  description="Add the user's/chat's username to every chat's badword list."),
  ChatOption(name='default_badwords', type=int,   settable=True,  default_user=10,  default_group=10,   visible=True,  description="Add words present in at least N other chats' badword lists to this chat's badword list. Disabled when set to less than 3."),
  ChatOption(name='ignore_files',     type=int,   settable=True,  default_user=0,   default_group=0,    visible=True,  description="Ignore all files (except stickers) sent in the chat."),
  ChatOption(name='backend',          type=int,   settable=True,  default_user=0,   default_group=0,    visible=False, description=''),
  ChatOption(name='is_bad',           type=int,   settable=False, default_user=0,   default_group=0,    visible=False, description=''),
  ChatOption(name='is_hidden',        type=int,   settable=False, default_user=0,   default_group=0,    visible=False, description=''),
  ChatOption(name='blacklisted',      type=int,   settable=False, default_user=0,   default_group=0,    visible=False, description=''),
  ChatOption(name='user_blacklisted', type=int,   settable=False, default_user=0,   default_group=0,    visible=False, description='')
]

options = { o.name : o for o in options_list }

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
    return options[option_name].default_group
  return options[option_name].default_user

class OptionError(Exception):
  pass

@with_cursor
def set_option_db(cursor, convid, option_name, value_parsed):
  cursor.execute('INSERT INTO options2 (convid, ' + option_name + ') VALUES (%s,%s) ON DUPLICATE KEY UPDATE ' + option_name + ' = %s', (convid, value_parsed, value_parsed))

def set_option(convid, option_name, value, user_only = True):
  if ((option_name not in options) or
      (user_only and (not options[option_name].settable))
     ):
    raise OptionError("Unknown option: %s" % repr(option_name))
  try:
    value_parsed = options[option_name].type(value)
  except ValueError:
    raise OptionError("Can't parse value %s as %s" % (repr(option_name), options[option_name].type.__name__))
  current_value = get_option(convid, option_name)
  if current_value == value_parsed:
    print('value matches')
    return
  optioncache.pop(hashkey(convid), None)
  set_option_db(convid, option_name, value_parsed)
