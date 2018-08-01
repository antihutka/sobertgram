from telegram.ext import Updater, MessageHandler, Filters, CommandHandler
from telegram import ChatAction
import logging
import socket
import MySQLdb
import re
import sys
from time import time, sleep
from random import uniform
import ConfigParser
from Queue import Queue
from threading import Thread
import traceback
import os.path
import unicodedata

Config = ConfigParser.ConfigParser()

convos = {}
times = {}
known_stickers = set()
downloaded_files = set()
replyqueues = {}
downloadqueue = Queue(maxsize=256)
options = {}
sticker_emojis = None
pqed_messages = set()
command_replies = set()

def getreplyqueue(convid):
  if convid not in replyqueues:
    replyqueues[convid] = Queue(maxsize=16)
    replyworker = Thread(target=wthread, args=(replyqueues[convid], 'reply_' + str(convid)))
    replyworker.setDaemon(True)
    replyworker.start()
  return replyqueues[convid]

def getconv(convid):
  if convid not in convos:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((Config.get('Backend', 'Host'), Config.getint('Backend', 'Port')))
    f = s.makefile()
    convos[convid] = (s,f)
  times[convid] = time()
  return convos[convid]

def convclean():
  now = time()
  for convid in times:
    if (convid in convos) and (times[convid] + Config.getfloat('Chat', 'Timeout') * 60 * 60 < now):
      print('Deleting conversation %d' % (convid,))
      getreplyqueue(convid).join()
      # TODO remove the queue
      s = convos[convid][0]
      s.shutdown(socket.SHUT_RDWR)
      s.close()
      convos[convid][1].close()
      del convos[convid]

def put(convid, text):
  if text == '':
    return
  text = re.sub('[\r\n]+', '\n',text).strip("\r\n")
  try:
    (s, f) = getconv(convid)
    s.send((text + '\n').encode('utf-8', 'ignore'))
  except Exception as e:
    print str(e)
    del convos[convid]

def get(convid):
  try:
    (s, f) = getconv(convid)
    s.send('\n')
    return lambda: f.readline().rstrip()
  except Exception as e:
    print str(e)
    del convos[convid]
    return ''

def user_name(user):
  if user.username:
    return user.username
  return '(' + user.first_name + ')'

def chatname(chat):
  try:
    if chat.title:
      return chat.title
    else:
      n = chat.first_name
      if chat.last_name:
        n = n + ' ' + chat.last_name
      return n
  except Exception as e:
    print "can't get name: ", str(e)
    return '<err>'

def lookup_sticker_emoji(emoji):
  if emoji in sticker_emojis:
    return emoji
  emoji = emoji.strip(u'\ufe00\ufe01\ufe02\ufe03\ufe04\ufe05\ufe06\ufe07\ufe09\ufe0a\ufe0b\ufe0c\ufe0d\ufe0e\ufe0f')
  if emoji in sticker_emojis:
    return emoji
  return None

def get_dbcon():
  db = MySQLdb.connect(host=Config.get('Database', 'Host'), user=Config.get('Database', 'User'), passwd=Config.get('Database', 'Password'), db=Config.get('Database', 'Database'), charset='utf8')
  cur = db.cursor()
  cur.execute('SET NAMES utf8mb4')
  return db, cur

def log(conv, username, fromid, fromname, sent, text):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `chat` (`convid`, `from`, `fromid`, `chatname`, `sent`, `text`) VALUES (%s, %s, %s, %s, %s, %s)", (conv, username, fromid, fromname, sent, text))
  db.commit()
  db.close()

def log_cmd(conv, username, fromname, cmd):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `commands` (`convid`, `from`, `chatname`, `command`) VALUES (%s, %s, %s, %s)", (conv, username, fromname, cmd))
  db.commit()
  db.close()

def log_sticker(conv, username, fromid, fromname, sent, text, file_id, set_name):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `chat` (`convid`, `from`, `fromid`, `chatname`, `sent`, `text`) VALUES (%s, %s, %s, %s, %s, %s)", (conv, username, fromid, fromname, sent, text))
  cur.execute("INSERT INTO `chat_sticker` (`id`, `file_id`, `set_name`) VALUES (LAST_INSERT_ID(), %s, %s)", (file_id, set_name))
  if file_id not in known_stickers:
    cur.execute("SELECT COUNT(*) FROM `stickers` WHERE `file_id` = %s", (file_id,))
    (exists,) = cur.fetchone()
    if exists == 0:
      print "Adding sticker <%s> <%s> < %s >" %  (file_id, set_name, text)
      cur.execute("REPLACE INTO `stickers` (`file_id`, `emoji`, `set_name`) VALUES (%s, %s, %s)", (file_id, text, set_name))
  db.commit()
  db.close()
  if file_id not in known_stickers:
    known_stickers.add(file_id)
    sticker_emojis.add(text)

def log_file(conv, username, chatname, ftype, fsize, attr, file_id):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `chat_files` (`convid`, `from`, `chatname`, `type`, `file_size`, `attr`, `file_id`) VALUES (%s, %s, %s, %s, %s, %s, %s)", (conv, username, chatname, ftype, fsize, attr, file_id))
  db.commit()
  db.close()

def log_status(conv, username, chatname, updates):
  if not updates:
    return
  db, cur = get_dbcon()
  for u in updates:
    cur.execute("INSERT INTO `status_updates` (`convid`, `from`, `chatname`, `type`, `value`) VALUES (%s, %s, %s, %s, %s)", (conv, username, chatname, u[0], u[1]))
  db.commit()
  db.close()

def rand_sticker(emoji = None):
  db, cur = get_dbcon()
  if emoji:
    emoji = lookup_sticker_emoji(emoji)
    if not emoji:
      return None
    cur.execute("SELECT `file_id`, `emoji`, `set_name` FROM `stickers` WHERE `freqmod` > 0 AND `emoji` = %s ORDER BY -LOG(1.0 - RAND()) / `freqmod` LIMIT 1", (emoji,))
  else:
    cur.execute("SELECT `file_id`, `emoji`, `set_name` FROM `stickers` WHERE `freqmod` > 0 ORDER BY -LOG(1.0 - RAND()) / `freqmod` LIMIT 1")
  row = cur.fetchone()
  db.close()
  return row

def get_sticker_emojis():
  db, cur = get_dbcon()
  cur.execute("SELECT DISTINCT `emoji` from `stickers` WHERE `freqmod` > 0")
  rows = cur.fetchall()
  db.close()
  return [unicode(x[0], 'utf8') for x in rows]

def already_pqd(txt):
  db, cur = get_dbcon()
  cur.execute("SELECT COUNT(*) FROM `pq` WHERE `message` = %s", (txt,));
  (exists,) = cur.fetchone()
  db.close()
  if exists > 0:
    return True
  return False

def log_pq(convid, userid, txt):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `pq` (`convid`, `userid`, `message`) VALUES (%s, %s, %s)", (convid, userid, txt))
  db.commit()
  db.close()

def option_set(convid, option, value):
  db, cur = get_dbcon()
  cur.execute("REPLACE INTO `options` (`convid`, `option`, `value`) VALUES (%s,%s, %s)", (convid, option, str(value)))
  db.commit()
  db.close()
  options[(convid, option)] = value

def option_get_raw(convid, option):
  if (convid, option) in options:
    return options[(convid, option)]
  db, cur = get_dbcon()
  cur.execute("SELECT `value` FROM `options` WHERE `convid` = %s AND `option` = %s", (convid, option))
  row = cur.fetchone()
  if row != None:
    options[(convid, option)] = row[0]
    return row[0]
  else:
    return None

def option_get_float(convid, option, def_u, def_g):
  try:
    oraw = option_get_raw(convid, option)
    if oraw != None:
      return float(oraw)
  except Exception as e:
    print("Error getting option %s for conv %d: %s" % (option, convid, str(e)))
  if convid > 0:
    return def_u
  else:
    return def_g


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def sendreply(bot, ci, fro, froi, fron):
  if getreplyqueue(ci).full():
    print('Warning: reply queue full, dropping reply')
    return
  bot.sendChatAction(chat_id=ci, action=ChatAction.TYPING)
  getmsg = get(ci)
  def rf():
    msg = getmsg()
    print(' => %s/%s/%d: %s' % (fron, fro, ci, unicode(msg, "utf8")))
    log(ci, fro, froi, fron, 1, msg)
    sp = option_get_float(ci, 'sticker_prob', 0.9, 0)
    if uniform(0, 1) < sp:
      rs = rand_sticker(unicode(msg, 'utf-8'))
      if rs:
        print('sending as sticker %s/%s' % (rs[2], rs[0]))
        bot.sendSticker(chat_id=ci, sticker=rs[0])
        return
    bot.sendMessage(chat_id=ci, text=msg)
  getreplyqueue(ci).put(rf)

def fix_name(value):
  value = unicode(re.sub('[/<>:"\\\\|?*]', '_', value))
  return value

def download_file(bot, ftype, fid, fname):
  fname = fix_name(fname)

  if fid in downloaded_files:
    return
  def df():
    filename = ftype + '/' + fname
    if os.path.isfile(filename):
      print('file ' + filename + ' already exists')
      return
    f = bot.getFile(file_id=fid)
    print 'downloading file ' + filename + ' from ' + f.file_path
    f.download(custom_path=filename, timeout=120)
    sleep(15)
  if downloadqueue.full():
    print('Warning: download queue full')
  downloadqueue.put(df, True, 30)
  downloaded_files.add(fid)

def getmessage(bot, ci, fro, froi, fron, txt):
  print('%s/%s/%d: %s' % (fron, fro, ci, txt))
  put(ci, txt)
  log(ci, fro, froi, fron, 0, txt)

def cifrofron(update):
  ci = update.message.chat_id
  fro = user_name(update.message.from_user)
  fron = chatname(update.message.chat)
  froi = update.message.from_user.id
  return ci, fro, fron, froi

def should_reply(bot, msg, ci, txt = None):
  if msg and msg.reply_to_message and msg.reply_to_message.from_user.id == bot.id:
    return True
  if not txt:
    txt = msg.text
  if txt and (Config.get('Chat', 'Keyword') in txt.lower()):
    return True
  rp = option_get_float(ci, 'reply_prob', 1, 0.02)
  return (uniform(0, 1) < rp)

def msg(bot, update):
  if not update.message:
    print('No message, channel?')
    return
  ci, fro, fron, froi = cifrofron(update)
  txt = update.message.text
  getmessage(bot, ci, fro, froi, fron, txt)
  if should_reply(bot, update.message, ci):
    sendreply(bot, ci, fro, froi, fron)
  convclean()

def start(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  print('%s/%d /start' % (fro, ci))
  sendreply(bot, ci, fro, froi, fron)

def me(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  txt = update.message.text
  getmessage(bot, ci, fro, froi, fron, txt)
  sendreply(bot, ci, fro, froi, fron)

def sticker(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  st = update.message.sticker
  set = '(unnamed)' if st.set_name is None else st.set_name
  emo = st.emoji or ''
  print('%s/%s/%d: [sticker <%s> <%s> < %s >]' % (fron, fro, ci, st.file_id, set, emo))
  put(ci, emo)
  log_sticker(ci, fro, froi, fron, 0, emo, st.file_id, set)
  if should_reply(bot, update.message, ci):
    sendreply(bot, ci, fro, froi, fron)
  download_file(bot, 'stickers', st.file_id, st.file_id + ' ' + set + '.webp');

def video(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  vid = update.message.video
  fid = vid.file_id
  attr = '%dx%d; length=%d; type=%s' % (vid.width, vid.height, vid.duration, vid.mime_type)
  size = vid.file_size
  print('%s/%s: video, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'video', fid, fid + '.mp4')
  log_file(ci, fro, fron, 'video', size, attr, fid)

def document(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  doc = update.message.document
  fid = doc.file_id
  size = doc.file_size
  name = doc.file_name
  if not name:
    name = '_unnamed_.mp4'
  attr = 'type=%s; name=%s' % (doc.mime_type, name)
  print('%s/%s: document, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'document', fid, fid + ' ' + name)
  log_file(ci, fro, fron, 'document', size, attr, fid)

def audio(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  aud = update.message.audio
  fid = aud.file_id
  size = aud.file_size
  ext = '.ogg'
  if aud.mime_type == 'audio/mp3':
    ext = '.mp3'
  attr = 'type=%s; duration=%d; performer=%s; title=%s' % (aud.mime_type, aud.duration, aud.performer, aud.title)
  print('%s/%s: audio, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'audio', fid, '%s %s - %s%s' % (fid, aud.performer, aud.title, ext))
  log_file(ci, fro, fron, 'audio', size, attr, fid)

def photo(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  txt = update.message.caption
  photos = update.message.photo
  maxsize = 0
  pho = None
  for photo in photos:
    if photo.file_size > maxsize and photo.file_size < 20 * 1024 * 1024:
      maxsize = photo.file_size
      pho = photo
  fid = pho.file_id
  attr = 'dim=%dx%d' % (pho.width, pho.height)
  if txt:
    attr += '; caption=' + txt
    getmessage(bot, ci, fro, froi, fron, txt)
    if should_reply(bot, update.message, ci, txt):
      sendreply(bot, ci, fro, froi, fron)
  print('%s/%s: photo, %d, %s, %s' % (fron, fro, maxsize, fid, attr))
  download_file(bot, 'photo', fid, fid + '.jpg')
  log_file(ci, fro, fron, 'photo', maxsize, attr, fid)

def voice(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  voi = update.message.voice
  fid = voi.file_id
  size = voi.file_size
  attr = 'type=%s; duration=%d' % (voi.mime_type, voi.duration)
  print('%s/%s: voice, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'voice', fid, fid + '.opus')
  log_file(ci, fro, fron, 'voice', size, attr, fid)

def status(bot, update):
  msg = update.message
  ci, fro, fron, froi = cifrofron(update)
  upd = []
  if msg.new_chat_members:
    for mmb in msg.new_chat_members:
      upd.append(('new_member', str(mmb.id) + ' ' + user_name(mmb)))
  if msg.left_chat_member:
    mmb = msg.left_chat_member
    upd.append(('left_member', str(mmb.id) + ' ' + user_name(mmb)))
  if msg.new_chat_title:
    upd.append(('new_title', msg.new_chat_title))
  if msg.group_chat_created:
    upd.append(('group_created', ''))
  if msg.supergroup_chat_created:
    upd.append(('supergroup_created', ''))
  if msg.migrate_from_chat_id:
    upd.append(('migrate_from_chat_id', str(msg.migrate_from_chat_id)))
  for u in upd:
    print('[UPDATE] %s / %s: %s  %s' % (fron, fro, u[0], u[1]))
  log_status(ci, fro, fron, upd)

def cmdreply(bot, ci, text):
  msg = bot.sendMessage(chat_id=ci, text=text)
  command_replies.add(msg.message_id)

def givesticker(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  foremo = None
  cmd = update.message.text
  m = re.match('^/[^ ]+ (.+)', cmd)
  if m:
    foremo = unicode(m.group(1)).strip()
  rs = rand_sticker(foremo)
  if not rs:
    cmdreply(bot, ci, '<no sticker for %s>\n%s' % (foremo, ''.join(list(sticker_emojis))))
  else:
    fid, emo, set = rs
    print('%s/%s/%d: [giving random sticker: <%s> <%s>]' % (fron, fro, ci, fid, set))
    bot.sendSticker(chat_id=ci, sticker=fid)

def wthread(q, n):
  while True:
    task = q.get()
    try:
      task()
    except Exception as e:
      print('Exception in thread %s: %s' % (n, str(e)))
      traceback.print_exc(file=sys.stdout)
    q.task_done()

def flushqueue(bot, update):
  ci = update.message.chat_id
  fro = update.message.from_user.username
  print('%s/%d requested queue flush' % (fro, ci))
  cmdreply(bot, ci, '<flush requested>')
  for qci, rq in replyqueues.items():
    print('flushing queue %d' % (qci,))
    rq.join()
  cmdreply(bot, ci, '<done>')

def cmd_option_get(bot, update):
  ci = update.message.chat_id
  txt = update.message.text.split()
  if (len(txt) != 2):
    cmdreply(bot, ci, '< invalid syntax >')
    return
  opt = txt[1]
  val = option_get_raw(ci, opt)
  if val == None:
    cmdreply(bot, ci, '<option %s not set>' % (opt,))
  else:
    cmdreply(bot, ci, '<option %s is set to %s>' % (opt, val))

def option_valid(o, v):
  if o == 'sticker_prob' or o == 'reply_prob':
    if re.match(r'^([0-9]+|[0-9]*\.[0-9]+)$', v):
      return True
    else:
      return False
  else:
    return False

def cmd_option_set(bot, update):
  ci = update.message.chat_id
  txt = update.message.text.split()
  if (len(txt) != 3):
    cmdreply(bot, ci, '< invalid syntax, use /option_set <option> <value> >')
    return
  opt = txt[1]
  val = txt[2]
  if option_valid(opt, val):
    option_set(ci, opt, val)
    cmdreply(bot, ci, '<option %s set to %s>' % (opt, val))
  else:
    cmdreply(bot, ci, '<invalid option or value>')

def cmd_option_flush(bot, update):
  options.clear()
  cmdreply(bot, update.message.chat_id, '<done>')

def logcmd(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  txt = update.message.text
  print('[COMMAND] %s/%s: %s' % (fron, fro, txt))
  log_cmd(ci, fro, fron, txt)

helpstring = """Talk to me and I'll reply, or add me to a group and I'll talk once in a while. I don't talk in groups too much, unless you mention my name.
Commands:
/option_set reply_prob <value> - set my reply probability in this chat when my name is not mentioned. Defaults to 0.02 in groups. (0-1.0)
/option_set sticker_prob <value> - set the probability of sending a (often NSFW) sticker in place of an emoji. Defaults to 0 in groups.
/pq - forward message to @StuffSobertSays
"""

def cmd_help(bot, update):
  cmdreply(bot, update.message.chat_id, helpstring)

def cmd_pq(bot, update):
  ci, fro, fron, froi = cifrofron(update)
  msg = update.message
  if (not msg.reply_to_message) or (msg.reply_to_message.from_user.id != bot.id):
    cmdreply(bot, ci, '<send that as a reply to my message!>')
    return

  repl = msg.reply_to_message
  replid = repl.message_id

  if (repl.sticker or not repl.text):
    cmdreply(bot, ci, '<only regular text messages are supported>')
    return
  if (replid in pqed_messages) or (already_pqd(repl.text)):
    cmdreply(bot, ci, '<message already forwarded>')
    return
  if replid in command_replies:
    cmdreply(bot, ci, '<that is a silly thing to forward!>')
    return
  bot.forwardMessage(chat_id=Config.get('Telegram', 'QuoteChannel'), from_chat_id=ci, message_id=replid)
  pqed_messages.add(replid)
  log_pq(ci, froi, repl.text)

def thr_console():
  for line in sys.stdin:
    pass

downloadworker = Thread(target=wthread, args=(downloadqueue, 'download'))
downloadworker.setDaemon(True)
downloadworker.start()
consoleworker = Thread(target=thr_console, args=())
consoleworker.setDaemon(True)
consoleworker.start()

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])

updater = Updater(token=Config.get('Telegram','Token'), request_kwargs={'read_timeout': 10, 'connect_timeout': 15})
dispatcher = updater.dispatcher

dispatcher.add_handler(CommandHandler('me', me), 0)
dispatcher.add_handler(MessageHandler(Filters.command, logcmd), 0)

dispatcher.add_handler(MessageHandler(Filters.text, msg), 1)

dispatcher.add_handler(MessageHandler(Filters.sticker, sticker), 2)
dispatcher.add_handler(MessageHandler(Filters.video, video), 2)
dispatcher.add_handler(MessageHandler(Filters.document, document), 2)
dispatcher.add_handler(MessageHandler(Filters.audio, audio), 2)
dispatcher.add_handler(MessageHandler(Filters.photo, photo), 2)
dispatcher.add_handler(MessageHandler(Filters.voice, voice), 2)
dispatcher.add_handler(MessageHandler(Filters.status_update, status), 2)

dispatcher.add_handler(CommandHandler('start', start), 3)
dispatcher.add_handler(CommandHandler('givesticker', givesticker), 3)
dispatcher.add_handler(CommandHandler('flushqueue', flushqueue), 3)
dispatcher.add_handler(CommandHandler('option_get', cmd_option_get), 3)
dispatcher.add_handler(CommandHandler('option_set', cmd_option_set), 3)
dispatcher.add_handler(CommandHandler('option_flush', cmd_option_flush), 3)
dispatcher.add_handler(CommandHandler('help', cmd_help), 3)
dispatcher.add_handler(CommandHandler('pq', cmd_pq), 3)

sticker_emojis = set(get_sticker_emojis())
print("%d sticker emojis loaded" % len(sticker_emojis))

updater.start_polling(timeout=60, read_latency=30)
