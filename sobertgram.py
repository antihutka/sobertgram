from telegram.ext import Updater, MessageHandler, Filters, CommandHandler
from telegram import ChatAction
import logging
import socket
import MySQLdb
import re
import sys
from time import time, sleep
from random import randint
import ConfigParser
from Queue import Queue
from threading import Thread
import traceback
import os.path

Config = ConfigParser.ConfigParser()

convos = {}
times = {}
known_stickers = set()
downloaded_files = set()
replyqueue = Queue(maxsize=64)
downloadqueue = Queue(maxsize=256)
options = {}

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
      replyqueue.join()
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

def chatname(chat):
  try:
    if chat.title:
      return chat.title
    else:
      return chat.first_name + ' ' + chat.last_name
  except Exception as e:
    print "can't get name: ", str(e)
    return '<err>'

def get_dbcon():
  db = MySQLdb.connect(host=Config.get('Database', 'Host'), user=Config.get('Database', 'User'), passwd=Config.get('Database', 'Password'), db=Config.get('Database', 'Database'), charset='utf8')
  cur = db.cursor()
  cur.execute('SET NAMES utf8mb4')
  return db, cur

def log(conv, username, fromname, sent, text):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `chat` (`convid`, `from`, `chatname`, `sent`, `text`) VALUES (%s, %s, %s, %s, %s)", (conv, username, fromname, sent, text))
  db.commit()
  db.close()

def log_sticker(conv, username, fromname, sent, text, file_id, set_name):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `chat` (`convid`, `from`, `chatname`, `sent`, `text`) VALUES (%s, %s, %s, %s, %s)", (conv, username, fromname, sent, text))
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

def log_file(conv, username, chatname, ftype, fsize, attr, file_id):
  db, cur = get_dbcon()
  cur.execute("INSERT INTO `chat_files` (`convid`, `from`, `chatname`, `type`, `file_size`, `attr`, `file_id`) VALUES (%s, %s, %s, %s, %s, %s, %s)", (conv, username, chatname, ftype, fsize, attr, file_id))
  db.commit()
  db.close()

def rand_sticker():
  db, cur = get_dbcon()
  cur.execute("SELECT `file_id`, `emoji`, `set_name` FROM `stickers` ORDER BY RAND() LIMIT 1")
  row = cur.fetchone()
  db.close()
  return row

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

def sendreply(bot, ci, fro, fron):
  bot.sendChatAction(chat_id=ci, action=ChatAction.TYPING)
  getmsg = get(ci)
  def rf():
    msg = getmsg()
    print(' => %s/%s/%d: %s' % (fron, fro, ci, unicode(msg, "utf8")))
    log(ci, fro, fron, 1, msg)
    bot.sendMessage(chat_id=ci, text=msg)
  if replyqueue.full():
    print('Warning: reply queue full')
  replyqueue.put(rf)

def download_file(bot, ftype, fid, fname):
  if fid in downloaded_files:
    return
  def df():
    filename = ftype + '/' + fname
    if os.path.isfile(filename):
      print('file ' + filename + ' already exists')
      return
    f = bot.getFile(file_id=fid)
    print 'downloading file ' + filename + ' from ' + f.file_path
    f.download(custom_path=filename)
    sleep(15)
  if downloadqueue.full():
    print('Warning: download queue full')
  downloadqueue.put(df, True, 30)
  downloaded_files.add(fid)

def getmessage(bot, ci, fro, fron, txt):
  print('%s/%s/%d: %s' % (fron, fro, ci, txt))
  put(ci, txt)
  log(ci, fro, fron, 0, txt)

def cifrofron(update):
  ci = update.message.chat_id
  fro = update.message.from_user.username
  fron = chatname(update.message.chat)
  return ci, fro, fron

def msg(bot, update):
  ci, fro, fron = cifrofron(update)
  txt = update.message.text
  getmessage(bot, ci, fro, fron, txt)
  if (ci > 0) or (randint(0, 100) < 2) or (Config.get('Chat', 'Keyword') in txt.lower()):
    sendreply(bot, ci, fro, fron)
  convclean()

def start(bot, update):
  ci, fro, fron = cifrofron(update)
  print('%s/%d /start' % (fro, ci))
  sendreply(bot, ci, fro, fron)

def me(bot, update):
  ci, fro, fron = cifrofron(update)
  txt = update.message.text
  getmessage(bot, ci, fro, fron, txt)
  sendreply(bot, ci, fro, fron)

def sticker(bot, update):
  ci, fro, fron = cifrofron(update)
  st = update.message.sticker
  set = '<unnamed>' if st.set_name is None else st.set_name
  emo = st.emoji or ''
  print('%s/%s/%d: [sticker <%s> <%s> < %s >]' % (fron, fro, ci, st.file_id, set, emo))
  put(ci, emo)
  log_sticker(ci, fro, fron, 0, emo, st.file_id, set)
  #bot.sendSticker(chat_id=ci, sticker=st.file_id)
  if (ci > 0) or (randint(0, 100) < 2):
    sendreply(bot, ci, fro, fron)
  download_file(bot, 'stickers', st.file_id, st.file_id + ' ' + set + '.webp');

def video(bot, update):
  ci, fro, fron = cifrofron(update)
  vid = update.message.video
  fid = vid.file_id
  attr = '%dx%d; length=%d; type=%s' % (vid.width, vid.height, vid.duration, vid.mime_type)
  size = vid.file_size
  print('%s/%s: video, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'video', fid, fid + '.mp4')
  log_file(ci, fro, fron, 'video', size, attr, fid)

def document(bot, update):
  ci, fro, fron = cifrofron(update)
  doc = update.message.document
  fid = doc.file_id
  size = doc.file_size
  attr = 'type=%s; name=%s' % (doc.mime_type, doc.file_name)
  print('%s/%s: document, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'document', fid, fid + ' ' + doc.file_name)
  log_file(ci, fro, fron, 'document', size, attr, fid)

def audio(bot, update):
  ci, fro, fron = cifrofron(update)
  aud = update.message.audio
  fid = aud.file_id
  size = aud.file_size
  ext = '.ogg'
  if aud.mime_type == 'audio/mp3':
    ext = '.mp3'
  attr = 'type=%s; duration=%d; performer=%s; title=%s' % (aud.mime_type, aud.duration, aud.performer, aud.title)
  print('%s/%s: audio, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'audio', fid, fid + ' ' + aud.performer + ' - ' + aud.title + ext)
  log_file(ci, fro, fron, 'audio', size, attr, fid)

def photo(bot, update):
  ci, fro, fron = cifrofron(update)
  photos = update.message.photo
  maxsize = 0
  pho = None
  for photo in photos:
    if photo.file_size > maxsize and photo.file_size < 20 * 1024 * 1024:
      maxsize = photo.file_size
      pho = photo
  fid = pho.file_id
  attr = 'dim=%dx%d' % (pho.width, pho.height)
  print('%s/%s: photo, %d, %s, %s' % (fron, fro, maxsize, fid, attr))
  download_file(bot, 'photo', fid, fid + '.jpg')
  log_file(ci, fro, fron, 'photo', maxsize, attr, fid)

def voice(bot, update):
  ci, fro, fron = cifrofron(update)
  voi = update.message.voice
  fid = voi.file_id
  size = voi.file_size
  attr = 'type=%s; duration=%d' % (voi.mime_type, voi.duration)
  print('%s/%s: voice, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(bot, 'voice', fid, fid + '.opus')
  log_file(ci, fro, fron, 'voice', size, attr, fid)

def givesticker(bot, update):
  ci, fro, fron = cifrofron(update)
  fid, emo, set = rand_sticker()
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
  bot.sendMessage(chat_id=ci, text='<flush requested>')
  replyqueue.join()
  bot.sendMessage(chat_id=ci, text='<done>')

def cmd_get(bot, update):
  ci, fro, fron = cifrofron(update)
  txt = update.message.text.split()
  opt = txt[1]
  val = option_get_raw(ci, opt)
  if val == None:
    bot.sendMessage(chat_id=ci, text='<option %s not set>' % (opt,))
  else:
    bot.sendMessage(chat_id=ci, text='<option %s is set to %s>' % (opt, val))

def cmd_set(bot, update):
  ci, fro, fron = cifrofron(update)
  txt = update.message.text.split()
  opt = txt[1]
  val = txt[2]
  option_set(ci, opt, val)

replyworker = Thread(target=wthread, args=(replyqueue, 'reply'))
replyworker.setDaemon(True)
replyworker.start()
downloadworker = Thread(target=wthread, args=(downloadqueue, 'download'))
downloadworker.setDaemon(True)
downloadworker.start()

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])

updater = Updater(token=Config.get('Telegram','Token'))
dispatcher = updater.dispatcher

dispatcher.add_handler(MessageHandler(Filters.text, msg))
dispatcher.add_handler(MessageHandler(Filters.sticker, sticker))
dispatcher.add_handler(MessageHandler(Filters.video, video))
dispatcher.add_handler(MessageHandler(Filters.document, document))
dispatcher.add_handler(MessageHandler(Filters.audio, audio))
dispatcher.add_handler(MessageHandler(Filters.photo, photo))
dispatcher.add_handler(MessageHandler(Filters.voice, voice))
dispatcher.add_handler(CommandHandler('start', start))
dispatcher.add_handler(CommandHandler('me', me))
dispatcher.add_handler(CommandHandler('givesticker', givesticker))
dispatcher.add_handler(CommandHandler('flushqueue', flushqueue))
dispatcher.add_handler(CommandHandler('get', cmd_get))
dispatcher.add_handler(CommandHandler('set', cmd_set))

updater.start_polling(timeout=20, read_latency=5)
