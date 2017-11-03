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

def rand_sticker():
  db, cur = get_dbcon()
  cur.execute("SELECT `file_id`, `emoji`, `set_name` FROM `stickers` ORDER BY RAND() LIMIT 1")
  row = cur.fetchone()
  db.close()
  return row

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
  if downloadqueue.full():
    print('Warning: download queue full')
  downloadqueue.put(df, True, 10)
  downloaded_files.add(fid)
  sleep(5)

def getmessage(bot, ci, fro, fron, txt):
  print('%s/%s/%d: %s' % (fron, fro, ci, txt))
  put(ci, txt)
  log(ci, fro, fron, 0, txt)


def msg(bot, update):
  ci = update.message.chat_id
  txt = update.message.text
  fro = update.message.from_user.username
  fron = chatname(update.message.chat)
  getmessage(bot, ci, fro, fron, txt)
  if (ci > 0) or (randint(0, 100) < 2) or (Config.get('Chat', 'Keyword') in txt.lower()):
    sendreply(bot, ci, fro, fron)
  convclean()

def start(bot, update):
  ci = update.message.chat_id
  fro = update.message.from_user.username
  fron = chatname(update.message.chat)
  print('%s/%d /start' % (fro, ci))
  sendreply(bot, ci, fro, fron)

def me(bot, update):
  ci = update.message.chat_id
  txt = update.message.text
  fro = update.message.from_user.username
  fron = chatname(update.message.chat)
  getmessage(bot, ci, fro, fron, txt)
  sendreply(bot, ci, fro, fron)

def sticker(bot, update):
  ci = update.message.chat_id
  fro = update.message.from_user.username
  fron = chatname(update.message.chat)
  st = update.message.sticker
  set = '<unnamed>' if st.set_name is None else st.set_name
  emo = st.emoji or ''
  print('%s/%s/%d: [sticker <%s> <%s> < %s >]' % (fron, fro, ci, st.file_id, set, emo))
  put(ci, emo)
  log_sticker(ci, fro, fron, 0, emo, st.file_id, set)
  #bot.sendSticker(chat_id=ci, sticker=st.file_id)
  if (ci > 0) or (randint(0, 100) < 2):
    sendreply(bot, ci, fro, fron)
  download_file(bot, 'stickers', st.file_id, st.file_id + '.webp');

def givesticker(bot, update):
  ci = update.message.chat_id
  fro = update.message.from_user.username
  fid, emo, set = rand_sticker()
  fron = chatname(update.message.chat)
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
dispatcher.add_handler(CommandHandler('start', start))
dispatcher.add_handler(CommandHandler('me', me))
dispatcher.add_handler(CommandHandler('givesticker', givesticker))
dispatcher.add_handler(CommandHandler('flushqueue', flushqueue))

updater.start_polling()
