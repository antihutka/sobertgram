from telegram.ext import Updater, MessageHandler, Filters, CommandHandler
from telegram import ChatAction
import logging
import socket
import MySQLdb
import re
import sys
from time import time
from random import randint
import ConfigParser

Config = ConfigParser.ConfigParser()

timeout = 60*60*24*2
convos = {}
times = {}

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
      s = convos[convid][0]
      s.shutdown(socket.SHUT_RDWR)
      s.close()
      convos[convid][1].close()
      del convos[convid]

def put(convid, text):
  text = re.sub('[\r\n]+', '\n',text).strip("\r\n")
  try:
    (s, f) = getconv(convid)
    s.send((text + '\n').encode('utf-8'))
  except Exception as e:
    print str(e)
    del convos[convid]

def get(convid):
  try:
    (s, f) = getconv(convid)
    s.send('\n')
    return f.readline().rstrip()
  except Exception as e:
    print str(e)
    del convos[convid]
    return ''

def log(conv, username, sent, text):
  db = MySQLdb.connect(host=Config.get('Database', 'Host'), user=Config.get('Database', 'User'), passwd=Config.get('Database', 'Password'), db=Config.get('Database', 'Database'), charset='utf8')
  cur = db.cursor()
  cur.execute('SET NAMES utf8mb4')
  cur.execute("INSERT INTO `chat` (`convid`, `from`, `sent`, `text`) VALUES (%s, %s, %s, %s)", (conv, username, sent, text))
  db.commit()
  db.close()


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def sendreply(bot, ci, fro):
  bot.sendChatAction(chat_id=ci, action=ChatAction.TYPING)
  msg = get(ci)
  print('  => ' + msg)
  log(ci, fro, 1, msg)
  bot.sendMessage(chat_id=ci, text=msg)


def msg(bot, update):
  ci = update.message.chat_id
  txt = update.message.text
  fro = update.message.from_user.username
  print('%s/%d: %s' % (fro, ci, txt))
  put(ci, txt)
  log(ci, fro, 0, txt)
  if (ci > 0) or (randint(0, 100) < 2) or (Config.get('Chat', 'Keyword') in txt.lower()):
    sendreply(bot, ci, fro)
  convclean()

def start(bot, update):
  ci = update.message.chat_id
  fro = update.message.from_user.username
  print('%s/%d /start' % (fro, ci))
  sendreply(bot, ci, fro)

def me(bot, update):
  ci = update.message.chat_id
  txt = update.message.text
  fro = update.message.from_user.username
  print('%s/%d %s' % (fro, ci, txt))
  put(ci, txt)
  log(ci, fro, 0, txt)
  sendreply(bot, ci, fro)

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])

updater = Updater(token=Config.get('Telegram','Token'))
dispatcher = updater.dispatcher

dispatcher.add_handler(MessageHandler(Filters.text, msg))
dispatcher.add_handler(CommandHandler('start', start))
dispatcher.add_handler(CommandHandler('me', me))

updater.start_polling()
