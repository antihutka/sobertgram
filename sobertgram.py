from telegram.ext.dispatcher import run_async
from telegram.ext import Updater, MessageHandler, Filters, CommandHandler, CallbackContext
from telegram import ChatAction, Update
import telegram as T
import logging
import re
import sys
from time import sleep
from random import uniform
from queue import Queue
import os.path
import subprocess
from cachetools import TTLCache, cached

from configuration import Config
from database import dbcur_queryone, with_cursor, cache_on_commit
from tgdatabase import *
import threads
from httpnn import HTTPNN
import asyncio
from concurrent.futures import ThreadPoolExecutor
from util import retry, inqueue

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])

downloaded_files = set()
downloadqueue = Queue(maxsize=1024)
cmdqueue = Queue()
pqed_messages = set()
command_replies = set()
last_msg_id = {}
logger = logging.getLogger(__file__)


def put(convid, text):
  nn.run_from_thread(nn.put, str(convid), text)

async def get_cb_as(callback, convid, bad_words):
  text = await nn.get(str(convid), bad_words)
  await asyncio.get_event_loop().run_in_executor(None, callback, text)

def get_cb(callback, convid, bad_words):
  nn.run_from_thread(get_cb_as, callback, convid, bad_words)

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
  except:
    logger.exception("can't get name:")
    return '<err>'

@with_cursor
def db_get_photo(cur, fid):
  cur.execute("SELECT COUNT(*) FROM chat_files WHERE type = 'photo' AND file_id = %s", (fid,))
  return cur.fetchone()[0]

def setup_logging():
  verbose = Config.getboolean('Logging', 'VerboseStdout')
  console = logging.StreamHandler()
  console.setLevel(logging.INFO if verbose else logging.WARNING)
  logfile = logging.FileHandler(Config.get('Logging', 'Logfile'))
  logfile.setLevel(logging.INFO)
  logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO, handlers = [console, logfile])
setup_logging()

def ireplace(old, new, text):
  idx = 0
  while idx < len(text):
    index_l = text.lower().find(old.lower(), idx)
    if index_l == -1:
      return text
    text = text[:index_l] + new + text[index_l + len(old):]
    idx = index_l + len(new) 
  return text

def get_cache_key(bot, ci):
  return ci

@cached(TTLCache(1024, 60), key = get_cache_key)
def can_send_message(bot, ci):
  self_member = bot.get_chat_member(ci, bot.id)
  if self_member.status == 'restricted' and not self_member.can_send_messages:
    return False
  return True

@cached(TTLCache(1024, 600), key = get_cache_key)
def can_send_sticker(bot, ci):
  self_member = bot.get_chat_member(ci, bot.id)
  if self_member.status == 'restricted' and not self_member.can_send_other_messages:
    return False
  return True

@run_async
def send_typing_notification(bot, convid):
  try:
    bot.sendChatAction(chat_id=convid, action=ChatAction.TYPING)
  except Exception:
    logger.exception("Can't send typing action")

def try_reply(repfun, *args, **kwargs):
  while True:
    try:
      repfun(*args, **kwargs)
      return
    except Exception as e:
      if (isinstance(e, T.error.BadRequest) and 
          'reply_to_message_id' in kwargs and 
          kwargs['reply_to_message_id']):
        logger.warning('Got BadRequest, trying without reply_to_message_id')
        del kwargs['reply_to_message_id']
        continue
      logger.exception('I got this error')
      return None

def sendreply(bot, ci, fro, froi, fron, replyto=None, replyto_cond=None, conversation = None, user=None):
  if asyncio.run_coroutine_threadsafe(nn.queued_for_key(str(ci)), nn.loop).result() > 16:
    logger.warning('Warning: reply queue full, dropping reply')
    return
  send_typing_notification(bot, ci)
  badwords = get_badwords(ci)
  badwords.sort(key=len, reverse=True)
  def rf(txt):
    omsg = msg = txt
    for bw in badwords:
      msg = ireplace(bw, '*' * len(bw), msg)
    logging.info(' => %s/%s/%d: %s' % (fron, fro, ci, msg))
    if omsg != msg:
      logging.info(' (original)=> %s' % (omsg,))
    sp = option_get_float(ci, 'sticker_prob', 0.9, 0)
    if (not replyto) and replyto_cond and (replyto_cond != last_msg_id[ci]):
      reply_to = replyto_cond
    else:
      reply_to = replyto
    last_msg_id[ci] = -1
    if uniform(0, 1) < sp and can_send_sticker(bot, ci):
      rs = rand_sticker(msg)
      if rs:
        logging.info('sending as sticker %s/%s' % (rs[2], rs[0]))
        dbid = []
        log_sticker(1, msg, rs[0], rs[2], reply_to_id = replyto_cond, conversation=conversation, user=user, rowid_out = dbid)
        m = try_reply(bot.sendSticker, chat_id=ci, sticker=rs[0], reply_to_message_id = reply_to)
        if m:
          log_add_msg_id(dbid, m.message_id)
        return
    dbid = []
    log(1, msg, original_message = omsg if omsg != msg else None, reply_to_id = replyto_cond, conversation=conversation, user=user, rowid_out = dbid)
    m = try_reply(bot.sendMessage, chat_id=ci, text=msg, reply_to_message_id=reply_to)
    if m:
      log_add_msg_id(dbid, m.message_id)
  get_cb(rf, ci, badwords)

def fix_name(value):
  value = re.sub('[/<>:"\\\\|?*]', '_', value)
  return value

def download_file(bot, ftype, fid, fname, on_finish=None):
  fname = fix_name(fname)

  def df():
    filename = ftype + '/' + fname
    if os.path.isfile(filename):
      logging.info('file ' + filename + ' already exists')
      if on_finish:
        on_finish(filename)
      return
    f = bot.getFile(file_id=fid)
    logging.info('downloading file ' + filename + ' from ' + f.file_path)
    f.download(custom_path=filename, timeout=120)
    if on_finish:
      on_finish(filename)
    sleep(3)
  if downloadqueue.full():
    logging.warning('Warning: download queue full')
    if not on_finish:
      return
  downloadqueue.put(df, True, 30)
  downloaded_files.add(fid)

def getmessage(bot, ci, fro, froi, fron, txt, msg_id, message):
  logging.info('%s/%s/%d: %s' % (fron, fro, ci, txt))
  put(ci, txt)

  reply_to_id = message.reply_to_message.message_id if message.reply_to_message else None
  conversation = message.chat
  user = message.from_user
  fwduser = message.forward_from
  fwdchat = message.forward_from_chat

  log(0, txt, msg_id=msg_id, reply_to_id=reply_to_id, conversation=conversation, user=user, fwduser=fwduser, fwdchat=fwdchat)

def cifrofron(update):
  ci = update.message.chat_id
  fro = user_name(update.message.from_user)
  fron = chatname(update.message.chat)
  froi = update.message.from_user.id
  return ci, fro, fron, froi

def should_reply(bot, msg, ci, txt = None):
  if msg and msg.reply_to_message and msg.reply_to_message.from_user.id == bot.id:
    return True and can_send_message(bot, ci)
  if not txt:
    txt = msg.text
  if txt and (Config.get('Chat', 'Keyword') in txt.lower()):
    return True and can_send_message(bot, ci)
  rp = option_get_float(ci, 'reply_prob', 1, 0.02)
  return (uniform(0, 1) < rp) and can_send_message(bot, ci)

def msg(update: Update, context: CallbackContext):
  if not update.message:
    return
  ci, fro, fron, froi = cifrofron(update)
  message = update.message
  txt = update.message.text
  last_msg_id[ci] = update.message.message_id
  getmessage(context.bot, ci, fro, froi, fron, txt, update.message.message_id, update.message)
  if should_reply(context.bot, update.message, ci):
    sendreply(context.bot, ci, fro, froi, fron, replyto_cond = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)

def me(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  message = update.message
  txt = update.message.text
  last_msg_id[ci] = update.message.message_id
  getmessage(context.bot, ci, fro, froi, fron, txt, update.message.message_id, update.message)
  sendreply(context.bot, ci, fro, froi, fron, replyto_cond = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)

def sticker(update: Update, context: CallbackContext):
  if not update.message:
    return
  ci, fro, fron, froi = cifrofron(update)
  message = update.message
  last_msg_id[ci] = update.message.message_id
  st = update.message.sticker
  set = '(unnamed)' if st.set_name is None else st.set_name
  emo = st.emoji or ''
  logging.info('%s/%s/%d: [sticker <%s> <%s> < %s >]' % (fron, fro, ci, st.file_id, set, emo))
  put(ci, emo)
  log_sticker(0, emo, st.file_id, set, msg_id = update.message.message_id, reply_to_id = update.message.reply_to_message.message_id if update.message.reply_to_message else None,
    fwduser = message.forward_from, fwdchat = message.forward_from_chat,
    conversation=update.message.chat, user=update.message.from_user)
  if should_reply(context.bot, update.message, ci):
    sendreply(context.bot, ci, fro, froi, fron, replyto_cond = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)
  download_file(context.bot, 'stickers', st.file_id, st.file_id + ' ' + set + '.webp');

def video(update: Update, context: CallbackContext):
  if not update.message:
    return
  ci, fro, fron, froi = cifrofron(update)
  vid = update.message.video
  fid = vid.file_id
  attr = '%dx%d; length=%d; type=%s' % (vid.width, vid.height, vid.duration, vid.mime_type)
  size = vid.file_size
  logging.info('%s/%s: video, %d, %s, %s' % (fron, fro, size, fid, attr))
  if (Config.getboolean('Download', 'Video', fallback=True)):
    download_file(context.bot, 'video', fid, fid + '.mp4')
  log_file('video', size, attr, fid, conversation=update.message.chat, user=update.message.from_user)

def document(update: Update, context: CallbackContext):
  if not update.message:
    return
  ci, fro, fron, froi = cifrofron(update)
  doc = update.message.document
  fid = doc.file_id
  size = doc.file_size
  name = doc.file_name
  if not name:
    name = '_unnamed_.mp4'
  attr = 'type=%s; name=%s' % (doc.mime_type, name)
  logging.info('%s/%s: document, %d, %s, %s' % (fron, fro, size, fid, attr))
  if (Config.getboolean('Download', 'Document', fallback=True)):
    download_file(context.bot, 'document', fid, fid + ' ' + name)
  log_file('document', size, attr, fid, conversation=update.message.chat, user=update.message.from_user)

def audio(update: Update, context: CallbackContext):
  if not update.message:
    return
  ci, fro, fron, froi = cifrofron(update)
  aud = update.message.audio
  fid = aud.file_id
  size = aud.file_size
  ext = '.ogg'
  if aud.mime_type == 'audio/mp3':
    ext = '.mp3'
  attr = 'type=%s; duration=%d; performer=%s; title=%s' % (aud.mime_type, aud.duration, aud.performer, aud.title)
  logging.info('%s/%s: audio, %d, %s, %s' % (fron, fro, size, fid, attr))
  if (Config.getboolean('Download', 'Audio', fallback=True)):
    download_file(context.bot, 'audio', fid, '%s %s - %s%s' % (fid, aud.performer, aud.title, ext))
  log_file('audio', size, attr, fid, conversation=update.message.chat, user=update.message.from_user)

def photo(update: Update, context: CallbackContext):
  if not update.message:
    return
  ci, fro, fron, froi = cifrofron(update)
  message = update.message
  last_msg_id[ci] = update.message.message_id
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
    getmessage(context.bot, ci, fro, froi, fron, txt, update.message.message_id, update.message)
    if should_reply(context.bot, update.message, ci, txt):
      sendreply(context.bot, ci, fro, froi, fron, replyto = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)
  logging.info('%s/%s: photo, %d, %s, %s' % (fron, fro, maxsize, fid, attr))
  def process_photo(f):
    logging.info('OCR running on %s' % f)
    ocrtext = subprocess.check_output(['tesseract', f, 'stdout']).decode('utf8', errors='ignore')
    ocrtext = re.sub('[\r\n]+', '\n',ocrtext).strip()
    logging.info('OCR: "%s"' % ocrtext)
    if ocrtext == "":
      return
    log_file_text(fid, 'ocr', ocrtext)
    def process_photo_reply(_context):
      put(ci, ocrtext)
      if (Config.get('Chat', 'Keyword') in ocrtext.lower()):
        logging.info('sending reply')
        sendreply(_context.bot, ci, fro, froi, fron, replyto=update.message.message_id, conversation=update.message.chat, user = update.message.from_user)
    updater.job_queue.run_once(process_photo_reply, 0)
  download_file(context.bot, 'photo', fid, fid + '.jpg', on_finish=process_photo)
  log_file('photo', maxsize, attr, fid, conversation=update.message.chat, user=update.message.from_user)

def cmd_download_photo(update: Update, context: CallbackContext):
  if not update.message:
    return
  fid = update.message.text.split(' ')[1]
  if db_get_photo(fid):
    download_file(context.bot, 'photo', fid, fid + '.jpg')
  else:
    logger.warning('Photo not in DB')

def voice(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  voi = update.message.voice
  fid = voi.file_id
  size = voi.file_size
  attr = 'type=%s; duration=%d' % (voi.mime_type, voi.duration)
  logging.info('%s/%s: voice, %d, %s, %s' % (fron, fro, size, fid, attr))
  download_file(context.bot, 'voice', fid, fid + '.opus')
  log_file('voice', size, attr, fid, conversation=update.message.chat, user=update.message.from_user)

def status(update: Update, context: CallbackContext):
  msg = update.message
  ci, fro, fron, froi = cifrofron(update)
  upd = []
  if msg.new_chat_members:
    for mmb in msg.new_chat_members:
      upd.append(('new_member', str(mmb.id) + ' ' + user_name(mmb), mmb))
  if msg.left_chat_member:
    mmb = msg.left_chat_member
    upd.append(('left_member', str(mmb.id) + ' ' + user_name(mmb), mmb))
  if msg.new_chat_title:
    upd.append(('new_title', msg.new_chat_title, None))
  if msg.group_chat_created:
    upd.append(('group_created', '', None))
  if msg.supergroup_chat_created:
    upd.append(('supergroup_created', '', None))
  if msg.migrate_from_chat_id:
    upd.append(('migrate_from_chat_id', str(msg.migrate_from_chat_id), None))
    log_migration(ci, msg.migrate_from_chat_id)
  for u in upd:
    logging.info('[UPDATE] %s / %s: %s  %s' % (fron, fro, u[0], u[1]))
  log_status(upd, conversation=update.message.chat, user=update.message.from_user)

def cmdreply(bot, ci, text):
  logging.info('=> %s' % text)
  msg = bot.sendMessage(chat_id=ci, text=text)
  command_replies.add(msg.message_id)

def givesticker(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  foremo = None
  cmd = update.message.text
  m = re.match('^/[^ ]+ (.+)', cmd)
  if m:
    foremo = m.group(1).strip()
  rs = rand_sticker(foremo)
  if not rs:
    cmdreply(context.bot, ci, '<no sticker for %s>\n%s' % (foremo, ''.join(list(sticker_emojis_g()))))
  else:
    fid, emo, set = rs
    logging.info('%s/%s/%d: [giving random sticker: <%s> <%s>]' % (fron, fro, ci, fid, set))
    context.bot.sendSticker(chat_id=ci, sticker=fid)

def cmd_ratelimit(inf):
  def outf(update: Update, context: CallbackContext):
    if (cmd_limit_check(update.message.chat_id) > 100):
      logging.warning('rate limited!')
      return
    inf(update, context)
  return outf

@cmd_ratelimit
def start(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  logging.info('%s/%d /start' % (fro, ci))
  sendreply(context.bot, ci, fro, froi, fron, conversation=update.message.chat, user = update.message.from_user)

@cmd_ratelimit
def cmd_option_get(update: Update, context: CallbackContext):
  ci = update.message.chat_id
  txt = update.message.text.split()
  if (len(txt) != 2):
    cmdreply(context.bot, ci, '< invalid syntax >')
    return
  opt = txt[1]
  val = option_get_raw(ci, opt)
  if val == None:
    cmdreply(context.bot, ci, '<option %s not set>' % (opt,))
  else:
    cmdreply(context.bot, ci, '<option %s is set to %s>' % (opt, val))

def option_valid(o, v):
  if o == 'sticker_prob' or o == 'reply_prob' or o == 'admin_only':
    if re.match(r'^([0-9]+|[0-9]*\.[0-9]+)$', v):
      return True
    else:
      return False
  else:
    return False

def user_is_admin(bot, convid, userid):
  if convid > 0:
    return True
  member = bot.get_chat_member(convid, userid)
  if member.status == 'administrator' or member.status == 'creator':
    return True
  return False

def admin_check(bot, convid, userid):
  if option_get_float(convid, 'admin_only', 0, 0) == 0:
    return True
  return user_is_admin(bot, convid, userid)

@inqueue(cmdqueue)
@cmd_ratelimit
def cmd_option_set(update: Update, context: CallbackContext):
  ci = update.message.chat_id
  txt = update.message.text.split()
  if (len(txt) != 3):
    cmdreply(context.bot, ci, '< invalid syntax, use /option_set <option> <value> >')
    return
  if not admin_check(context.bot, ci, update.message.from_user.id):
     cmdreply(context.bot, ci, '< you are not allowed to use this command >')
     return
  opt = txt[1]
  val = txt[2]
  if option_valid(opt, val):
    option_set(ci, opt, val)
    cmdreply(context.bot, ci, '<option %s set to %s>' % (opt, val))
  else:
    cmdreply(context.bot, ci, '<invalid option or value>')

def cmd_option_flush(update: Update, context: CallbackContext):
  if not update.message:
    return
  options.clear()
  badword_cache.clear()
  cmdreply(context.bot, update.message.chat_id, '<done>')

def logcmd(update: Update, context: CallbackContext):
  if not update.message:
    return
  ci, fro, fron, froi = cifrofron(update)
  txt = update.message.text
  logging.info('[COMMAND] %s/%s: %s' % (fron, fro, txt))
  log_cmd(txt, conversation = update.message.chat, user = update.message.from_user)

helpstring = """Talk to me and I'll reply, or add me to a group and I'll talk once in a while. I don't talk in groups too much, unless you mention my name.
Commands:
/option_set reply_prob <value> - set my reply probability in this chat when my name is not mentioned. Defaults to 0.02 in groups. (0-1.0)
/option_set sticker_prob <value> - set the probability of sending a (often NSFW) sticker in place of an emoji. Defaults to 0 in groups.
/option_set admin_only <0|1> - when set to 1, only admins can change options and bad words
/badword bad_word - add or remove bad_word from the per channel bad word list. Lists bad words when used without an argument.
/pq - forward message to %s
/stats - print group/user stats
"""

@cmd_ratelimit
def cmd_help(update: Update, context: CallbackContext):
  cmdreply(context.bot, update.message.chat_id, helpstring % (Config.get('Telegram', 'QuoteChannel'),))

@inqueue(cmdqueue)
@cmd_ratelimit
def cmd_pq(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  msg = update.message
  if (not msg.reply_to_message) or (msg.reply_to_message.from_user.id != context.bot.id):
    cmdreply(context.bot, ci, '<send that as a reply to my message!>')
    return

  repl = msg.reply_to_message
  replid = repl.message_id

  if (repl.sticker or not repl.text):
    cmdreply(context.bot, ci, '<only regular text messages are supported>')
    return
  if (replid in pqed_messages) or (already_pqd(repl.text)):
    cmdreply(context.bot, ci, '<message already forwarded>')
    return
  if replid in command_replies:
    cmdreply(context.bot, ci, '<that is a silly thing to forward!>')
    return
  if pq_limit_check(froi) >= 5:
    cmdreply(context.bot, ci, '<slow down a little!>')
    return
  context.bot.forwardMessage(chat_id=Config.get('Telegram', 'QuoteChannel'), from_chat_id=ci, message_id=replid)
  pqed_messages.add(replid)
  log_pq(ci, froi, repl.text)

@inqueue(cmdqueue)
@cmd_ratelimit
def cmd_stats(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  recv, sent, firstdate, rank, trecv, tsent, actusr, actgrp, quality = db_stats(ci)
  quality_s = ("%.0f%%" % (quality*100)) if quality else "Unknown"
  cmdreply(context.bot, ci, 'Chat stats for %s:\nMessages received: %d (%d total)\nMessages sent: %d (%d total)\nFirst message: %s\nGroup/user rank: %d\n'
                    'Chat quality: %s\n'
                    'Users/groups active in the last 48 hours: %d/%d'
                    % (fron, recv, trecv, sent, tsent, firstdate.isoformat() if firstdate else 'Never', rank, quality_s, actusr, actgrp))

@cmd_ratelimit
def cmd_badword(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  msg = update.message.text
  msg_split = msg.split(' ', 1)
  bw = get_badwords(ci)
  if len(msg_split) == 1:
    cmdreply(context.bot, ci, '< Current bad words: %s (%d) >' % (' '.join((repr(w) for w in bw)), len(bw)))
  else:
    if not admin_check(context.bot, ci, froi):
      cmdreply(context.bot, ci, '< you are not allowed to use this command >')
      return
    badword = msg_split[1].strip().lower()
    if '\n' in badword:
      cmdreply(context.bot, ci, '< Bad word contains newline >')
      return
    if badword in bw:
      delete_badword(ci, badword)
      cmdreply(context.bot, ci, '< Bad word %s removed >' % (repr(badword)))
    else:
      add_badword(ci, badword, froi)
      cmdreply(context.bot, ci, '< Bad word %s added >' % (repr(badword)))

def thr_console():
  for line in sys.stdin:
    pass

loadstickers()

threads.start_thread(args=(downloadqueue, 'download'))
threads.start_thread(args=(logqueue, 'dblogger'))
threads.start_thread(args=(cmdqueue, 'commands'))
threads.start_thread(target=thr_console, args=())


nn = HTTPNN(Config.get('Backend', 'Url'), Config.get('Backend', 'Keyprefix'))
nn.run_thread()
nn.loop.set_default_executor(ThreadPoolExecutor(max_workers=4))

updater = Updater(token=Config.get('Telegram','Token'), request_kwargs={'read_timeout': 10, 'connect_timeout': 15}, use_context=True)
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
dispatcher.add_handler(CommandHandler('option_get', cmd_option_get), 3)
dispatcher.add_handler(CommandHandler('option_set', cmd_option_set), 3)
dispatcher.add_handler(CommandHandler('option_flush', cmd_option_flush), 3)
dispatcher.add_handler(CommandHandler('help', cmd_help), 3)
dispatcher.add_handler(CommandHandler('pq', cmd_pq), 3)
dispatcher.add_handler(CommandHandler('stats', cmd_stats), 3)
dispatcher.add_handler(CommandHandler('badword', cmd_badword), 3)
dispatcher.add_handler(CommandHandler('download_photo', cmd_download_photo), 3)

updater.start_polling(timeout=60, read_latency=30)
