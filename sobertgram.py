from telegram.ext import Updater, MessageHandler, filters, CommandHandler, CallbackContext, ApplicationBuilder
from telegram import Update
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
import asyncache
from pathlib import Path
import unicodedata
import options

from configuration import Config
from database import dbcur_queryone, with_cursor, cache_on_commit
import tgdatabase
from tgdatabase import *
import threads
from httpnn import HTTPNN
import asyncio
from concurrent.futures import ThreadPoolExecutor
from util import retry, inqueue, KeyCounters
from commandfilter import is_nonstandard_command

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])

downloaded_files = set()
downloadqueue = Queue()
cmdqueue = Queue()
miscqueue = Queue()
pqed_messages = set()
command_replies = set()
last_msg_id = {}
logger = logging.getLogger(__name__)

def get_backend_for(convid):
  bid = options.get_option(convid, 'backend')
  if bid not in backends:
    bid = 0
  return backends[bid]

def put(convid, text):
  backend = get_backend_for(convid)
  return backend.put(str(convid), text)

def get(convid, bad_words):
  backend = get_backend_for(convid)
  return backend.get(str(convid), bad_words)

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

@asyncache.cached(TTLCache(1024, 120), key = get_cache_key)
async def can_send_message(bot, ci):
  self_member = await bot.get_chat_member(ci, bot.id)
  if self_member.status == 'restricted' and not self_member.can_send_messages:
    return False
  return True

@asyncache.cached(TTLCache(1024, 600), key = get_cache_key)
async def can_send_sticker(bot, ci):
  self_member = await bot.get_chat_member(ci, bot.id)
  if self_member.status == 'restricted' and not self_member.can_send_other_messages:
    return False
  return True

async def send_typing_notification(bot, convid):
  try:
    await bot.sendChatAction(chat_id=convid, action=T.constants.ChatAction.TYPING)
  except Exception:
    logger.exception("Can't send typing action")

async def try_reply(repfun, *args, **kwargs):
  while True:
    try:
      m = await repfun(*args, **kwargs)
      return m
    except Exception as e:
      if (isinstance(e, T.error.BadRequest) and 
          'reply_to_message_id' in kwargs and 
          kwargs['reply_to_message_id']):
        logger.warning('Got BadRequest, trying without reply_to_message_id')
        del kwargs['reply_to_message_id']
        continue
      if (isinstance(e, T.error.Unauthorized)):
        logger.warning('Unauthorized, bot kicked from group?')
        return None
      logger.exception('I got this error')
      return None

async def sendreply(bot, ci, fro, froi, fron, replyto=None, replyto_cond=None, conversation = None, user=None):
  backend = get_backend_for(ci)
  if await backend.queued_for_key(str(ci)) > 16:
    logger.warning('Warning: reply queue full, dropping reply')
    return
  await send_typing_notification(bot, ci)
  badwords = get_badwords(ci) + get_badwords(0) + tgdatabase.get_filtered_usernames()
  gbwcount = options.get_option(ci, 'default_badwords')
  if gbwcount >= 3:
    badwords += tgdatabase.get_default_badwords(gbwcount)
  msg = await get(ci, badwords)
  logger.info(' => %s/%s/%d: %s' % (fron, fro, ci, msg))
  sp = options.get_option(ci, 'sticker_prob')
  orpl = options.get_option(ci, 'send_as_reply')
  if (not replyto) and replyto_cond and orpl > 0 and (replyto_cond != last_msg_id[ci] or orpl > 1):
    reply_to = replyto_cond
  else:
    reply_to = replyto
  last_msg_id[ci] = -1
  if uniform(0, 1) < sp and await can_send_sticker(bot, ci):
    rs = rand_sticker(msg)
    if rs:
      logger.info('sending as sticker %s/%s' % (rs[2], rs[0]))
      dbid = []
      log_sticker(1, msg, rs[0], None, rs[2], reply_to_id = replyto_cond, conversation=conversation, user=user, rowid_out = dbid)
      m = await try_reply(bot.sendSticker, chat_id=ci, sticker=rs[0], reply_to_message_id = reply_to)
      if m:
        log_add_msg_id(dbid, m.message_id)
      return
  dbid = []
  log(1, msg, original_message = None, reply_to_id = replyto_cond, conversation=conversation, user=user, rowid_out = dbid)
  m = await try_reply(bot.sendMessage, chat_id=ci, text=msg, reply_to_message_id=reply_to)
  if m:
    log_add_msg_id(dbid, m.message_id)

def fix_name(value):
  value = re.sub('[/<>:"\\\\|?*]', '_', value)
  return value

downloads_per_chat = KeyCounters()

def download_file(bot, fid, filename, convid, on_finish=None):
  def df():
    downloads_per_chat.dec(convid)
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    f = bot.getFile(file_id=fid)
    existingpath = is_file_downloaded(f.file_unique_id)
    if existingpath:
      logger.info('file %s already downloaded as %s', filename, existingpath)
      if on_finish:
        on_finish(existingpath)
      return
    logger.info('downloading file %s from %s, %d/%d pending' % (filename, f.file_path, downloads_per_chat[convid], downloadqueue.qsize()))
    f.download(custom_path=filename, timeout=120)
    log_file_download(f.file_unique_id, filename, f.file_size)
    if on_finish:
      on_finish(filename)
    sleep(1)
  pending_chat = downloads_per_chat[convid]
  if pending_chat > 20:
    logger.warning("Skipping download - %d downloads pending for chat, %d downloads pending total" % (pending_chat, downloadqueue.qsize()))
    return
  downloads_per_chat.inc(convid)
  downloadqueue.put(df, True, 30)
  downloaded_files.add(fid)

async def getmessage(bot, ci, fro, froi, fron, txt, msg_id, message):
  logger.info('%s/%s/%d: %s' % (fron, fro, ci, txt))
  await put(ci, txt)

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

async def should_reply(bot, msg, ci, txt = None):
  if msg and msg.reply_to_message and msg.reply_to_message.from_user.id == bot.id:
    return True and await can_send_message(bot, ci)
  if not txt:
    txt = msg.text
  if txt and (Config.get('Chat', 'Keyword') in txt.lower()):
    return True and await can_send_message(bot, ci)
  rp = options.get_option(ci, 'reply_prob')
  return (uniform(0, 1) < rp) and await can_send_message(bot, ci)

class BlacklistingLogger():
  def __init__(self):
    self.dropped = 0
  def bad(self):
    self.dropped += 1
  def good(self):
    if self.dropped == 0:
      return
    logger.info("[blacklist] %d events dropped", self.dropped)
    self.dropped = 0

blkLog = BlacklistingLogger()

# TODO test/fix for blacklisted chats
def update_wrap(f):
  def wrapped(update: Update, context: CallbackContext):
    if not update.message:
      return
    is_blacklisted = options.get_option(update.message.chat_id, 'blacklisted') > 0 or options.get_option(update.message.from_user.id, 'user_blacklisted') > 0
    if is_blacklisted:
      blkLog.bad()
      return
    blkLog.good()
    return f(update = update, context = context)
  return wrapped

@update_wrap
async def msg(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  message = update.message
  txt = update.message.text
  if options.get_option(ci, 'ignore_commands') > 0 and is_nonstandard_command(txt):
    return
  last_msg_id[ci] = update.message.message_id
  await getmessage(context.bot, ci, fro, froi, fron, txt, update.message.message_id, update.message)
  if await should_reply(context.bot, update.message, ci):
    await sendreply(context.bot, ci, fro, froi, fron, replyto_cond = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)

@update_wrap
def me(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  message = update.message
  txt = update.message.text
  last_msg_id[ci] = update.message.message_id
  getmessage(context.bot, ci, fro, froi, fron, txt, update.message.message_id, update.message)
  sendreply(context.bot, ci, fro, froi, fron, replyto_cond = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)

def emojiname(emoji):
  try:
    return ' '.join((unicodedata.name(e) for e in emoji))
  except:
    return 'unknown'

@update_wrap
def sticker(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  message = update.message
  last_msg_id[ci] = update.message.message_id
  st = update.message.sticker
  set = '(unnamed)' if st.set_name is None else st.set_name
  emo = st.emoji or ''
  logger.info('%s/%s/%d: [sticker <%s> <%s> < %s >]' % (fron, fro, ci, st.file_id, set, emo))
  put(ci, emo)
  reluniq = tgdatabase.get_rel_uniq(ci)
  can_learn = (options.get_option(ci, 'is_bad') < 1) and (reluniq is not None) and (reluniq > 0.5)
  logger.info("uniq/canlearn %s %s", reluniq, can_learn)
  log_sticker(0, emo, st.file_id, st.file_unique_id, set, msg_id = update.message.message_id, reply_to_id = update.message.reply_to_message.message_id if update.message.reply_to_message else None,
    fwduser = message.forward_from, fwdchat = message.forward_from_chat,
    conversation=update.message.chat, user=update.message.from_user,
    learn_sticker = can_learn)
  #await this
  if should_reply(context.bot, update.message, ci):
    sendreply(context.bot, ci, fro, froi, fron, replyto_cond = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)
  download_file(context.bot, st.file_id, 'stickers2/%s/%s %s.%s' % (fix_name(set), fix_name(st.file_unique_id), fix_name(emojiname(emo)), 'tgs' if st.is_animated else 'webp'), convid=ci);

@update_wrap
def video(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  vid = update.message.video
  fid = vid.file_id
  uid = vid.file_unique_id
  attr = '%dx%d; length=%d; type=%s' % (vid.width, vid.height, vid.duration, vid.mime_type)
  size = vid.file_size
  logger.info('%s/%s: video, %d, %s, %s' % (fron, fro, size, fid, attr))
  if options.get_option(ci, 'ignore_files') > 0:
    return
  if (Config.getboolean('Download', 'Video', fallback=True)):
    download_file(context.bot, fid, 'video/' + fix_name(uid) + '.mp4', convid=ci)
  log_file('video', size, attr, fid, uid, conversation=update.message.chat, user=update.message.from_user)

@update_wrap
def document(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  doc = update.message.document
  fid = doc.file_id
  uid = doc.file_unique_id
  size = doc.file_size
  name = doc.file_name
  if not name:
    name = '_unnamed_.mp4'
  attr = 'type=%s; name=%s' % (doc.mime_type, name)
  logger.info('%s/%s: document, %d, %s, %s' % (fron, fro, size, fid, attr))
  if options.get_option(ci, 'ignore_files') > 0:
    return
  if (Config.getboolean('Download', 'Document', fallback=True)):
    download_file(context.bot, fid, 'document/' + fix_name(uid) + ' ' + fix_name(name), convid=ci)
  log_file('document', size, attr, fid, uid, conversation=update.message.chat, user=update.message.from_user)

@update_wrap
def audio(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  aud = update.message.audio
  fid = aud.file_id
  uid = aud.file_unique_id
  size = aud.file_size
  ext = '.ogg'
  if aud.mime_type == 'audio/mp3':
    ext = '.mp3'
  attr = 'type=%s; duration=%d; performer=%s; title=%s' % (aud.mime_type, aud.duration, aud.performer, aud.title)
  logger.info('%s/%s: audio, %d, %s, %s' % (fron, fro, size, fid, attr))
  if options.get_option(ci, 'ignore_files') > 0:
    return
  if (Config.getboolean('Download', 'Audio', fallback=True)):
    download_file(context.bot, fid, 'audio/' + '%s %s - %s%s' % (fix_name(uid), fix_name(aud.performer), fix_name(aud.title), fix_name(ext)), convid=ci)
  log_file('audio', size, attr, fid, uid, conversation=update.message.chat, user=update.message.from_user)

@update_wrap
def photo(update: Update, context: CallbackContext):
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
  uid = pho.file_unique_id
  attr = 'dim=%dx%d' % (pho.width, pho.height)
  if txt:
    attr += '; caption=' + txt
    getmessage(context.bot, ci, fro, froi, fron, txt, update.message.message_id, update.message)
    # await this
    if should_reply(context.bot, update.message, ci, txt):
      sendreply(context.bot, ci, fro, froi, fron, replyto = update.message.message_id, conversation=update.message.chat, user = update.message.from_user)
  logger.info('%s/%s: photo, %d, %s, %s' % (fron, fro, maxsize, fid, attr))
  def process_photo(f):
    logger.info('OCR running on %s' % f)
    ocrtext = subprocess.check_output(['tesseract', f, 'stdout']).decode('utf8', errors='ignore')
    ocrtext = re.sub('[\r\n]+', '\n',ocrtext).strip()
    logger.info('OCR: "%s"' % ocrtext)
    if ocrtext == "":
      return
    log_file_text(fid, 'ocr', ocrtext)
    if options.get_option(ci, 'is_bad') > 0:
      ocrtext = ocrtext[-100:]
    def process_photo_reply(_context):
      put(ci, ocrtext)
      if (Config.get('Chat', 'Keyword') in ocrtext.lower()):
        logger.info('sending reply')
        sendreply(_context.bot, ci, fro, froi, fron, replyto=update.message.message_id, conversation=update.message.chat, user = update.message.from_user)
    updater.job_queue.run_once(process_photo_reply, 0)
  if options.get_option(ci, 'ignore_files') > 0:
    return
  download_file(context.bot, fid, 'photo/' + fix_name(uid) + '.jpg', on_finish=process_photo, convid=ci)
  log_file('photo', maxsize, attr, fid, uid, conversation=update.message.chat, user=update.message.from_user)

@update_wrap
def cmd_download_photo(update: Update, context: CallbackContext):
  fid = update.message.text.split(' ')[1]
  if db_get_photo(fid):
    download_file(context.bot, fid, 'photo/' + fix_name(uid) + '.jpg', convid=0)
  else:
    logger.warning('Photo not in DB')

@update_wrap
def voice(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  voi = update.message.voice
  fid = voi.file_id
  uid = voi.file_unique_id
  size = voi.file_size
  attr = 'type=%s; duration=%d' % (voi.mime_type, voi.duration)
  logger.info('%s/%s: voice, %d, %s, %s' % (fron, fro, size, fid, attr))
  if options.get_option(ci, 'ignore_files') > 0:
    return
  download_file(context.bot, fid, 'voice/' + fix_name(uid) + '.opus', convid=ci)
  log_file('voice', size, attr, fid, uid, conversation=update.message.chat, user=update.message.from_user)

@update_wrap
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
    logger.info('[UPDATE] %s / %s: %s  %s' % (fron, fro, u[0], u[1]))
  log_status(upd, conversation=update.message.chat, user=update.message.from_user)

def cmdreply(bot, ci, text):
  if options.get_option(ci, 'silent_commands') > 0:
    logger.info('=> [silent] %s', text)
    return
  logger.info('=> %s' % text)
  msg = bot.sendMessage(chat_id=ci, text=text)
  command_replies.add((ci, msg.message_id))

@update_wrap
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
    logger.info('%s/%s/%d: [giving random sticker: <%s> <%s>]' % (fron, fro, ci, fid, set))
    context.bot.sendSticker(chat_id=ci, sticker=fid)

def cmd_ratelimit(inf):
  def outf(update: Update, context: CallbackContext):
    if (cmd_limit_check(update.message.chat_id) > 100):
      logger.warning('rate limited!')
      return
    inf(update, context)
  return outf

@update_wrap
@cmd_ratelimit
def start(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  if options.get_option(ci, 'silent_commands') > 0:
    logger.info('ignoring /start')
    return
  logger.info('%s/%d /start' % (fro, ci))
  sendreply(context.bot, ci, fro, froi, fron, conversation=update.message.chat, user = update.message.from_user)

def user_is_admin(bot, convid, userid, owner_only):
  if convid > 0:
    return True
  member = bot.get_chat_member(convid, userid)
  if (member.status == 'administrator' and not owner_only) or member.status == 'creator':
    return True
  return False

def admin_check(bot, convid, userid, option_name='admin_only'):
  if options.get_option(convid, option_name) == 0:
    return True
  return user_is_admin(bot, convid, userid, options.get_option(convid, option_name) >= 2)

@update_wrap
@inqueue(cmdqueue)
@cmd_ratelimit
def cmd_option_set(update: Update, context: CallbackContext):
  ci = update.message.chat_id
  txt = update.message.text.split()
  if (len(txt) != 3):
    cmdreply(context.bot, ci, 'Invalid syntax, use /option_set <option> <value>.\nUse /option_list to list options.')
    return
  if not admin_check(context.bot, ci, update.message.from_user.id):
     cmdreply(context.bot, ci, '< you are not allowed to use this command >')
     return
  opt = txt[1]
  val = txt[2]
  try:
    options.set_option(ci, opt, val)
    cmdreply(context.bot, ci, '<option %s set to %s>' % (opt, val))
  except options.OptionError as oe:
    cmdreply(context.bot, ci, '<%s>' % (str(oe),))

@update_wrap
def cmd_option_flush(update: Update, context: CallbackContext):
  options.optioncache.clear()
  badword_cache.clear()
  cmdreply(context.bot, update.message.chat_id, '<done>')

@update_wrap
@inqueue(cmdqueue)
@cmd_ratelimit
def cmd_option_list(update: Update, context: CallbackContext):
  ci = update.message.chat_id
  repl = 'Options, use /option_set <name> <value> to change:'
  for opt in options.options.values():
    if not opt.settable:
      continue
    if not opt.visible:
      continue
    repl += "\n========\n%s (%s)" % (opt.name, opt.type.__name__)
    if opt.default_user == opt.default_group:
      repl += "\nDefault: %s" % opt.default_user
    else:
      repl += "\nDefault: users: %s, groups: %s" % (opt.default_user, opt.default_group)
    repl += "\nCurrent value: %s" % (options.get_option(ci, opt.name))
    repl += "\n%s" % (opt.description)
  cmdreply(context.bot, update.message.chat_id, repl)

@update_wrap
def logcmd(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  txt = update.message.text
  logger.info('[COMMAND] %s/%s: %s' % (fron, fro, txt))
  log_cmd(txt, conversation = update.message.chat, user = update.message.from_user)

helpstring = """Talk to me and I'll reply, or add me to a group and I'll talk once in a while. I don't talk in groups too much, unless you mention my name.
Commands:
/option_list - Show all configurable options
/option_set <option_name> <value> - Set an option
/badword bad_word - add or remove bad_word from the per channel bad word list. Lists bad words when used without an argument.
/pq - forward message to %s
/stats - print group/user stats
"""

@update_wrap
@cmd_ratelimit
def cmd_help(update: Update, context: CallbackContext):
  cmdreply(context.bot, update.message.chat_id, helpstring % (Config.get('Telegram', 'QuoteChannel'),))

@update_wrap
@inqueue(cmdqueue)
@cmd_ratelimit
def cmd_pq(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)

  if not admin_check(context.bot, ci, update.message.from_user.id, option_name='admin_only_pq'):
     cmdreply(context.bot, ci, '< you are not allowed to use this command >')
     return

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
  if (ci, replid) in command_replies:
    cmdreply(context.bot, ci, '<that is a silly thing to forward!>')
    return
  if pq_limit_check(froi) >= 5:
    cmdreply(context.bot, ci, '<slow down a little!>')
    return
  context.bot.forwardMessage(chat_id=Config.get('Telegram', 'QuoteChannel'), from_chat_id=ci, message_id=replid)
  pqed_messages.add(replid)
  log_pq(ci, froi, repl.text)

@update_wrap
@inqueue(cmdqueue)
@cmd_ratelimit
def cmd_stats(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  s = db_stats(ci)
  quality_s = ("%.0f%%" % (s['quality']*100)) if s['quality'] else "Unknown"
  lin = []
  lin.append('Chat stats for %s:' % fron)
  lin.append('Messages received: %d (%d total)' % (s['recv'], s['trecv']))
  lin.append('Messages sent: %d (%d total)' % (s['sent'], s['tsent']))
  lin.append('First message: %s' % (s['firstdate'].isoformat() if s['firstdate'] else 'Never'))
  lin.append('Group/user rank: %d' % s['rank'])
  lin.append('Chat quality: %s' % quality_s)
  if s['badness'] is not None:
    lin.append('Chat badness: %.1f%%' % (s['badness'] * 100))
  lin.append('Users/groups active in the last 48 hours: %d/%d' % (s['actusr'], s['actgrp']))
  cmdreply(context.bot, ci, '\n'.join(lin))

@update_wrap
@cmd_ratelimit
def cmd_badword(update: Update, context: CallbackContext):
  ci, fro, fron, froi = cifrofron(update)
  msg = update.message.text
  msg_split = msg.split(' ', 1)
  bw = get_badwords(ci)
  if len(msg_split) == 1:
    repl = 'Current bad words: %s (%d)' % (', '.join((repr(w) for w in bw)), len(bw))
    gbw = options.get_option(ci, 'default_badwords')
    if gbw >= 3:
      gbws = tgdatabase.get_default_badwords(gbw)
      repl += "\nDefault bad words: %s (%d)" % (', '.join(repr(w) for w in gbws), len(gbws))
    cmdreply(context.bot, ci, repl)
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
    elif any((existing in badword) for existing in bw):
      ebws = list(filter(lambda e: e in badword, bw))
      cmdreply(context.bot, ci, '< Bad word %s already matched by %s >' % (repr(badword), repr(ebws)))
    else:
      redundant = list(filter(lambda e: badword in e, bw))
      add_badword(ci, badword, froi, remove = redundant)
      rmsg = ("\n< Redundant badwords %s removed >" % repr(redundant)) if redundant else ""
      cmdreply(context.bot, ci, '< Bad word %s added >%s' % (repr(badword), rmsg))

import magic

@inqueue(miscqueue)
@update_wrap
def cmd_migrate_stickers(update: Update, context: CallbackContext):
  msg_split = update.message.text.split(' ', 1)
  cnt = int(msg_split[1]) if len(msg_split) > 1 else 1
  logger.info('Migrating %d stickers' % cnt)
  already_downloaded = 0
  deleted = 0
  moved = 0
  missing = 0
  for (fid, set, emoji) in get_stickers_to_migrate(cnt):
    logger.info('Migrating %s' % fid)
    try:
      fil = context.bot.get_file(fid)
    except:
      logger.exception("Can't get file, skipping")
      continue
    logger.info('File info: %s' % repr(fil.__dict__))
    origpath = 'stickers/' + fix_name(fid) + ' ' + fix_name(set) + '.webp'
    if is_file_downloaded(fil.file_unique_id):
      already_downloaded += 1
      logger.info('File already downloaded')
      if os.path.isfile(origpath):
        logger.info('Original file exists')
        os.remove(origpath)
        deleted += 1
      update_sticker_id(fid, fil.file_unique_id)
    elif os.path.isfile(origpath):
      logger.info('Original %s exists', origpath)
      mimetype = magic.from_file(origpath, mime=True)
      extension = 'tgs' if mimetype == 'application/gzip' else 'webp'
      newpath = 'stickers2/%s/%s %s.%s' % (fix_name(set), fix_name(fil.file_unique_id), fix_name(emojiname(emoji)), extension)
      logger.info('Type %s, New path is %s', mimetype, newpath)
      Path(newpath).parent.mkdir(parents=True, exist_ok=True)
      os.rename(origpath, newpath)
      log_file_download(fil.file_unique_id, newpath, fil.file_size)
      update_sticker_id(fid, fil.file_unique_id)
      moved += 1
    else:
      logger.info('Original %s missing, updating %s->%s', origpath, fid, fil.file_unique_id)
      update_sticker_id(fid, fil.file_unique_id)
      missing += 1
  cmdreply(context.bot, update.message.chat_id, "Done [%d/%d/%d/%d]" % (already_downloaded, deleted, moved, missing))

@inqueue(cmdqueue)
@update_wrap
def cmd_secret_for(update: Update, context: CallbackContext):
  msg_split = update.message.text.split(' ', 1)
  userid = update.message.from_user.id if len(msg_split) < 2 else int(msg_split[1])
  stats = tgdatabase.userstats(userid)
  cmdreply(context.bot, update.message.chat_id, 'Stats for %s' % stats)

def thr_console():
  for line in sys.stdin:
    pass

loadstickers()

threads.start_thread(args=(downloadqueue, 'download'))
threads.start_thread(args=(logqueue, 'dblogger'))
threads.start_thread(args=(cmdqueue, 'commands'))
threads.start_thread(args=(miscqueue, 'misc'))
threads.start_thread(target=thr_console, args=())


nn = HTTPNN(Config.get('Backend', 'Url'), Config.get('Backend', 'Keyprefix'))
nn.initialize2()
#nn.run_thread()
#nnexec = ThreadPoolExecutor(max_workers=4)
#nn.loop.set_default_executor(nnexec)
backends = {}
backends[0] = nn
for section in (x for x in Config.sections() if x.startswith('Backend:')):
  logger.info("Initializing backend %s", section)
  annid = int(section.split(':')[1])
  annurl = Config.get(section, 'Url')
  ann = HTTPNN(annurl, Config.get('Backend', 'Keyprefix'))
  #ann.run_thread()
  #ann.loop.set_default_executor(nnexec)
  backends[annid] = ann

app = ApplicationBuilder().token(Config.get('Telegram','Token')).concurrent_updates(True).build()

app.add_handler(CommandHandler('me', me), 0)
app.add_handler(MessageHandler(filters.COMMAND, logcmd), 0)

app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), msg), 1)

app.add_handler(MessageHandler(filters.Sticker.ALL, sticker), 2)
app.add_handler(MessageHandler(filters.VIDEO, video), 2)
app.add_handler(MessageHandler(filters.Document.ALL, document), 2)
app.add_handler(MessageHandler(filters.AUDIO, audio), 2)
app.add_handler(MessageHandler(filters.PHOTO, photo), 2)
app.add_handler(MessageHandler(filters.VOICE, voice), 2)
app.add_handler(MessageHandler(filters.StatusUpdate.ALL, status), 2)

app.add_handler(CommandHandler('start', start), 3)
app.add_handler(CommandHandler('givesticker', givesticker), 3)
app.add_handler(CommandHandler('option_set', cmd_option_set), 3)
app.add_handler(CommandHandler('option_flush', cmd_option_flush), 3)
app.add_handler(CommandHandler('option_list', cmd_option_list), 3)
app.add_handler(CommandHandler('help', cmd_help), 3)
app.add_handler(CommandHandler('pq', cmd_pq), 3)
app.add_handler(CommandHandler('stats', cmd_stats), 3)
app.add_handler(CommandHandler('badword', cmd_badword), 3)
app.add_handler(CommandHandler('download_photo', cmd_download_photo), 3)
app.add_handler(CommandHandler('migrate_stickers', cmd_migrate_stickers, filters=filters.User(user_id=Config.getint('Admin', 'Admin'))), 3)
app.add_handler(CommandHandler('secret_for', cmd_secret_for, filters=filters.User(user_id=Config.getint('Admin', 'Admin'))), 3)

app.run_polling()
