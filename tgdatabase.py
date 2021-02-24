from queue import Queue
import logging
from cachetools import cached, TTLCache

from util import retry, inqueue
from database import dbcur_queryone, dbcur_queryrow, with_cursor, cache_on_commit

logger = logging.getLogger(__name__)
known_stickers = set()
logqueue = Queue()
sticker_emojis = None

chatinfo_last = {}
def update_chatinfo_current(cur, convid, chatinfo_id):
  if (convid not in chatinfo_last) or (chatinfo_last[convid] != chatinfo_id):
    logger.info("Updating chatinfo_current: %d -> %d" % (convid, chatinfo_id))
    cur.execute("REPLACE INTO chatinfo_current (convid, chatinfo_id) VALUES (%s, %s)", (convid, chatinfo_id))
    cache_on_commit(cur, chatinfo_last, convid, chatinfo_id)

chatinfo_cache = {}
def get_chatinfo_id(cur, chat):
  if chat is None:
    return None
  metadata = (chat.id, chat.username, chat.first_name, chat.last_name, getattr(chat, 'title', None))
  if metadata in chatinfo_cache:
    update_chatinfo_current(cur, chat.id, chatinfo_cache[metadata])
    return chatinfo_cache[metadata]
  cur.execute("SELECT chatinfo_id FROM chatinfo WHERE chat_id <=> %s AND username <=> %s AND first_name <=> %s AND last_name <=> %s AND title <=> %s FOR UPDATE", metadata)
  res = cur.fetchall()
  if res:
    cache_on_commit(cur, chatinfo_cache, metadata, res[0][0])
    update_chatinfo_current(cur, chat.id, res[0][0])
    return res[0][0]
  cur.execute("INSERT INTO chatinfo (chat_id, username, first_name, last_name, title) VALUES (%s, %s, %s, %s, %s)", metadata)
  rid = cur.lastrowid
  cache_on_commit(cur, chatinfo_cache, metadata, rid)
  update_chatinfo_current(cur, chat.id, rid)
  return rid

@inqueue(logqueue)
@retry(10)
@with_cursor
def log(cur, sent, text, original_message = None, msg_id = None, reply_to_id = None, conversation=None, user=None, rowid_out = None, fwduser = None, fwdchat = None):
  chatinfo_id = get_chatinfo_id(cur, conversation)
  userinfo_id = get_chatinfo_id(cur, user)
  fwduser_id = get_chatinfo_id(cur, fwduser)
  fwdchat_id = get_chatinfo_id(cur, fwdchat)
  conv = conversation.id
  fromid = user.id
  cur.execute("INSERT INTO `chat` (`convid`, `fromid`, `sent`, `text`, `msg_id`, chatinfo_id, userinfo_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", (conv, fromid, sent, text, msg_id, chatinfo_id, userinfo_id))
  rowid = cur.lastrowid
  if original_message:
    cur.execute("INSERT INTO `chat_original` (`id`, `original_text`) VALUES (LAST_INSERT_ID(), %s)", (original_message,))
  if reply_to_id:
    cur.execute("INSERT INTO `replies` (`id`, `reply_to`) VALUES (LAST_INSERT_ID(), %s)", (reply_to_id,))
  if fwduser or fwdchat:
    cur.execute("INSERT INTO `forwarded_from` (`id`, `fwd_userinfo_id`, `fwd_chatinfo_id`) VALUES (LAST_INSERT_ID(), %s, %s)", (fwduser_id, fwdchat_id))
  if rowid_out is not None:
    rowid_out.append(rowid)
  return rowid

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_cmd(cur, cmd, conversation = None, user = None):
  chatinfo_id = get_chatinfo_id(cur, conversation)
  userinfo_id = get_chatinfo_id(cur, user)
  conv = conversation.id
  cur.execute("INSERT INTO `commands` (`convid`, `command`, chatinfo_id, userinfo_id) VALUES (%s, %s, %s, %s)", (conv, cmd, chatinfo_id, userinfo_id))

def log_file_id(cur, file_id, file_unique_id):
  cur.execute("INSERT INTO file_ids(file_id, file_unique_id) VALUES (%s,%s)", (file_id, file_unique_id))
  return cur.lastrowid

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_sticker(cur, sent, text, file_id, file_unique_id, set_name, msg_id = None, reply_to_id = None, conversation=None, user=None, rowid_out = None, fwduser = None, fwdchat = None, learn_sticker = False):
  chatinfo_id = get_chatinfo_id(cur, conversation)
  userinfo_id = get_chatinfo_id(cur, user)
  fwduser_id = get_chatinfo_id(cur, fwduser)
  fwdchat_id = get_chatinfo_id(cur, fwdchat)
  conv = conversation.id
  fromid = user.id
  if not sent:
    fid = log_file_id(cur, file_id, file_unique_id)
    logger.info("FID logged: %d/%s/%s" % (fid, file_id, file_unique_id))
  cur.execute("INSERT INTO `chat` (`convid`, `fromid`, `sent`, `text`, `msg_id`, chatinfo_id, userinfo_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", (conv, fromid, sent, text, msg_id, chatinfo_id, userinfo_id))
  rowid = cur.lastrowid
  cur.execute("INSERT INTO `chat_sticker` (`id`, `file_id`, `set_name`) VALUES (LAST_INSERT_ID(), %s, %s)", (file_id, set_name))
  if reply_to_id:
    cur.execute("INSERT INTO `replies` (`id`, `reply_to`) VALUES (LAST_INSERT_ID(), %s)", (reply_to_id,))
  if fwduser or fwdchat:
    cur.execute("INSERT INTO `forwarded_from` (`id`, `fwd_userinfo_id`, `fwd_chatinfo_id`) VALUES (LAST_INSERT_ID(), %s, %s)", (fwduser_id, fwdchat_id))
  if learn_sticker and file_unique_id and file_unique_id not in known_stickers:
    cur.execute("SELECT COUNT(*) FROM `stickers` WHERE `file_id` = %s", (file_unique_id,))
    (exists,) = cur.fetchone()
    if exists == 0:
      logger.info("Adding sticker <%s> <%s> < %s >" % (file_unique_id, set_name, text))
      cur.execute("REPLACE INTO `stickers` (`file_id`, `emoji`, `set_name`) VALUES (%s, %s, %s)", (file_unique_id, text, set_name))
    known_stickers.add(file_unique_id)
    sticker_emojis.add(text)
  if rowid_out is not None:
    rowid_out.append(rowid)
  return rowid

def lookup_sticker_emoji(emoji):
  if emoji in sticker_emojis:
    return emoji
  emoji = emoji.strip(u'\ufe00\ufe01\ufe02\ufe03\ufe04\ufe05\ufe06\ufe07\ufe09\ufe0a\ufe0b\ufe0c\ufe0d\ufe0e\ufe0f')
  if emoji in sticker_emojis:
    return emoji
  return None

@with_cursor
def get_sticker_emojis(cur):
  cur.execute("SELECT DISTINCT `emoji` from `stickers` WHERE set_name NOT IN (SELECT set_name FROM bad_stickersets) AND `freqmod` > 0")
  rows = cur.fetchall()
  return [x[0] for x in rows]

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_add_msg_id(cur, db_id, msg_id):
  if isinstance(msg_id, list) and msg_id:
    msg_id = msg_id[0]
  cur.execute("UPDATE `chat` SET `msg_id`=%s WHERE `id`=%s AND msg_id IS NULL", (msg_id, db_id))

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_file(cur, ftype, fsize, attr, file_id, file_unique_id, conversation=None, user=None):
  chatinfo_id = get_chatinfo_id(cur, conversation)
  userinfo_id = get_chatinfo_id(cur, user)
  conv = conversation.id
  fid = log_file_id(cur, file_id, file_unique_id)
  cur.execute("INSERT INTO `chat_files` (`convid`, `type`, `file_size`, `attr`, `file_id`, chatinfo_id, userinfo_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", (conv, ftype, fsize, attr, file_id, chatinfo_id, userinfo_id))

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_status(cur, updates, conversation=None, user=None):
  chatinfo_id = get_chatinfo_id(cur, conversation)
  userinfo_id = get_chatinfo_id(cur, user)
  conv = conversation.id
  if not updates:
    return
  for u in updates:
    member_id = get_chatinfo_id(cur, u[2])
    cur.execute("INSERT INTO `status_updates` (`convid`, `type`, `value`, chatinfo_id, userinfo_id, member_id) VALUES (%s, %s, %s, %s, %s, %s)", (conv, u[0], u[1], chatinfo_id, userinfo_id, member_id))

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_migration(cur, newid, oldid):
  try:
    cur.execute("INSERT INTO `chat_migrations` (`newid`, `oldid`) VALUES (%s, %s)", (newid, oldid))
    cur.execute("UPDATE `badwords` SET `convid`=%s WHERE `convid`=%s", (newid, oldid))
    cur.execute("UPDATE `options2` SET `convid`=%s WHERE `convid`=%s", (newid, oldid))
  except:
    logger.exception("Migration failed:")

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_file_text(cur, fileid, texttype, filetext):
  cur.execute("REPLACE INTO `file_text` (`file_id`, `type`, `file_text`) VALUES (%s, %s, %s)", (fileid, texttype, filetext))

@retry(5)
@with_cursor
def is_file_downloaded(cur, uniqid):
  cur.execute("SELECT `path` FROM `downloaded_files` WHERE `unique_id` = %s", (uniqid,))
  r = cur.fetchone()
  if r:
    return r[0]
  else:
    return None

@retry(5)
@with_cursor
def log_file_download(cur, uniqid, fpath, fsize):
  cur.execute("INSERT INTO `downloaded_files` (`unique_id`, `path`, `size`) VALUES (%s, %s, %s)", (uniqid, fpath, fsize))

def get_id_by_uniq(cur, uniq_id):
  cur.execute("SELECT `file_id` FROM `file_ids` WHERE `file_unique_id` = %s ORDER BY `id` DESC LIMIT 1", (uniq_id,))
  r = cur.fetchone()
  if r:
    return r[0]
  else:
    return uniq_id

@retry(5)
@with_cursor
def rand_sticker(cur, emoji = None):
  if emoji:
    emoji = lookup_sticker_emoji(emoji)
    if not emoji:
      return None
    cur.execute("SELECT `file_id`, `emoji`, `set_name` FROM `stickers` WHERE set_name NOT IN (SELECT set_name FROM bad_stickersets) AND `freqmod` > 0 AND `emoji` = %s ORDER BY -LOG(1.0 - RAND()) / `freqmod` LIMIT 1", (emoji,))
  else:
    cur.execute("SELECT `file_id`, `emoji`, `set_name` FROM `stickers` WHERE set_name NOT IN (SELECT set_name FROM bad_stickersets) AND `freqmod` > 0 ORDER BY -LOG(1.0 - RAND()) / `freqmod` LIMIT 1")
  r = cur.fetchone()
  if r:
    return (get_id_by_uniq(cur, r[0]), r[1], r[2])
  else:
    return None

@retry(5)
@with_cursor
def already_pqd(cur, txt):
  cur.execute("SELECT COUNT(*) FROM `pq` WHERE `message` = %s", (txt,))
  (exists,) = cur.fetchone()
  if exists > 0:
    return True
  return False

@retry(5)
@with_cursor
def db_stats(cur, convid):
  s = {}
  s['recv'] = dbcur_queryone(cur, "SELECT message_count FROM `chat_counters` WHERE convid = %s AND sent=0", (convid,), 0)
  s['sent'] = dbcur_queryone(cur, "SELECT message_count FROM `chat_counters` WHERE convid = %s AND sent=1", (convid,), 0)
  s['firstdate'] = dbcur_queryone(cur, "SELECT MIN(`date`) FROM `chat` WHERE convid = %s", (convid,))
  s['rank'] = dbcur_queryone(cur, "SELECT COUNT(*)+1 FROM chat_counters WHERE SIGN(convid) = SIGN(%s) AND sent = 0 AND message_count > %s", (convid, s['recv']))
  s['trecv'] = dbcur_queryone(cur, "SELECT value FROM `counters` WHERE name='count_recv'");
  s['tsent'] = dbcur_queryone(cur, "SELECT value FROM `counters` WHERE name='count_sent'");
  s['actusr'] = dbcur_queryone(cur, "SELECT COUNT(DISTINCT convid) FROM `chat_counters` WHERE convid > 0 AND last_date > DATE_SUB(NOW(), INTERVAL 48 HOUR)")
  s['actgrp'] = dbcur_queryone(cur, "SELECT COUNT(DISTINCT convid) FROM `chat_counters` WHERE convid < 0 AND last_date > DATE_SUB(NOW(), INTERVAL 48 HOUR)")
  s['quality'], s['badness'] = dbcur_queryrow(cur, "SELECT uniqueness_rel, badness FROM chat_uniqueness LEFT JOIN chat_uniqueness_rel USING (convid)  WHERE convid = %s AND last_count_valid >= 100", (convid,), default=(None,None))
  return s

@cached(TTLCache(1024, 15*60))
@retry(3)
@with_cursor
def get_rel_uniq(cur, convid):
  return dbcur_queryone(cur, "SELECT uniqueness_rel FROM chat_uniqueness LEFT JOIN chat_uniqueness_rel USING (convid)  WHERE convid = %s AND last_count_valid >= 100", (convid,))

@inqueue(logqueue)
@retry(10)
@with_cursor
def log_pq(cur, convid, userid, txt):
  cur.execute("INSERT INTO `pq` (`convid`, `userid`, `message`) VALUES (%s, %s, %s)", (convid, userid, txt))

@retry(5)
@with_cursor
def pq_limit_check(cur, userid):
  cur.execute("SELECT COUNT(*) FROM pq WHERE userid=%s AND date > DATE_SUB(NOW(), INTERVAL 1 HOUR)", (userid,))
  res = cur.fetchone()[0]
  return res

@retry(5)
@with_cursor
def cmd_limit_check(cur, convid):
  cur.execute("SELECT COUNT(*) FROM commands WHERE convid=%s AND date > DATE_SUB(NOW(), INTERVAL 10 MINUTE) AND id > (SELECT MAX(id) - 1000 FROM commands)", (convid,))
  res = cur.fetchone()[0]
  return res

badword_cache = {}

@retry(5)
@with_cursor
def get_badwords(cur, convid):
  if convid in badword_cache:
    return badword_cache[convid]
  cur.execute("SELECT `badword` FROM `badwords` WHERE `convid` = %s", (convid,))
  r = [x[0] for x in cur]
  badword_cache[convid] = r
  return r

@retry(5)
@with_cursor
def add_badword(cur, convid, badword, by):
  cur.execute("INSERT INTO `badwords` (`convid`, `badword`, `addedby`) VALUES (%s, %s, %s)", (convid, badword, by))
  badword_cache[convid].append(badword)

@retry(5)
@with_cursor
def delete_badword(cur, convid, badword):
  cur.execute("DELETE FROM `badwords` WHERE `convid` = %s AND `badword` = %s", (convid, badword))
  badword_cache[convid].remove(badword)

@with_cursor
def get_stickers_to_migrate(cur, cnt):
  cur.execute("SELECT file_id, set_name, emoji FROM stickers WHERE length(file_id) > 22 LIMIT %s", (cnt,))
  return cur.fetchall()

@with_cursor
def update_sticker_id(cur, fileid, uniqid):
  cur.execute("SELECT COUNT(*) FROM file_ids WHERE file_id=%s", (fileid,))
  if cur.fetchone()[0] > 0:
    logger.info("File ID already logged, skipping")
  else:
    log_file_id(cur, fileid, uniqid)
  cur.execute("SELECT freqmod FROM stickers WHERE file_id=%s", (uniqid,))
  r = cur.fetchone()
  if r:
    cur.execute("SELECT freqmod FROM stickers WHERE file_id=%s", (fileid,))
    newfreq = cur.fetchone()[0]
    logger.info("Sticker already converted with freqmod=%d, new freqmod=%d", r[0], newfreq)
    if r[0] == 1 and newfreq != r[0]:
      logger.info("Changing freqmod")
      cur.execute("UPDATE stickers SET freqmod=%s WHERE file_id=%s", (newfreq,uniqid))
    cur.execute("DELETE FROM stickers WHERE file_id=%s", (fileid,))
  else:
    logger.info("Sticker not converted yet, changing id")
    cur.execute("UPDATE stickers SET file_id=%s WHERE file_id=%s", (uniqid, fileid))

@with_cursor
def db_get_photo(cur, fid):
  cur.execute("SELECT COUNT(*) FROM chat_files WHERE type = 'photo' AND file_id = %s", (fid,))
  return cur.fetchone()[0]

@cached(TTLCache(1024, 15*60))
@retry(5)
@with_cursor
def get_filtered_usernames(cur):
  cur.execute("SELECT DISTINCT username FROM options2 LEFT JOIN chatinfo_current USING (convid) LEFT JOIN chatinfo USING (chatinfo_id) WHERE filter_username > 0 AND username IS NOT NULL LIMIT 10")
  return ['@' + x[0].lower() for x in cur]

@cached(TTLCache(1024, 15*60))
@retry(5)
@with_cursor
def get_default_badwords(cur, mincount):
  cur.execute("SELECT badword,COUNT(*) "
              "FROM badwords LEFT JOIN options2 USING (convid) LEFT JOIN (SELECT convid, message_count FROM chat_counters WHERE sent=0) cnt USING (convid) "
              "WHERE COALESCE(is_bad, 0) <= 0 AND message_count >= 1000 GROUP BY badword HAVING COUNT(*) >= %s;", (mincount,))
  return [x[0] for x in cur]

def loadstickers():
  global sticker_emojis
  sticker_emojis = set(get_sticker_emojis())
  logger.info("%d sticker emojis loaded" % len(sticker_emojis))

def sticker_emojis_g():
  return sticker_emojis
