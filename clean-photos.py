import time
import os
import sys
import random

from configuration import Config
from database import with_cursor
from MySQLdb._exceptions import OperationalError

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])



def getcounts(cursor, fid):
  cursor.execute("SELECT SUM(IF(COALESCE(is_bad, 0) = 0 AND COALESCE(delete_photos, 0) = 0, 1, 0)), SUM(IF(COALESCE(is_bad, 0) > 0 OR COALESCE(delete_photos, 0) > 0, 1, 0)) FROM chat_files LEFT JOIN options2 USING (convid) WHERE file_id=%s", (fid,))
  r = cursor.fetchone()
  #print('getcounts %s->%s' % (fid, r))
  return r

def getcounts_u(cursor, fid):
  cursor.execute("SELECT SUM(IF(COALESCE(is_bad, 0) = 0 AND COALESCE(delete_photos, 0) = 0, 1, 0)), SUM(IF(COALESCE(is_bad, 0) > 0 OR COALESCE(delete_photos, 0) > 0, 1, 0)) FROM file_ids LEFT JOIN chat_files USING (file_id) LEFT JOIN options2 USING (convid) WHERE file_unique_id=%s", (fid,))
  r = cursor.fetchone()
  #print('getcounts_u %s->%s' % (fid, r))
  return r

def getage(cursor, fid):
  cursor.execute("SELECT MAX(UNIX_TIMESTAMP(date)) FROM chat_files WHERE file_id=%s", (fid,))
  return cursor.fetchone()[0]

def getage_u(cursor, fid):
  cursor.execute("SELECT MAX(UNIX_TIMESTAMP(added_on)) FROM file_ids WHERE file_unique_id=%s", (fid,))
  return cursor.fetchone()[0]

def missing_fidrow(cursor, fuid):
  cursor.execute("SELECT COUNT(*) FROM file_ids WHERE file_unique_id=%s", (fuid,))
  return cursor.fetchone()[0] == 0

@with_cursor
def delete_dbentry(cursor, fid):
  cursor.execute("DELETE FROM downloaded_files WHERE unique_id=%s", (fid,))
  #print(cursor.rowcount)
  #cursor.execute("SELECT COUNT(*) FROM downloaded_files WHERE unique_id=%s", (fid,))
  #print(cursor.fetchall())

minage = 7
minsize = 16384

@with_cursor
def check_files(cursor, directory, extension):
  curtime = time.time()
  files = os.listdir(directory + '/')
  random.shuffle(files)
  print("%d %s files found" % (len(files), directory))
  proccnt = 0
  totalcnt = 0
  uniqidcnt = 0
  newcnt = 0
  goodcnt = 0
  delsize = 0
  for filename in files:
    totalcnt += 1
    fullname = directory + '/' + filename
    if not filename.endswith(extension):
      continue
    fileid = filename[:-len(extension)]
    mtime = os.path.getmtime(fullname)
    filesize = os.path.getsize(fullname)
    fileage = (curtime-mtime)/60/60/24
    is_orphan = False
    assert fileage >= 0
    if fileage < minage:
      newcnt += 1
      continue
    if len(fileid) < 28:
      uniqidcnt += 1
      good, bad = getcounts_u(cursor, fileid)
      lastdate = getage_u(cursor, fileid)
      isuniq = True
      is_orphan = missing_fidrow(cursor, fileid)
    else:
      good, bad = getcounts(cursor, fileid)
      lastdate = getage(cursor, fileid)
      isuniq = False
    if (good is None) or (bad is None):
      print('File not in DB? %s age %.2f orphan=%s' % (fileid, fileage, is_orphan))
      if not is_orphan:
        continue
    lastage = 0 if lastdate is None else (curtime-lastdate)/60/60/24
    assert lastage>0 or is_orphan
    if lastage < minage and not is_orphan:
      newcnt += 1
      continue
    #print('%s %s %s' % (fileid, good, bad))
    if (not is_orphan) and (good > 0) and (filesize > minsize):
      goodcnt += 1
      continue
    assert is_orphan or bad > 0 or filesize <= minsize

    proccnt += 1
    delsize += filesize
    print("[T: %6d U: %6d N: %6d G: %6d P:%6d %.2f MB] File %s %.1f kB %.2f/%.2f days old good %d bad %d" % (totalcnt, uniqidcnt, newcnt, goodcnt, proccnt, delsize / 1024 / 1024, fileid, filesize / 1024, fileage, lastage, 0 if good is None else good, 0 if bad is None else bad))
    #print("Deleting %s" % fullname)
    if isuniq:
      delete_dbentry(fileid)
    os.remove(fullname)
    time.sleep(0.1)

@with_cursor
def check_file_text(cur):
  cur.execute("SELECT file_id, type FROM file_text WHERE TIMESTAMPDIFF(DAY, date, CURRENT_TIMESTAMP) > 90 LIMIT 50000")
  res = cur.fetchall()
  print("Deleting %d old file_text rows" % len(res))
  for r in res:
    cur.execute("DELETE FROM file_text WHERE file_id=%s AND type=%s", r)

@with_cursor
def check_chat_files(cur):
  cur.execute("SELECT id FROM chat_files LEFT JOIN options2 USING (convid) WHERE TIMESTAMPDIFF(DAY, date, CURRENT_TIMESTAMP) > 90 AND is_bad > 0 AND type IN ('document', 'video', 'audio', 'voice', 'photo') LIMIT 250000")
  res = cur.fetchall()
  print("Deleting %d chat_files rows" % len(res))
  for r in res:
    cur.execute("DELETE FROM chat_files WHERE id=%s", r)

def pick_startid(cur, tablename, colname='id', rowcnt=10000000):
  cur.execute("SELECT MAX(" + colname + ") FROM " + tablename)
  maxid = cur.fetchone()[0]
  startid = random.randint(0, maxid-rowcnt) if maxid > rowcnt else 0;
  return startid, startid+rowcnt, maxid

@with_cursor
def check_file_ids(cur):
  startid, endid, maxid = pick_startid(cur, "file_ids")
  cur.execute("SELECT COUNT(*) FROM file_ids WHERE id BETWEEN %s AND %s", (startid, endid))
  cnt = cur.fetchone()[0]
  cur.execute("SELECT file_ids.id FROM file_ids LEFT JOIN chat_files USING (file_id) LEFT JOIN chat_sticker USING (file_id) WHERE file_ids.id BETWEEN %s AND %s AND chat_files.id IS NULL AND chat_sticker.id IS NULL", (startid, endid))
  res = cur.fetchall()
  print("Deleting %d/%d file_ids between %d and %d out of %d" % (len(res), cnt, startid, endid, maxid))
  for r in res:
    cur.execute("DELETE FROM file_ids WHERE id=%s", r)

@with_cursor
def purge_replies(cur):
  startid, endid, maxid = pick_startid(cur, "replies")
  cur.execute("SELECT COUNT(*) FROM replies WHERE id BETWEEN %s AND %s", (startid, endid))
  cnt = cur.fetchone()[0]
  cur.execute("SELECT id FROM chat INNER JOIN replies USING (id) WHERE id BETWEEN %s AND %s AND convid IN (SELECT convid FROM options2 WHERE purge_chat>0) LIMIT 150000", (startid, endid))
  res = cur.fetchall()
  print("Deleting %d/%d replies (%d-%d), id %d-%d/%d" % (len(res), cnt, res[0][0] if res else 0, res[-1][0] if res else 0, startid, endid, maxid))
  for r in res:
    cur.execute("DELETE FROM replies WHERE id=%s", r)

@with_cursor
def purge_stickers(cur):
  startid, endid, maxid = pick_startid(cur, "chat_sticker");
  cur.execute("SELECT COUNT(*) FROM chat_sticker WHERE id BETWEEN %s AND %s", (startid, endid))
  cnt = cur.fetchone()[0]
  cur.execute("SELECT id FROM chat INNER JOIN chat_sticker USING (id) WHERE id BETWEEN %s AND %s AND convid IN (SELECT convid FROM options2 WHERE purge_chat>0) LIMIT 30000", (startid, endid))
  res = cur.fetchall()
  print("Deleting %d/%d stickers (%d-%d) id %d-%d/%d" % (len(res), cnt, res[0][0] if res else 0, res[-1][0] if res else 0, startid, endid, maxid))
  for r in res:
    cur.execute("DELETE FROM chat_sticker WHERE id=%s", r)

@with_cursor
def purge_unique_messages(cur):
  startid, endid, maxid = pick_startid(cur, "chat")
  cur.execute("SELECT COUNT(*) FROM chat WHERE id BETWEEN %s AND %s", (startid, endid))
  cnt = cur.fetchone()[0]
  cur.execute("SELECT id, hash FROM chat LEFT JOIN chat_hashcounts ON (hash=UNHEX(SHA2(text,256))) WHERE id BETWEEN %s AND %s AND convid IN (SELECT convid FROM options2 WHERE purge_chat>0) AND LENGTH(text) > 16 AND count = 1"
              " AND id NOT IN (SELECT id FROM replies) AND id NOT IN (SELECT id FROM chat_sticker) AND id NOT IN (SELECT id FROM forwarded_from) LIMIT 100000", (startid, endid))
  res = cur.fetchall()
  print("Deleting %d/%d messages (%d-%d) id %d-%d/%d" % (len(res), cnt, res[0][0] if res else 0, res[-1][0] if res else 0, startid, endid, maxid))
  for r in res:
    cur.execute("DELETE FROM chat WHERE id=%s", (r[0],))
    assert(cur.rowcount == 1)
    cur.execute("DELETE FROM chat_hashcounts WHERE hash=%s AND count=1", (r[1],))
    assert(cur.rowcount == 1)
  return len(res)

@with_cursor
def purge_duplicate_messages(cur):
  startid, endid, maxid = pick_startid(cur, "chat", rowcnt=30000000)
  cur.execute("SELECT COUNT(*) FROM chat WHERE id BETWEEN %s AND %s", (startid, endid))
  cnt = cur.fetchone()[0]
  cur.execute("SELECT id, hash FROM chat LEFT JOIN chat_hashcounts ON (hash=UNHEX(SHA2(text,256))) WHERE id BETWEEN %s AND %s AND convid IN (SELECT convid FROM options2 WHERE purge_chat>0) AND LENGTH(text) > 16 AND count > 1"
              " AND id NOT IN (SELECT id FROM replies) AND id NOT IN (SELECT id FROM chat_sticker) AND id NOT IN (SELECT id FROM forwarded_from) AND hash IN (SELECT hash FROM bad_messages) AND message_id <> id LIMIT 100000", (startid, endid))
  res = cur.fetchall()
  print("Deleting %d/%d duplicate messages (%d-%d) id %d-%d/%d" % (len(res), cnt, res[0][0] if res else 0, res[-1][0] if res else 0, startid, endid, maxid))
  deleted = set()
  skipped = 0
  for r in res:
    if r[1] in deleted:
      skipped += 1
      continue
    deleted.add(r[1])
    cur.execute("DELETE FROM chat WHERE id=%s", (r[0],))
    assert(cur.rowcount == 1)
    cur.execute("UPDATE chat_hashcounts SET count = count - 1 WHERE hash=%s AND count>1", (r[1],))
    assert(cur.rowcount == 1)
  print("Deleted %d, skipped %d" % (len(deleted), skipped))
  return len(deleted)

check_files('photo', '.jpg')
check_files('voice', '.opus')

check_file_text()
check_chat_files()
check_file_ids()
purge_replies()
purge_stickers()

dltd = 0
while dltd < 200000:
  dltd += purge_unique_messages()
  try:
    dltd += purge_duplicate_messages()
  except OperationalError as e:
    if e.args[0] != 1213:
      raise
    print("Deadlocked.")
  print("Total deleted %d messages." % (dltd,))
