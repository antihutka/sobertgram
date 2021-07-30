import time
import os
import sys
import random

from configuration import Config
from database import with_cursor

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
    assert fileage >= 0
    if fileage < minage:
      newcnt += 1
      continue
    if len(fileid) < 28:
      uniqidcnt += 1
      good, bad = getcounts_u(cursor, fileid)
      lastdate = getage_u(cursor, fileid)
      isuniq = True
    else:
      good, bad = getcounts(cursor, fileid)
      lastdate = getage(cursor, fileid)
      isuniq = False
    if (good is None) or (bad is None):
      print('File not in DB? %s age %.2f' % (fileid, fileage))
      continue
    lastage = (curtime-lastdate)/60/60/24
    assert lastage>0
    if lastage < minage:
      newcnt += 1
      continue
    #print('%s %s %s' % (fileid, good, bad))
    if (good > 0) and (filesize > minsize):
      goodcnt += 1
      continue
    assert bad > 0 or filesize <= minsize

    proccnt += 1
    delsize += filesize
    print("[T: %6d U: %6d N: %6d G: %6d P:%6d %.2f MB] File %s %.1f kB %.2f/%.2f days old good %d bad %d" % (totalcnt, uniqidcnt, newcnt, goodcnt, proccnt, delsize / 1024 / 1024, fileid, filesize / 1024, fileage, lastage, good, bad))
    print("Deleting %s" % fullname)
    if isuniq:
      delete_dbentry(fileid)
    os.remove(fullname)
    time.sleep(0.1)

check_files('photo', '.jpg')
check_files('voice', '.opus')

