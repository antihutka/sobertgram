import time
import os
import sys
import random

from configuration import Config
from database import with_cursor

if len(sys.argv) != 2:
  raise Exception("Wrong number of arguments")
Config.read(sys.argv[1])

curtime = time.time()
files = os.listdir('photo/')
random.shuffle(files)

print("%d photos found" % len(files))

def getcounts(cursor, fid):
  cursor.execute("SELECT SUM(IF(COALESCE(is_bad, 0) = 0, 1, 0)), SUM(IF(COALESCE(is_bad, 0) > 0, 1, 0)) FROM chat_files LEFT JOIN options2 USING (convid) WHERE file_id=%s", (fid,))
  r = cursor.fetchone()
  return r

@with_cursor
def check_files(cursor):
  proccnt = 0
  totalcnt = 0
  uniqidcnt = 0
  newcnt = 0
  goodcnt = 0
  delsize = 0
  for filename in files:
    totalcnt += 1
    fullname = 'photo/' + filename
    if not filename.endswith('.jpg'):
      continue
    fileid = filename[:-4]
    if len(fileid) < 54:
      uniqidcnt += 1
      continue
    mtime = os.path.getmtime(fullname)
    filesize = os.path.getsize(fullname)
    fileage = (curtime-mtime)/60/60/24
    assert fileage >= 0
    if fileage < 60:
      newcnt += 1
      continue
    good, bad = getcounts(cursor, fileid)
    #print('%s %s %s' % (fileid, good, bad))
    if (good is None) or (bad is None):
      print('File not in DB? %s age %.2f' % (fileid, fileage))
      continue
    if (good > 0):
      goodcnt += 1
      continue
    assert bad > 0

    proccnt += 1
    delsize += filesize
    print("[T: %6d U: %6d N: %6d G: %6d P:%6d %.2f MB] File %s %.1f kB %.2f days old good %d bad %d" % (totalcnt, uniqidcnt, newcnt, goodcnt, proccnt, delsize / 1024 / 1024, fileid, filesize / 1024, fileage, good, bad))
    print("Deleting %s" % fullname)
    os.remove(fullname)
    time.sleep(0.1)

check_files()
