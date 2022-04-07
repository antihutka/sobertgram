import re

regex = r"\+$"
compiled = re.compile(regex)

def is_nonstandard_command(msg):
  return bool(compiled.match(msg))
