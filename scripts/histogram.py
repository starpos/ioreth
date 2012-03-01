#!/usr/bin/env python

import sys
from decimal import Decimal
import itertools

def getHistogram(width, strGenerator):
  """
  width :: Decimal
  strGenerator :: generator(str)
  return :: generator((representative :: Decimal,count :: int))
  
  """
  def mapper(x):
    return Decimal(int(Decimal(x) / width)) * width
  vals = itertools.imap(mapper, strGenerator)
  def countGenerator(g):
    c = 0
    for a in g:
      c += 1
    return c
  for k,g in itertools.groupby(sorted(vals)):
    yield (k, countGenerator(g))


def doMain():
  if len(sys.argv) != 2 or sys.argv[1] == '-h':
    print "Usage: %s [width] < [a value in a line]" % sys.argv[0]
    sys.exit(1)
  width = Decimal(sys.argv[1])
  for v,c in getHistogram(width, sys.stdin):
    print v,c

if __name__ == "__main__":
  doMain()
