#!/usr/bin/env python

__all__ = ['getHistogram']

import sys
import types
from decimal import Decimal
import itertools

def getHistogram(width, f):
  """
  Get histogram data.
  
  width :: Decimal
  f :: generator(str) | file
  return :: generator((representative :: Decimal,count :: int))
  
  """
  assert(isinstance(width, Decimal))
  assert(type(f) is types.GeneratorType or isinstance(f, file))
  
  def rounder(x):
    assert(type(x) is types.StringType)
    return Decimal(int(Decimal(x) / width)) * width
  vals = itertools.imap(rounder, f)

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
  for representative, count in getHistogram(width, sys.stdin):
    print representative, count

if __name__ == "__main__":
  doMain()
