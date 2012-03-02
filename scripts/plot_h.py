#!/usr/bin/env python

import sys
import os
import re
import time
from decimal import *
import itertools
import Gnuplot

import util
from histogram_plot import *

def doMain():

  widths = ['0.00001', '0.0001', '0.001'] # 10us, 100us, 1ms
  patternMap = {'rnd':'random', 'seq':'sequential'}
  params = [(pattern, mode, bsU)
            for (pattern, mode) in [('rnd', 'read'), ('rnd', 'write'), ('rnd', 'mix'),
                                    ('seq', 'read'), ('seq', 'write')]
            for bsU in ['512', '4k', '32k', '256k']]
  #params = [('rnd', 'read', '4k')]
  print params #debug

  def histogramG(f):
    for line in f:
      rec = line.rstrip().split()
      yield (Decimal(rec[0]), int(rec[1]))

  for pattern, mode, bsU in params:
    for width in widths:
      fn = './%s/%s/%s/histogram_%s' % (pattern, mode, str(util.u2s(bsU)), width)
      title = 'Histogram with %s %s, blocksize %s, width %s' % (patternMap[pattern], mode, bsU, width)
      output = '%s_%s_bs%s_w%s' % (pattern, mode, bsU, width)
      f = open(fn)
      hp = HistogramPlot(histogramG(f), width, title, output)
      hp.plot()
      f.close()
  
if __name__ == "__main__":
  doMain()
