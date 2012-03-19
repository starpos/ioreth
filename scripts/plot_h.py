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

  if len(sys.argv) < 3:
    print "Usage: %s [param file] [target directory] ([template])" % sys.argv[0]
    print r"template is like 'histogram_%s_%s_bs%s_th%s_w%s.png'"
    exit(1)
  paramFilePath = sys.argv[1]
  targetDirectory = sys.argv[2]
  if len(sys.argv) == 4:
    outputFileTemplate = sys.argv[3]
  else:
    outputFileTemplate = r'histogram_%s_%s_bs%s_th%s_w%s.png'

  patternMap = {'rnd':'random', 'seq':'sequential'}

  params = util.getParams(paramFilePath)
  blockSizeUnitList = params['blockSizeUnitList']
  patternModeList = params['patternModeList']
  nThreadsList = params['nThreadsList']
  widthList = params['widthList']

  patternModeBsUList = [(pattern, mode, bsU)
                        for (pattern, mode) in patternModeList
                        for bsU in blockSizeUnitList]
  
  def histogramG(f):
    for line in f:
      rec = line.rstrip().split()
      yield (Decimal(rec[0]), int(rec[1]))

  for pattern, mode, bsU in patternModeBsUList:
    for nth in nThreadsList:
      for width in widthList:
        fn = '%s/%s/%s/%s/%s/histogram_%s' % \
            (targetDirectory, pattern, mode, nth, str(util.u2s(bsU)), width)
        title = 'Histogram with %s %s, blocksize %s, nThreads %s, width %s' % \
            (patternMap[pattern], mode, bsU, nth, width)
        output = outputFileTemplate % (pattern, mode, bsU, nth, width)
        f = open(fn)
        hp = HistogramPlot(histogramG(f), width, title, output)
        hp.plot()
        f.close()
  
if __name__ == "__main__":
  doMain()
