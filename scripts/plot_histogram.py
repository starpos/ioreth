#!/usr/bin/env python

import sys
import os
import re
import time
from decimal import *
import itertools
import Gnuplot

import util
import param
from histogram_plot import *

def doMain():

  if len(sys.argv) == 1:
    print "Usage: %s [param file]" % sys.argv[0]
    exit(1)

  paramFilePath = sys.argv[1]
  rawParams = param.getParams(paramFilePath)
  try:
    params = param.ParamsForHistogramPlot(rawParams)
  except param.ParamsException, msg:
    print msg
    exit(1)
  
  plotDirList = params.plotDirList()
  outDirList = params.outDirList()
  assert(len(plotDirList) == len(outDirList))
  
  patternMap = params.patternMap({'rnd':'random', 'seq':'sequential'})
  outFileTemplate = params.outFileTemplate(r'histogram_%s_%s_bs%s_th%s_w%s.png')
  blockSizeUnitList = params.blockSizeUnitList()
  patternModeList = params.patternModeList()
  nThreadsList = params.nThreadsList()
  widthList = params.widthList()

  paramsList = [(pattern, mode, bsU, nth, width)
                for (pattern, mode) in patternModeList
                for bsU in blockSizeUnitList
                for nth in nThreadsList
                for width in widthList]
  
  def histogramG(f):
    for line in f:
      rec = line.rstrip().split()
      yield (Decimal(rec[0]), int(rec[1]))
  
  for plotDir, outDir in zip(plotDirList, outDirList):

    if not os.path.exists(outDir):
      os.makedirs(outDir)
    
    for pattern, mode, bsU, nth, width in paramsList:
      fn = '%s/%s/%s/%s/%s/histogram_%s' % \
          (plotDir, pattern, mode, nth, str(util.u2s(bsU)), width)
      title = 'Histogram with %s %s, blocksize %s, nThreads %s, width %s' % \
          (patternMap[pattern], mode, bsU, nth, width)
      outputFile = outFileTemplate % (pattern, mode, bsU, nth, width)
      output = "%s/%s" % (outDir, outputFile)
      f = open(fn)
      hp = HistogramPlot(histogramG(f), width, title, output, debug=False)
      hp.plot()
      f.close()
  
if __name__ == "__main__":
  doMain()
