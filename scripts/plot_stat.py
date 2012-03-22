#!/usr/bin/env python

import sys
import os
import re
import time
import itertools
from decimal import Decimal
import Gnuplot

import param
from relation import *
from csvlike import *
from performance_plot import *

def main():

  if len(sys.argv) < 2:
    print "Usage %s [param file]" % sys.argv[0]
    exit(1)

  rawParams = param.getParams(sys.argv[1])
  try:
    params = param.ParamsForStatPlot(rawParams)
  except param.ParamsException, msg:
    print msg
    exit(1)

  plotFileList = params.plotFileList()
  outDirList = params.outDirList()
  outFileTemplate = params.outFileTemplate()
  outFileParams = params.outFileParams()
  titleTemplate = params.titleTemplate()
  targetColumn = params.targetColumn()
  yLabel = params.yLabel()
  scale = params.scale()
  patternMap = params.patternMap({'rnd':'random', 'seq':'sequential'})
  
  for outDir, plotFile in zip(outDirList, plotFileList):
    if not os.path.exists(outDir):
      os.makedirs(outDir)
    outFilePathTemplate = outDir + '/' + outFileTemplate
    if not os.path.exists(plotFile):
      print "File %s does not exist." % plotFile
      continue
    
    f = open(plotFile)
    c = CsvLike(f)

    plotPerformanceData(c, titleTemplate, outFilePathTemplate,
                        targetColumn, yLabel, patternMap,
                        outFileParams, scale=scale, xRange='*:*', debug=False)
    f.close()

if __name__ == "__main__":
  main()
