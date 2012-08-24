#!/usr/bin/env python

'''
Plot performance data.

Each plot is (pattern, mode, bs).
Graph legends are 

'''

import sys
import os

import param
from csvlike import CsvLike
from performance_multi import plotMultiData

def main():

  if len(sys.argv) < 2:
    print "Usage %s [param file]" % sys.argv[0]
    exit(1)

  rawParams = param.getParams(sys.argv[1])
  try:
    params = param.ParamsForMultiPlot(rawParams)
  except param.ParamsException, msg:
    print msg
    exit(1)

  plotFileList = params.plotFileList()
  legendList = params.legendList()
  outDir = params.outDir()
  outFileTemplate = params.outFileTemplate()
  outFileParams = params.outFileParams()
  titleTemplate = params.titleTemplate()
  targetColumn = params.targetColumn()
  xRange = params.xRange('*:*')
  yLabel = params.yLabel()
  scale = params.scale()
  patternMap = params.patternMap({'rnd':'random', 'seq':'sequential'})

  fList = []
  relColLegList = []
  for plotFile, legend in zip(plotFileList, legendList):
      if not os.path.exists(plotFile):
          print 'File %s does not exist.' % plotFile
          exit(1)
      f = open(plotFile)
      fList.append(f)
      c = CsvLike(f)
      # do not close 'f' because CsvLike will read contents in lazy manner.
      relColLegList.append((c, targetColumn, legend))

  if not os.path.exists(outDir):
      os.makedirs(outDir)
  outFilePathTemplate = outDir + '/' + outFileTemplate

  plotMultiData(relColLegList, titleTemplate, outFilePathTemplate,
                yLabel, patternMap,
                outFileParams, scale=scale, xRange=xRange, debug=False)
  
  for f in fList:
      f.close()
      
if __name__ == "__main__":
  main()
