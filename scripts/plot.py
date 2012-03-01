#!/usr/bin/env python

import sys
import os
import re
import time
import itertools
import Gnuplot

import util
from relation import *
from csvlike import *
from performance_plot import *


def plotPerformanceData(csvLike, titleTemplate, outputTemplate, targetColumn, ylabel,
                        patternMap, keyPairs):
  """
  """
  assert(isinstance(csvLike, CsvLike))
  assert(isinstance(titleTemplate, str))
  assert(isinstance(outputTemplate, str))
  assert(isinstance(targetColumn, str))
  assert(isinstance(ylabel, str))
  assert(isinstance(patternMap, dict))
  assert(isinstance(keyPairs, list))
  
  for pattern, blockSizeU in keyPairs:
    title = titleTemplate % (patternMap[pattern], blockSizeU)
    output = outputTemplate % (pattern, blockSizeU)
    blockSizeS = str(util.u2s(blockSizeU))
    rp = PerformancePlot(csvLike, pattern, blockSizeS, targetColumn, ylabel, title, output)
    rp.plot()

def doMain():

  patternMap = {'rnd':'random', 'seq':'sequential'}
  pairs = [(x, y) for x in ['rnd', 'seq'] for y in ['512', '4k', '32k', '256k']]
  #pairs = [('rnd', '4k')]
  print pairs #debug

  ### response ###
  
  f = open('response.plot')
  c = CsvLike(f)
  
  titleTemplate = 'response with pattern %s, blocksize %s'
  outputTemplate = 'res_%s_bs%s.png'
  targetColumn = 'response'
  ylabel = 'Response time [sec]'

  plotPerformanceData(c, titleTemplate, outputTemplate, targetColumn, ylabel, patternMap, pairs)
  f.close()

  ### Bps ###
  
  f = open('throughput.plot')
  c = CsvLike(f)
  
  titleTemplate = 'Throughput with pattern %s, blocksize %s'
  outputTemplate = 'bps_%s_bs%s.png'
  targetColumn = 'Bps'
  ylabel = 'Throughput [bytes/sec]'

  plotPerformanceData(c, titleTemplate, outputTemplate, targetColumn, ylabel, patternMap, pairs)
  f.close()

  ### iops ###
  
  f = open('throughput.plot')
  c = CsvLike(f)
  
  titleTemplate = 'IOPS with pattern %s, blocksize %s'
  outputTemplate = 'iops_%s_bs%s.png'
  targetColumn = 'iops'
  ylabel = 'IOPS'

  plotPerformanceData(c, titleTemplate, outputTemplate, targetColumn, ylabel, patternMap, pairs)
  f.close()
  
if __name__ == "__main__":
  doMain()
