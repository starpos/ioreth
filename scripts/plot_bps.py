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

def doMain():

  if len(sys.argv) < 2:
    print "Usage %s [throughput.plot]" % sys.argv[0]
  throughputPlotPath = sys.argv[1]
  if len(sys.argv) >= 3:
    outputTemplate = sys.argv[2]
  else:
    outputTemplate = 'bps_%s_bs%s.png'

  patternMap = {'rnd':'random', 'seq':'sequential'}
  pairs = [(x, y) for x in ['rnd', 'seq'] for y in ['512', '4k', '32k', '256k']]

  ### Bps ###
  
  f = open(throughputPlotPath)
  c = CsvLike(f)
  
  titleTemplate = 'Throughput with pattern %s, blocksize %s'
  targetColumn = 'Bps'
  ylabel = 'Throughput [bytes/sec]'

  plotPerformanceData(c, titleTemplate, outputTemplate, targetColumn, ylabel, patternMap, pairs)
  f.close()

if __name__ == "__main__":
  doMain()
