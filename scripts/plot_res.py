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
    print "Usage %s [response.plot]" % sys.argv[0]
  responsePlotPath = sys.argv[1]
  if len(sys.argv) >= 3:
    outputTemplate = sys.argv[2]
  else:
    outputTemplate = 'res_%s_bs%s.png'

  patternMap = {'rnd':'random', 'seq':'sequential'}
  pairs = [(x, y) for x in ['rnd', 'seq'] for y in ['512', '4k', '32k', '256k']]

  ### response ###
  
  f = open(responsePlotPath)
  c = CsvLike(f)
  
  titleTemplate = 'response with pattern %s, blocksize %s'
  targetColumn = 'response'
  ylabel = 'Response time [sec]'

  plotPerformanceData(c, titleTemplate, outputTemplate, targetColumn, ylabel, patternMap, pairs)
  f.close()

if __name__ == "__main__":
  doMain()
