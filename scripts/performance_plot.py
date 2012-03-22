#/usr/bin/env python

__all__ = ['PerformancePlot', 'plotPerformanceData']

import sys
import os
import re
import time
import itertools
from decimal import Decimal
import Gnuplot

from relation import *
from csvlike import *
import util

class PerformancePlot:
  """
  Data and methods to plot.

  Type definition.
    GroupedRelations :: [(mode :: (str,), plot :: Relation)]
  
  """
  def __init__(self, rel, pattern, blockSize, target, ylabel, title='', output='output.png',
               scale=Decimal(1.0), xRange='*:*', yRange='0:*', debug=False):
    """
    rel :: Relation
    pattern :: str
    blockSize :: str(int)
    target :: str
      Target column name like 'response', 'Bps', or 'iops'.
    ylabel :: str
      Label of Y axis.
    title :: str
    output :: str
      Output file name.
    scale :: Decimal
      Scale of the plot data.
    xRange :: str
      xrange.
    yRange :: str
      yrange.
    debug :: bool
      debug print if True.
    
    """
    assert(isinstance(rel, Relation))
    assert(isinstance(pattern, str))
    assert(isinstance(blockSize, str) and int(blockSize) > 0)
    assert(isinstance(target, str))
    assert(isinstance(ylabel, str))
    assert(isinstance(title, str))
    assert(isinstance(output, str))
    assert(isinstance(scale, Decimal))
    assert(isinstance(xRange, str))
    assert(isinstance(yRange, str))
    self.__rel = rel
    self.__pattern = pattern
    self.__blockSize = blockSize
    self.__target = target
    self.__ylabel = ylabel
    self.__title = title
    self.__output = output
    self.__scale = scale
    self.__xrange = xRange
    self.__yrange = yRange
    self.__debug = debug

  def plot(self):
    """
    Plot data.

    """
    #print "relation size: ", self.__rel.size(), self.__pattern, self.__blockSize #debug
    groupedRelations = self.__filterAndGroupByMode(
      self.__rel, self.__pattern, self.__blockSize, self.__target, self.__scale)
    #print "groupedRelations: ", len(groupedRelations) #debug
    plotData = self.__toPlotData(groupedRelations)
    #print "plotData: ", plotData #debug
    self.__doPlot(plotData, self.__output, self.__title, self.__ylabel,
                  self.__xrange, self.__yrange)

  def __filterAndGroupByMode(self, rel, pattern, blockSize, target, scale=Decimal(1.0)):
    """
    Get plot data for 'number of threads' vs 'response time'.

    rel :: Relation
      Schema (pattern, mode, nThreads, blockSize, response).
      pattern :: "rnd" | "seq"
        rnd means random access pattern,
        seq menas sequential access pattern.
      nThreads :: int
        Number of threads. (This is X axis.)
      blockSize :: int
        Block size [bytes].
      'target' :: str
        Column name of data. (This is Y axis.)
    pattern :: str
    blockSize :: str(int)
    target :: str
        Target column name.
    scale :: Decimal
      Scale of the Y data.

    return :: GroupedRelations
      Schema of each relation: (nThreads, target)

    """
    k1 = (pattern, blockSize)
    def pred(rec):
      #print "rec: ", rec.show() #debug
      k2 = rec.getKey(['pattern', 'blockSize']).raw()
      #print "(k1,k2): ", (k1,k2) #debug
      return k1 == k2
    #print "rel: ", rel.show() #debug
    rel2 = rel.filter(pred)
    #print "rel2: ", rel2.show() #debug
    def mapper((pattern, mode, nThreads, blockSize, target)):
      return (pattern, mode, nThreads, blockSize, str(Decimal(target) * Decimal(scale)))
    cols = ['pattern', 'mode', 'nThreads', 'blockSize', target]
    rel3 = rel2.map(cols, cols, mapper)
    #print "rel3: ", rel3.show() #debug
    rel4 = rel3.groupbyAsRelation(['mode'], ['nThreads', target])
    #print "rel4: ", rel4 #debug
    return rel4

  def __toPlotData(self, groupedRelation):
    """
    gorupedRelation :: dict(rawKey :: tuple([str]), Relation)
    return :: Gnuplot.PlotItems.Data

    """
    #print "len(groupedRelation): ", len(groupedRelation)
    
    plots = []
    for rawKey, rel in groupedRelation.iteritems():
      plots.append(
        Gnuplot.PlotItems.Data(rel.getL(), with_="lp", title=','.join(rawKey)))
    return plots

  def __doPlot(self, plotData, outputFileName, title, ylabel,
               xRange='*:*', yRange='0:*'):
    """
    plotData :: [Gnuplot.PlotItems.Data]
    outputFileName :: str
    title :: str
    ylabel :: str
      Label of Y axis.
    xRange :: str
    yRange :: yRange
    return :: None
    
    """
    print "Plot to %s ..." % outputFileName
    if self.__debug:
      g = Gnuplot.Gnuplot(debug=1)
    else:
      g = Gnuplot.Gnuplot()
    g.title(title)
    g.xlabel('Number of threads')
    g.ylabel(ylabel)
    g('set key top left')
    g('set terminal png')
    g('set xrange [%s]' % xRange)
    g('set yrange [%s]' % yRange)
    g('set output "%s"' % outputFileName)
    g.plot(*plotData)


def plotPerformanceData(csvLike, titleTemplate, outputTemplate, targetColumn, yLabel,
                        patternMap, params, scale=Decimal(1.0), xRange='*:*', debug=False):
                        
  """
  params :: [(pattern :: str, bsU :: str, yRange :: str|None)]
      list of parameters.
  
  """
  assert(isinstance(csvLike, CsvLike))
  assert(isinstance(titleTemplate, str))
  assert(isinstance(outputTemplate, str))
  assert(isinstance(targetColumn, str))
  assert(isinstance(yLabel, str))
  assert(isinstance(patternMap, dict))
  assert(isinstance(params, list))
  assert(isinstance(scale, Decimal))
  assert(isinstance(xRange, str))
  assert(isinstance(debug, bool))
  
  for pattern, blockSizeU, yRange in params:
    title = titleTemplate % (patternMap[pattern], blockSizeU)
    output = outputTemplate % (pattern, blockSizeU)
    blockSizeS = str(util.u2s(blockSizeU))
    if yRange is None:
      yRange = '0:*'
    rp = PerformancePlot(csvLike, pattern, blockSizeS,
                         targetColumn, yLabel, title, output,
                         scale, xRange, yRange, debug)
    rp.plot()
