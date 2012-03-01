#/usr/bin/env python

__all__ = ['PerformancePlot']

import sys
import os
import re
import time
import itertools
import Gnuplot

from relation import *
from csvlike import *

class PerformancePlot:
  """
  Data and methods to plot.

  Type definition.
    GroupedRelations :: [(mode :: (str,), plot :: Relation)]
  
  """
  def __init__(self, rel, pattern, blockSize, target, ylabel, title='', output='output.png'):
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
    
    """
    assert(isinstance(rel, Relation))
    assert(isinstance(pattern, str))
    assert(isinstance(blockSize, str) and int(blockSize) > 0)
    assert(isinstance(target, str))
    assert(isinstance(ylabel, str))
    assert(isinstance(title, str))
    assert(isinstance(output, str))
    self.__rel = rel
    self.__pattern = pattern
    self.__blockSize = blockSize
    self.__target = target
    self.__ylabel = ylabel
    self.__title = title
    self.__output = output

  def plot(self):
    """
    Plot data.

    """
    #print "relation size: ", self.__rel.size(), self.__pattern, self.__blockSize #debug
    groupedRelations = self.__filterAndGroupByMode(
      self.__rel, self.__pattern, self.__blockSize, self.__target)
    #print "groupedRelations: ", len(groupedRelations) #debug
    plotData = self.__toPlotData(groupedRelations)
    #print "plotData: ", plotData #debug
    self.__doPlot(plotData, self.__output, self.__title, self.__ylabel)

  def __filterAndGroupByMode(self, rel, pattern, blockSize, target):
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
      'target' :: Decimal
        Something to plot. (This is Y axis.)
    pattern :: str
    blockSize :: str(int)

    return :: GroupedRelations
      Schema of each relation: (nThreads, target)

    """
    k1 = (pattern, blockSize)
    def pred(rec):
      #print "rec: ", rec.show() #debug
      k2 = Key(rec, ['pattern', 'blockSize']).raw()
      #print "(k1,k2): ", (k1,k2) #debug
      return k1 == k2
    #print "rel: ", rel.show() #debug
    rel2 = rel.filter(pred)
    #print "rel2: ", rel2.show() #debug
    rel3 = rel2.groupbyAsRelation(['mode'], ['nThreads', target])
    #print "rel3: ", rel3 #debug
    return rel3

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

  def __doPlot(self, plotData, outputFileName, title, ylabel):
    """
    plotData :: [Gnuplot.PlotItems.Data]
    outputFileName :: str
    title :: str
    ylabel :: str
      Label of Y axis.
    return :: None
    
    """
    g = Gnuplot.Gnuplot(debug=1)
    g.title(title)
    g.xlabel('Number of threads')
    g.ylabel(ylabel)
    g('set key top left')
    g('set terminal png')
    g('set yrange[0:*]')
    g('set output "%s"' % outputFileName)
    g.plot(*plotData)

