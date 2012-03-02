#/usr/bin/env python

__all__ = ['HistogramPlot']

import sys
import os
import re
import time
import itertools
import types
from decimal import *
import Gnuplot

from relation import *
from csvlike import *

class HistogramPlot:
  """
  Data and methods to plot.
  
  """
  def __init__(self, histogramG, width, title='', output='output.png'):
    """
    histogramG :: generator((representative :: Decimal, count :: int))
    width :: str
      Width of each bin [sec].
    title :: str
    output :: str
      Output file name.
    
    """
    assert(type(histogramG) is types.GeneratorType)
    assert(isinstance(titl, str) and Decimal(width) > 0.0)
    assert(isinstance(title, str))
    assert(isinstance(output, str))
    self.__histogramG = histogramG
    self.__width = width
    self.__title = title
    self.__output = output

  def plot(self):
    """
    Plot data.

    """
    plot = self.__toPlotData(self.__histogramG)
    self.__doPlot(plot, self.__title, self.__output, self.__width)

  def __toPlotData(self, histogramG):
    return Gnuplot.PlotItems.Data(list(histogramG), with_="boxes")

  def __doPlot(self, plot, title, output, width):
    """
    plot :: Gnuplot.PlotItems.Data
    title :: str
    output :: str
    width :: str
    return :: None
    
    """
    g = Gnuplot.Gnuplot(debug=1)
    g.title(title)
    g.xlabel('Response time [sec]')
    g.ylabel('Number of plots (width: %s)' % width)
    g('set key top left')
    g('set boxwidth %s' % width)
    g('set style fill solid 1.0')
    g('set terminal png')
    g('set logscale y')
    g('set xrange [0:%f]' % (Decimal(width) * 100))
    g('set yrange [1:*]')
    g('set output "%s"' % output)
    g.plot(plot)

