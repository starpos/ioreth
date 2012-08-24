#/usr/bin/env python

__all__ = ['MultiPlot', 'plotMultiData']

from decimal import Decimal
import Gnuplot

import util

class MultiPlot:
  """
  Data and methods to multiple plot.

  Type definition.
    GroupedRelations :: [(mode :: (str,), plot :: Relation)]
  
  """
  def __init__(self, relColLegList, pattern, mode, blockSize,
               ylabel, title='', output='output.png',
               scale=Decimal(1.0), xRange='*:*', yRange='0:*', debug=False):
    """
    relColLegList :: [(rel, col, legend)]
    pattern :: str
    mode :: str
    blockSize :: str(int)
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
    assert(isinstance(relColLegList, list))
    assert(isinstance(pattern, str))
    assert(isinstance(mode, str))
    assert(isinstance(blockSize, str) and int(blockSize) > 0)
    assert(isinstance(ylabel, str))
    assert(isinstance(title, str))
    assert(isinstance(output, str))
    assert(isinstance(scale, Decimal))
    assert(isinstance(xRange, str))
    assert(isinstance(yRange, str))
    self.__relColLegList = relColLegList
    self.__pattern = pattern
    self.__mode = mode
    self.__blockSize = blockSize
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
    legRelList = []
    for rel, tCol, legend in self.__relColLegList:
      rel2 = self.__filter(rel, self.__pattern, self.__mode, self.__blockSize,
                           tCol, self.__scale)
      legRelList.append((legend, rel2))
    plotData = self.__toPlotData(legRelList)
    self.__doPlot(plotData, self.__output, self.__title, self.__ylabel,
                  self.__xrange, self.__yrange)

  def __filter(self, rel, pattern, mode, blockSize, target, scale=Decimal(1.0)):
    """
    Filter only required data for plot.

    rel :: Relation
      Schema ('pattern', 'mode', 'nThreads', 'blockSize', target)
      pattern :: "rnd" | "seq"
        rnd means random access pattern,
        seq menas sequential access pattern.
      mode :: 'read' | 'write' | 'mix'
        read/write. mix means half-and-half.
      nThreads :: int
        Number of threads. (This is X axis.)
      blockSize :: int
        Block size [bytes].
      target :: Decimal
        Target data. (This is Y axis.)

    pattern :: str
    blockSize :: str(int)
    target :: str
        Target column name.
    scale :: Decimal
      Scale of the Y data.
      
    return :: Relation
      Schema: ('nThreads', target)
    
    """
    k1 = (pattern, mode, blockSize)
    def pred(rec):
      k2 = rec.getKey(['pattern', 'mode', 'blockSize']).raw()
      return k1 == k2
    rel2 = rel.filter(pred)
    def mapper((pattern, mode, nThreads, blockSize, target)):
      return (pattern, mode, nThreads, blockSize, str(Decimal(target) * Decimal(scale)))
    cols = ['pattern', 'mode', 'nThreads', 'blockSize', target]
    rel3 = rel2.map(cols, cols, mapper)
    rel4 = rel3.project(['nThreads', target])
    return rel4

  def __toPlotData(self, legRelList):
    """
    regRelList :: [legend :: str, Relation]
    return :: Gnuplot.PlotItems.Data

    """
    plots = []
    for legend, rel in legRelList:
      plots.append(
        Gnuplot.PlotItems.Data(rel.getL(), with_="lp", title=legend))
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
    g('set key bottom right')
    g('set terminal png')
    g('set xrange [%s]' % xRange)
    g('set yrange [%s]' % yRange)
    g('set output "%s"' % outputFileName)
    g.plot(*plotData)


def plotMultiData(relColLegList, titleTemplate, outputTemplate,
                  yLabel, patternMap,
                  params, scale=Decimal(1.0), xRange='*:*', debug=False):
  """
  relColLegList :: [(rel, col, legend)]
    rel :: Relation
    col :: str
      target column name
    legend :: str
      legend title in the plot.
    Each legend must be unique in the list.
  
  params :: [(pattern :: str, mode :: str, bsU :: str, yRange :: str|None)]
      list of parameters.
  
  """
  assert(isinstance(relColLegList, list))
  assert(isinstance(titleTemplate, str))
  assert(isinstance(outputTemplate, str))
  assert(isinstance(yLabel, str))
  assert(isinstance(patternMap, dict))
  assert(isinstance(params, list))
  assert(isinstance(scale, Decimal))
  assert(isinstance(xRange, str))
  assert(isinstance(debug, bool))

  for pattern, mode, bsU, yRange in params:
    title = titleTemplate % (patternMap[pattern], mode, bsU)
    output = outputTemplate % (pattern, mode, bsU)
    bsS = str(util.u2s(bsU))
    if yRange is None:
      yRange = '0:*'
    rp = MultiPlot(relColLegList, pattern, mode, bsS,
                   yLabel, title, output,
                   scale, xRange, yRange, debug)
    rp.plot()
  
  pass
