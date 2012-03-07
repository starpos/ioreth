#!/usr/bin/env python

"""


"""

import sys
import os

from param import *
from relation import *

def createTableStr1(headerX, itemMatrix):
    """
    Create mediawiki-formatted table with X header.
    
    headerX :: [str]
        len(headerX) is X length.
    itemMatrix :: [items]
        len(itemMatrix) is Y length.
    items :: [str]
        len(items) is X length.

    return :: str
        Mediawiki-formatted table.

    """

    def begin():
        return '{| class="wikitable" border="1" style="text-align:left"\n'

    def end():
        return '|}\n'

    def header(headerX):
        """
        headerX :: [str]

        """
        return '! ' + ' !! '.join(headerX) + '\n'

    def items(items):
        return '| ' + ' || '.join(items) + '\n'

    def sep():
        return '|-\n'

    assert(isinstance(headerX, list))
    assert(isinstance(itemMatrix, list))

    return begin() + header(headerX) + sep() \
        + sep().join(map(items, itemMatrix)) \
        + end()

def createTableStr2(headerX, headerY, itemMatrix):
    """
    Create mediawiki-formatted table with X header and and Y header.
    
    """
    
    def begin():
        return '{| class="wikitable" border="1" style="text-align:left"\n'

    def end():
        return '|}\n'

    def header(headerX):
        """
        headerX :: [str]

        """
        return '! !! ' + ' !! '.join(headerX) + '\n'

    def items((header, items)):
        return '! ' + header + ' || ' + ' || '.join(items) + '\n'

    def sep():
        return '|-\n'

    return begin() + header(headerX) + sep() \
        + sep().join(map(items, zip(headerY, itemMatrix))) \
        + end()


def renderStatistics(f, chartTypes, patterns, blockSizeUs, prefix=''):
    """
    f :: file
       file object to write rendered text.
    chartTypes :: [str]
        chart types like 'res', 'iops', 'bps'.
    patterns :: [str]
        pattern like 'rnd', 'seq'.
    blockSIzeUs :: [str]
        block size with unit like '512', '4k', '32k', '256k'.
    prefix :: str
        prefix of filepath.
    return :: None
    
    """
    #paramsStat = getParamsRelForStatistics()
    #for chartType, mode in groupByAsRelation(['chartType', 'mode']).iterkeys():
    paramsC = [(chartType, pattern)
              for chartType in chartTypes
              for pattern in patterns]
    paramsY = [bsU for bsU in blockSizeUs]

    def mapperC(paramC):
        d0 = {'res': 'Response', 'iops': 'IOPS', 'bps': 'Throughput'}
        d1 = {'rnd': 'random', 'seq': 'sequential'}
        chartType, pattern = paramC
        return '%s, %s' % (d0[chartType], d1[pattern])
        
    def mapperY(paramY):
        return "bs %s" % paramY
        
    for paramC in paramsC:
        chartType, pattern = paramC
        print >>f, "==== %s ====" % mapperC(paramC)
        matrix = []
        for bsU in paramsY:
            line = []
            line.append('[[%s%s_%s_bs%s.png|width=%dpx|height=%dpx]]' % (prefix, chartType, pattern, bsU, 640, 640 * 3 / 4))
            matrix.append(line)
        print >>f, createTableStr2([mapperC(paramC)], map(mapperY, paramsY), matrix)
        
    #print createTableStr2(map(mapperX, paramsX), map(mapperY, paramsY), matrix)


def renderHistogram(f, patternModes, blockSizeUs, nThreadss, widths, prefix=''):
    """
    f :: file
    patternModes :: [(pattern :: str, mode :: str)]
        pattern is like 'rnd', 'seq'.
        mode is like 'read', 'write', 'mix'.
    blockSizeUs :: [str]
        block size with unit like '512', '4k', '32k', '256k'.
    nThreadss :: [str]
        nthreads like '1', '4', '8'.
    widths :: [str]
        bins' width of response histogram in second like '0.01', '0.001'.
    prefix :: str
        prefix of filename.
    return :: None
        
    """

    # keyCols = ['chartType', 'pattern']
    # d = paramsStat.groupbyAsRelation(keyCols)
    # for key, rel in d.iteritems():
    #     for s in applyStringTemplate('%s_%s_bs%s.png', rel):
    #         print s
    #     print
    paramsY0 = patternModes
    paramsY1 = blockSizeUs
    paramsY2 = nThreadss
    paramsX0 = widths

    def mapperY0((pattern, mode)):
        d0 = {'rnd': 'random', 'seq': 'sequential'}
        return (d0[pattern], mode)
    
    paramsX = ["width %s" % width for width in paramsX0]
    # paramsY = ["%s %s, bs %s, nThreads %s" % (pattern, mode, bsU, nThreads)
    #            for (pattern, mode) in map(mapperY0, paramsY0)
    #            for bsU in paramsY1
    #            for nThreads in paramsY2]
    paramsY = ["bs %s, nThreads %s" % (bsU, nThreads)
               for bsU in paramsY1
               for nThreads in paramsY2]
    
    for pattern, mode in paramsY0:
        print >>f, "==== Response histogram with %s %s ====" % mapperY0((pattern, mode))
        matrix = []
        for bsU in paramsY1:
            for nThreads in paramsY2:
                line = []
                for width in paramsX0:
                    line.append('[[%shistogram_%s_%s_bs%s_th%s_w%s.png|width=%dpx|height=%dpx]]'
                                % (prefix, pattern, mode, bsU, nThreads, width, 240, 240 * 3 / 4))
                matrix.append(line)
        print >>f, createTableStr2(paramsX, paramsY, matrix)

    #print createTableStr2(paramsX, paramsY, matrix)
    #paramsHist = getParamsRelForHistogram()

def renderIorethReport(f, exprName, params, goals=None, settings=None, conclusion=None):
    """
    Render ioreth report in mediawiki format.
    
    exprName :: str
        Experiment name like '20120227a'
    params :: dict(paramName :: str, [str] | [(str, str)])
        Parameter data.
    goals :: str
        Goals text in mediawiki format.
    settings :: str
        Settings text in mediawiki format.
    conclusion ::str
        Conclusion text in mediawiki format.
    
    """
    print >>f, '== %s ==' % exprName
    print >>f, '== Goals =='

    if goals is not None:
        print >>f, goals
    
    print >>f, '== Settings =='
    if settings is not None:
        print >>f, settings

    print >>f, '== Results =='

    chartTypeList = params['chartTypeList']
    patternList = params['patternList']
    blockSizeUnitList = params['blockSizeUnitList']
    patternModeList = params['patternModeList']
    nThreadsList = params['nThreadsList']
    widthList = params['widthList']

    print >>f, '=== Performance Statistics ==='
    
    renderStatistics(f, chartTypeList, patternList,
                     blockSizeUnitList,
                     prefix='%s/charts/' % exprName)

    print >>f, '=== Response Histogram ==='

    renderHistogram(f, patternModeList, blockSizeUnitList,
                    nThreadsList, widthList,
                    prefix=('%s/histogram/' % exprName))
    
    print >>f, '== Conclusion =='
    if conclusion is not None:
        print >>f, conclusion

def getParams(fileName):
    """
    fileName :: str
        parameter file name.

    params :: dict(paramName :: str, [str] | [(str, str)])
        Parameter data.
    
    """
    f = open(fileName, 'r')
    s = ''
    for line in f:
        s += line
    f.close()
    params = eval(s)
    return params
        
def doMain():

    if len(sys.argv) == 1:
        print "Specify expreiment name and prepare parameter file."
        exit(1)

    paramName = sys.argv[1]
    paramFileName = '%s.param' % paramName
    wikiFileName = '%s.mediawiki' % paramName
        
    params = getParams(paramFileName)
    f = open(wikiFileName, 'w')
    renderIorethReport(f, paramName, params)
    f.close()

if __name__ == '__main__':
    doMain()
