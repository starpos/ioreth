#!/usr/bin/env python

"""
Make report template with mediawiki format from a parameter file.

"""

import sys
import os

from relation import *
from util import *

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


def renderStatistics(f, chartTypes, patterns, blockSizeUs, names=['Default'], prefixes=[''], widthPx=640, heightPx=(640 * 3 / 4)):
    """
    f :: file
       file object to write rendered text.
    chartTypes :: [str]
        chart types like 'res', 'iops', 'bps'.
    patterns :: [str]
        pattern like 'rnd', 'seq'.
    blockSizeUs :: [str]
        block size with unit like '512', '4k', '32k', '256k'.
    names :: [str]
        list of name.
    prefixes :: [str]
        list of prefix of filepath
    return :: None

    len(names) == len(prefixes) is required.
    
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
            for prefix in prefixes:
                line.append('[[%s%s_%s_bs%s.png|width=%dpx|height=%dpx]]' % \
                                (prefix, chartType, pattern, bsU, widthPx, heightPx))
            matrix.append(line)
        print >>f, createTableStr2(names, map(mapperY, paramsY), matrix)
        
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

def renderIorethReport(f, params, goals=None, settings=None, conclusion=None):
    """
    Render ioreth report in mediawiki format.
    
    params :: dict(paramName :: str, [str] | [(str, str)])
        Parameter data.
    goals :: str
        Goals text in mediawiki format.
    settings :: str
        Settings text in mediawiki format.
    conclusion ::str
        Conclusion text in mediawiki format.
    
    """
    exprName = params['exprName']
    nameList = params['nameList']
    statisticsDirList = params['statisticsDirectories']
    if 'histogramDirectory' in params:
        histogramDir = params['histogramDirectory']
    else:
        histogramDir = None

    if 'widthPx' in params:
        widthPx = int(params['widthPx'])
    else:
        widthPx = 640
    if 'heightPx' in params:
        heightPx = int(params['heightPx'])
    else:
        heightPx = widthPx * 3 / 4
        
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
                     names=nameList,
                     prefixes=map(lambda x: '%s/' % x, statisticsDirList),
                     widthPx=widthPx, heightPx=heightPx)

    if histogramDir is not None:
        print >>f, '=== Response Histogram ==='
        renderHistogram(f, patternModeList, blockSizeUnitList,
                        nThreadsList, widthList,
                        prefix='%s/' % histogramDir)
    
    print >>f, '== Conclusion =='
    if conclusion is not None:
        print >>f, conclusion

def doMain():

    if len(sys.argv) == 1:
        print "Usage %s [param file]" % sys.argv[0]
        exit(1)

    paramFileName = sys.argv[1]
    params = getParams(paramFileName)
    wikiFileName = params['exprName'] + '.mediawiki'
    f = open(wikiFileName, 'w')
    renderIorethReport(f, params)
    f.close()

if __name__ == '__main__':
    doMain()
