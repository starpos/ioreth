#!/usr/bin/env python

"""
Make report template with mediawiki format from a parameter file.

"""

import sys
import os

from relation import *
from util import *
from param import *

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


def renderStatistics(f, chartTypes, patterns, blockSizeUs, names=['Default'], prefixes=[''],
                     template='%s_%s_bs%s.png', widthPx=640, heightPx=(640 * 3 / 4)):
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
    template :: str
        chart file name template with placeholders
        for chatType, pattern, and blockSizeUnit.
    widthPx :: int
        chart width in pixel in the table.
    heightPx :: int
        chart height in pixel in the table.
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
                fileName = template % (chartType, pattern, bsU)
                item = '[[%s%s|width=%dpx|height=%dpx]]' % \
                    (prefix, fileName, widthPx, heightPx)
                line.append(item)
                            
            matrix.append(line)
        print >>f, createTableStr2(names, map(mapperY, paramsY), matrix)
        
    #print createTableStr2(map(mapperX, paramsX), map(mapperY, paramsY), matrix)


def renderHistogram(f, patternModeList, bsUList, nThreadsList, widthList,
                    prefix='', template='histogram_%s_%s_bs%s_th%s_w%s.png',
                    widthPx=240, heightPx=(240 * 3 / 4)):
    """
    f :: file
    patternModeList :: [(pattern :: str, mode :: str)]
        pattern is like 'rnd', 'seq'.
        mode is like 'read', 'write', 'mix'.
    bsUList :: [str]
        block size with unit like '512', '4k', '32k', '256k'.
    nThreadsList :: [int]
        nthreads like 1, 4, 8.
    widthList :: [str]
        bins' width of response histogram in second like '0.01', '0.001'.
    prefix :: str
        prefix of filename.
    template :: str
        chart file name tempalte with placeholders for
        pattern, mode, bsU, nThreads, and width.
    widthPx :: int
        chart width in pixel in the table.
    heightPx :: int
        chart height in pixel in the table.
    return :: None
        
    """

    # keyCols = ['chartType', 'pattern']
    # d = paramsStat.groupbyAsRelation(keyCols)
    # for key, rel in d.iteritems():
    #     for s in applyStringTemplate('%s_%s_bs%s.png', rel):
    #         print s
    #     print
    paramsY0 = patternModeList
    paramsY1 = bsUList
    paramsY2 = map(str, nThreadsList)
    paramsX0 = widthList

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
                    fileName = template % \
                        (pattern, mode, bsU, nThreads, width)
                    line.append('[[%s%s|width=%dpx|height=%dpx]]'
                                % (prefix, fileName, widthPx, heightPx))
                matrix.append(line)
        print >>f, createTableStr2(paramsX, paramsY, matrix)

    #print createTableStr2(paramsX, paramsY, matrix)
    #paramsHist = getParamsRelForHistogram()

def renderIorethReport(f, params, goals=None, settings=None, conclusion=None):
    """
    Render ioreth report in mediawiki format.
    
    params :: Params
        Parameter data.
    goals :: str
        Goals text in mediawiki format.
    settings :: str
        Settings text in mediawiki format.
    conclusion ::str
        Conclusion text in mediawiki format.
    
    """
    titleName = params.titleName()
    blockSizeUnitList = params.blockSizeUnitList()

    print >>f, '== %s ==' % titleName
    print >>f, '== Goals =='

    if goals is not None:
        print >>f, goals
    
    print >>f, '== Settings =='
    if settings is not None:
        print >>f, settings

    print >>f, '== Results =='

    if params.isPlotStat():
        statNameList = params.statNameList()
        statDirList = params.statDirList()
        chartTypeList = params.chartTypeList()
        patternList = params.patternList()
        widthPx = params.statWidthPx(default=640)
        heightPx = params.statHeightPx(default=widthPx * 3 / 4)
        sTemplate = params.statChartFileTemplate(
            default='%s_%s_bs%s.png')
        print >>f, '=== Performance Statistics ==='
        renderStatistics(f, chartTypeList, patternList,
                         blockSizeUnitList,
                         names=statNameList,
                         prefixes=map(lambda x: '%s/' % x, statDirList),
                         template=sTemplate,
                         widthPx=widthPx, heightPx=heightPx)
    
    if params.isPlotHistogram():
        histogramDir = params.histogramDir()
        hTemplate = params.histogramChartFileTemplate(
            default='histogram_%s_%s_bs%s_th%s_w%s.png')
        patternModeList = params.patternModeList()
        nThreadsList = params.nThreadsList()
        widthList = params.widthList()
        widthPx = params.histogramWidthPx(default=240)
        heightPx = params.histogramHeightPx(default=widthPx * 3 / 4)
        print >>f, '=== Response Histogram ==='
        renderHistogram(f, patternModeList, blockSizeUnitList,
                        nThreadsList, widthList,
                        prefix='%s/' % histogramDir,
                        template=hTemplate,
                        widthPx=widthPx, heightPx=heightPx)
    
    print >>f, '== Conclusion =='
    if conclusion is not None:
        print >>f, conclusion

def doMain():

    if len(sys.argv) == 1:
        print "Usage %s [param file]" % sys.argv[0]
        exit(1)

    paramFileName = sys.argv[1]
    rawParams = getParams(paramFileName)
    try:
        params = ParamsForReport(rawParams)
    except ParamsException, msg:
        print msg
        exit(1)
    
    wikiFileName = params.titleName() + '.mediawiki'
    f = open(wikiFileName, 'w')
    renderIorethReport(f, params)
    f.close()

if __name__ == '__main__':
    doMain()
