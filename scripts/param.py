#!/usr/bin/env python

"""
Make report template with mediawiki format from a parameter file.

"""

__all__ = ['getParams', 'ParamsForStatPlot', 'ParamsForHistogramPlot',
           'ParamsForExpr', 'ParamsForReport', 'ParamsException']

import re
from decimal import Decimal

import util

def getParams(fileName):
    """
    fileName :: str
        parameter file name.

    params :: dict(paramName :: str, any)
        Parameter data.
    
    """
    f = open(fileName, 'r')
    s = ''
    for line in f:
        s += line
    f.close()
    params = eval(s)
    return params

class ParamsException(Exception):
    pass

def checkAndThrow(predicate, msg="error"):
    if not predicate:
        raise ParamsException, msg

class Params:
    """
    Parameters for experiments.
    
    """

    def __init__(self, params):
        self.__params = params
        self.checkParams()

    def checkParamExist(self, name):
        checkAndThrow(name in self.__params, "%s is not defined." % name)

    def checkParam(self, name, typeId, maybe=False):
        def checkParamStrict(name, typeId):
          self.checkParamExist(name)
          checkAndThrow(isinstance(self.__params[name], typeId),
                        "%s is not the type %s." % (name, str(typeId)))
        if maybe:
            if name in self.__params:
                checkParamStrict(name, typeId)
        else:
            checkParamStrict(name, typeId)

    def checkParamMaybe(self, name, typeId):
        self.checkParam(name, typeId, maybe=True)

    def checkParamList(self, name, typeId, canBeNone=False):
        self.checkParamExist(name)
        checkAndThrow(isinstance(self.__params[name], list),
                      "%s is not list." % name)
        for x in self.__params[name]:
            if (not canBeNone) or (x is not None):
                checkAndThrow(isinstance(x, typeId),
                              "An item in %s is not the type %s." % (name, str(typeId)))
        
    def checkParams(self):
        checkAndThrow(isinstance(self.__params, dict),
                      "params is not dictionary.")
        for key in self.__params.iterkeys():
            checkAndThrow(isinstance(key, str),
                          "The key %s is not string." % str(key))

    def getParams(self):
        return self.__params
            
    def get(self, key):
        return self.__params[key]

    def getMaybe(self, key, default=None):
        if key in self.__params:
            return self.__params[key]
        else:
            return default

class ParamsChecker(Params):
    """
    Parameters with cheker methods.
    
    """

    def __init__(self, params):
        Params.__init__(self, params)
        self.checkParams()
        
    def checkParams(self):
        Params.checkParams(self)

    def nThreadsListCheck(self):
        self.checkParamList('nThreadsList', int)
        for nThreads in self.nThreadsList():
            checkAndThrow(nThreads > 0, "Satisfy nThreads > 0")

    def patternModeListCheck(self):
        self.checkParamList('patternModeList', tuple)
        for pattern, mode in self.patternModeList():
            checkAndThrow(isinstance(pattern, str), "pattern is not str.")
            checkAndThrow(pattern == 'seq' or pattern == 'rnd', "pattern must be seq or rnd.")
            checkAndThrow(isinstance(mode, str),"mode is not str.")
            checkAndThrow(mode == 'read' or mode == 'write' or mode == 'mix',
                          "mode must be read or write or mix.")

    def blockSizeUnitListCheck(self):
        self.checkParamList('blockSizeUnitList', str)
        for bsU in self.blockSizeUnitList():
            bs = util.u2s(bsU)
            checkAndThrow(isinstance(bs, int), 'bs must be int.')
            checkAndThrow(bs > 0, 'bs must > 0.')
    

class ParamsForStatPlot(ParamsChecker):
    """
    Parameters for statistics plotting.
    
    """
    scaleReStr = r'([0-9]+)(?:/([0-9]+))?'
    
    def __init__(self, params):
        ParamsChecker.__init__(self, params)
        self.checkParams()

    def checkParams(self):
        ParamsChecker.checkParams(self)
        self.checkParamList('plotFileList', str)
        self.checkParamList('outDirList', str)
        l1 = len(self.plotFileList())
        l2 = len(self.outDirList())
        checkAndThrow(l1 == l2,
                      "length of plotFileList and outDirList is different.")
        self.checkParamMaybe('outFileTemplate', str)
        self.checkParamList('outFileParams', tuple)
        for t in self.outFileParams():
            checkAndThrow(len(t) == 3, "%s length is not 3." % str(t))
        self.checkParam('titleTemplate', str)
        self.checkParam('targetColumn', str)
        self.checkParam('yLabel', str)
        self.checkParamMaybe('patternMap', dict)
        self.checkParamMaybe('xRange', str)
        self.checkParamMaybe('scale', str)
        if 'scale' in self.getParams():
            checkAndThrow(re.match(ParamsForStatPlot.scaleReStr, self.scaleStr()),
                          "scale is not pattern '%s'" % ParamsForStatPlot.scaleReStr)

    def plotFileList(self):
        return self.get('plotFileList')
    
    def outDirList(self):
        return self.get('outDirList')

    def outFileTemplate(self, default=None):
        return self.getMaybe('outFileTemplate', default)

    def outFileParams(self):
        return self.get('outFileParams')

    def titleTemplate(self):
        return self.get('titleTemplate')

    def targetColumn(self):
        return self.get('targetColumn')

    def yLabel(self):
        return self.get('yLabel')

    def patternMap(self, default=None):
        return self.getMaybe('patternMap', default)

    def xRange(self, default="*:*"):
        return self.getMaybe('xRange', default)

    def scaleStr(self, default='1'):
        return self.getMaybe('scale', default)
    
    def scale(self, default='1'):
        """
        return :: Decimal
        
        """
        m = re.match(ParamsForStatPlot.scaleReStr, self.scaleStr())
        assert(m is not None)
        upper = m.group(1)
        lower = m.group(2)
        if lower is None:
            lower = '1'
        return Decimal(upper)/Decimal(lower)


class ParamsForHistogramPlot(ParamsChecker):
    """
    Parameters for histogram plotting.
    
    """
    def __init__(self, params):
        ParamsChecker.__init__(self, params)
        self.checkParams()

    def checkParams(self):
        ParamsChecker.checkParams(self)
        self.checkParamList('plotDirList', str)
        self.checkParamList('outDirList', str)
        self.checkParamMaybe('patternMap', dict)
        self.checkParamMaybe('outFileTemplate', str)
        self.patternModeListCheck()
        self.blockSizeUnitListCheck()
        self.nThreadsListCheck()
        self.checkParamList('widthList', str)

    def patternMap(self, default=None):
        return self.getMaybe('patternMap', default)

    def plotDirList(self):
        return self.get('plotDirList')
    
    def outDirList(self):
        return self.get('outDirList')

    def outFileTemplate(self, default=None):
        return self.getMaybe('outFileTemplate', default)

    def patternModeList(self):
        return self.get('patternModeList')
    
    def blockSizeUnitList(self):
        return self.get('blockSizeUnitList')

    def nThreadsList(self):
        return self.get('nThreadsList')
        
    def widthList(self):
        return self.get('widthList')


    
    
class ParamsForExpr(ParamsChecker):
    """
    Parameters for expreiment.
    
    """

    def __init__(self, params):
        ParamsChecker.__init__(self, params)
        self.checkParams()

    def checkParams(self):
        ParamsChecker.checkParams(self)
        self.checkParam('iores', str)
        self.checkParam('ioth', str)
        self.checkParam('targetDevice', str)
        self.checkParam('nLoop', int)
        self.checkParam('runPeriod', int)
        self.patternModeListCheck()
        self.nThreadsListCheck()
        self.blockSizeUnitListCheck()
        self.checkParam('storeEachLog', bool)

    def iores(self):
        return self.get('iores')

    def ioth(self):
        return self.get('ioth')

    def targetDevice(self):
        return self.get('targetDevice')

    def nLoop(self):
        return self.get('nLoop')

    def runPeriod(self):
        return self.get('runPeriod')

    def patternModeList(self):
        return self.get('patternModeList')

    def nThreadsList(self):
        return self.get('nThreadsList')

    def blockSizeUnitList(self):
        return self.get('blockSizeUnitList')

    def storeEachLog(self):
        return self.get('storeEachLog')

    def paramIter(self):
        """
        return :: generator((str, str, int, str))
            Generator of pattern, mode, nThreads, bsU.
        
        """
        for pattern, mode in self.patternModeList():
            for nThreads in self.nThreadsList():
                for bsU in self.blockSizeUnitList():
                    assert(isinstance(pattern, str))
                    assert(isinstance(mode, str))
                    assert(isinstance(nThreads, int))
                    assert(isinstance(bsU, str))
                    yield (pattern, mode, nThreads, bsU)


class ParamsForReport(ParamsChecker):
    """
    Parameters for report.
    
    """

    def __init__(self, params):
        ParamsChecker.__init__(self, params)
        self.checkParams()

    def checkParams(self):
        ParamsChecker.checkParams(self)
        self.checkParam('titleName', str)
        self.blockSizeUnitListCheck()
        
        self.checkParam('isPlotStat', bool)
        if self.isPlotStat():
            self.checkParamList('statNameList', str)
            self.checkParamList('statDirList', str)
            self.checkParamMaybe('statChartFileTemplate', str)
            self.checkParamList('chartTypeList', str)
            self.checkParamList('patternList', str)
            self.checkParamMaybe('statWidthPx', int)
            self.checkParamMaybe('statHeightPx', int)
        
        self.checkParam('isPlotHistogram', bool)
        if self.isPlotHistogram():
            self.checkParam('histogramDir', str)
            self.patternModeListCheck()
            self.nThreadsListCheck()
            self.checkParamMaybe('histogramChartFileTemplate', str)
            self.checkParamList('widthList', str)
            self.checkParamMaybe('histogramWidthPx', int)
            self.checkParamMaybe('histogramHeightPx', int)
            

    def titleName(self):
        return self.get('titleName')

    def blockSizeUnitList(self):
        return self.get('blockSizeUnitList')


    def isPlotStat(self):
        return self.get('isPlotStat')
    
    def statNameList(self):
        return self.get('statNameList')

    def statDirList(self):
        return self.get('statDirList')

    def chartTypeList(self):
        return self.get('chartTypeList')
    
    def patternList(self):
        return self.get('patternList')

    def statWidthPx(self, default=None):
        return self.getMaybe('statWidthPx', default)

    def statHeightPx(self, default=None):
        return self.getMaybe('statHeightPx', default)

    def statChartFileTemplate(self, default=None):
        return self.getMaybe('statChartFileTemplate', default)

    
    def isPlotHistogram(self):
        return self.get('isPlotHistogram')

    def histogramDir(self):
        return self.get('histogramDir')

    def patternModeList(self):
        return self.get('patternModeList')

    def nThreadsList(self):
        return self.get('nThreadsList')

    def histogramWidthPx(self, default=None):
        return self.getMaybe('histogramWidthPx', default)

    def histogramHeightPx(self, default=None):
        return self.getMaybe('histogramHeightPx', default)

    def widthList(self):
        return self.get('widthList')

    def histogramChartFileTemplate(self, default=None):
        return self.getMaybe('histogramChartFileTemplate', default)
