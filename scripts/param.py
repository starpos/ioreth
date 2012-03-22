#!/usr/bin/env python

"""
Make report template with mediawiki format from a parameter file.

"""

__all__ = ['getParams', 'ParamsForExpr', 'ParamsForReport', 'ParamsException']

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

    def checkParam(self, name, typeId):
        self.checkParamExist(name)
        checkAndThrow(isinstance(self.__params[name], typeId),
                      "%s is not the type %s." % (name, str(typeId)))

    def checkParamMaybe(self, name, typeId):
        if name in self.__params:
            self.checkParam(name, typeId)

    def checkParamList(self, name, typeId):
        self.checkParamExist(name)
        checkAndThrow(isinstance(self.__params[name], list),
                      "%s is not list." % name)
        for x in self.__params[name]:
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


class ParamsShared(Params):
    """
    Parameters shared by both ParamsForExpr and ParamsForReport.
    
    """

    def __init__(self, params):
        Params.__init__(self, params)
        self.checkParams()
        
    def checkParams(self):
        self.checkParamList('nThreadsList', int)
        for nThreads in self.nThreadsList():
            checkAndThrow(nThreads > 0, "Satisfy nThreads > 0")
        self.checkParamList('patternModeList', tuple)
        for pattern, mode in self.patternModeList():
            checkAndThrow(isinstance(pattern, str))
            checkAndThrow(pattern == 'seq' or pattern == 'rnd')
            checkAndThrow(isinstance(mode, str))
            checkAndThrow(mode == 'read' or mode == 'write' or mode == 'mix')
        self.checkParamList('blockSizeUnitList', str)
        for bsU in self.blockSizeUnitList():
            bs = util.u2s(bsU)
            checkAndThrow(isinstance(bs, int))
            checkAndThrow(bs > 0)
    
    def patternModeList(self):
        return self.get('patternModeList')

    def nThreadsList(self):
        return self.get('nThreadsList')

    def blockSizeUnitList(self):
        return self.get('blockSizeUnitList')

    
class ParamsForExpr(ParamsShared):
    """
    Parameters for expreiment.
    
    """

    def __init__(self, params):
        ParamsShared.__init__(self, params)
        self.checkParams()

    def checkParams(self):
        self.checkParam('iores', str)
        self.checkParam('ioth', str)
        self.checkParam('targetDevice', str)
        self.checkParam('nLoop', int)
        self.checkParam('runPeriod', int)
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


class ParamsForReport(ParamsShared):
    """
    Parameters for report.
    
    """

    def __init__(self, params):
        ParamsShared.__init__(self, params)
        self.checkParams()

    def checkParams(self):
        self.checkParam('exprName', str)
        self.checkParamList('nameList', str)
        self.checkParamList('statisticsDirectories', str)
        self.checkParamMaybe('histogramDirectory', str)
        self.checkParamMaybe('widthPx', int)
        self.checkParamMaybe('heightPx', int)
        self.checkParamList('chartTypeList', str)
        self.checkParamList('patternList', str)
        self.checkParamList('widthList', str)
        self.checkParamMaybe('statisticsChartFileNameTemplate', str)
        self.checkParamMaybe('histogramChartFileNameTemplate', str)
        
    def exprName(self):
        return self.get('exprName')

    def nameList(self):
        return self.get('nameList')

    def statisticsDirectories(self):
        return self.get('statisticsDirectories')

    def histogramDirectory(self):
        return self.getMaybe('histogramDirectory')

    def widthPx(self, default=None):
        return self.getMaybe('widthPx', default)

    def heightPx(self, default=None):
        return self.getMaybe('heightPx', default)

    def chartTypeList(self):
        return self.get('chartTypeList')

    def patternList(self):
        return self.get('patternList')

    def widthList(self):
        return self.get('widthList')

    def statisticsChartFileNameTemplate(self, default=None):
        return self.getMaybe('statisticsChartFileNameTemplate', default)

    def histogramChartFileNameTemplate(self, default=None):
        return self.getMaybe('histogramChartFileNameTemplate', default)
