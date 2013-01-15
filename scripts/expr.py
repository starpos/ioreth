#!/usr/bin/env python

import os
import sys
import util
import time

from param import *

class Command(ParamsForExpr):
    """
    Experiment command generator.

    """

    def __init__(self, params):

        ParamsForExpr.__init__(self, params)

        self.setPattern('rnd')
        self.setMode('read')
        self.setNThreads(1)
        self.setBsU('4k')
        self.setRunPeriod(ParamsForExpr.runPeriod(self))
        self.setTargetDevice(ParamsForExpr.targetDevice(self))
        self.setStoreEachLog(ParamsForExpr.storeEachLog(self))
        self.setWarmup(ParamsForExpr.warmup(self))
        self.setSleep(ParamsForExpr.sleep(self))
        self.setIgnorePeriod(ParamsForExpr.ignorePeriod(self))
        self.setInitCmd(ParamsForExpr.initCmd(self))
        self.setExitCmd(ParamsForExpr.exitCmd(self))

    def clone(self):

        cmd = Command(self.getParams())
        cmd.setPattern(self.pattern())
        cmd.setMode(self.mode())
        cmd.setRunPeriod(self.runPeriod())
        cmd.setTargetDevice(self.targetDevice())
        cmd.setNThreads(self.nThreads())
        cmd.setBsU(self.bsU())
        cmd.setSleep(self.sleep())
        cmd.setIgnorePeriod(self.ignorePeriod())
        return cmd

    def pattern(self):
        return self.__pattern
    def mode(self):
        return self.__mode
    def runPeriod(self):
        return self.__runPeriod
    def targetDevice(self):
        return self.__targetDevice
    def nThreads(self):
        return self.__nThreads
    def bsU(self):
        return self.__bsU
    def bs(self):
        return self.__bs
    def storeEachLog(self):
        return self.__storeEachLog
    def warmup(self):
        return self.__warmup
    def sleep(self):
        return self.__sleep
    def ignorePeriod(self):
        return self.__ignorePeriod
    def initCmd(self):
        return self.__initCmd
    def exitCmd(self):
        return self.__exitCmd

    def setPattern(self, pattern):
        assert(pattern == 'seq' or pattern == 'rnd')
        self.__pattern = pattern

    def setMode(self, mode):
        assert(mode == 'read' or mode == 'write' or mode == 'mix')
        self.__mode = mode

    def setRunPeriod(self, runPeriod):
        assert(isinstance(runPeriod, int))
        assert(runPeriod > 0)
        self.__runPeriod = runPeriod

    def setTargetDevice(self, targetDevice):
        assert(isinstance(targetDevice, str))
        assert(len(targetDevice) > 0)
        self.__targetDevice = targetDevice

    def setNThreads(self, nThreads):
        assert(isinstance(nThreads, int))
        self.__nThreads = nThreads

    def setBsU(self, bsU):
        assert(isinstance(bsU, str))
        assert(isinstance(util.u2s(bsU), int))
        self.__bsU = bsU
        self.__bs = util.u2s(bsU)

    def setStoreEachLog(self, storeEachLog):
        assert(isinstance(storeEachLog, bool))
        self.__storeEachLog = storeEachLog

    def setWarmup(self, warmup):
        assert(isinstance(warmup, bool))
        self.__warmup = warmup

    def setSleep(self, sleep):
        assert(isinstance(sleep, int))
        self.__sleep = sleep

    def setIgnorePeriod(self, ignorePeriod):
        assert(isinstance(ignorePeriod, int))
        assert(ignorePeriod >= 0)
        self.__ignorePeriod = ignorePeriod

    def setInitCmd(self, initCmd):
        if initCmd is None:
            self.__initCmd = None
        else:
            assert(isinstance(initCmd, str))
            self.__initCmd = initCmd
    def setExitCmd(self, exitCmd):
        if exitCmd is None:
            self.__exitCmd = None
        else:
            assert(isinstance(exitCmd, str))
            self.__exitCmd = exitCmd

    def optMode(self, mode):
        return {'read':'', 'write':'-w', 'mix':'-m'}[mode]

    def binary(self, pattern):
        return {'seq':self.ioth(), 'rnd':self.iores()}[pattern]

    def optR(self):
        if self.storeEachLog():
            return '-r'
        else:
            return ''

    def cmdStr(self):
        if self.pattern() == "seq":
            parallelOpt = "-t 0 -q"
        else:
            parallelOpt = "-t"
        retS = "%s -b %d -p %d %s %d %s %s %s" % \
            (self.binary(self.pattern()), self.bs(), self.runPeriod(),
             parallelOpt, self.nThreads(),
             self.optR(), self.optMode(self.mode()),
             self.targetDevice())
        if self.pattern() == "seq":
            return retS
        else:
            return retS + (" -i %s" % self.ignorePeriod())

    def resDirPath(self, loop):
        assert(isinstance(loop, int))
        assert(loop >= 0)
        return 'res/%s/%s/%d/%d/%d' % \
            (self.pattern(), self.mode(), self.nThreads(), self.bs(), loop)


def runExpr(rawParams):
    """
    Generate and execute commands using given parameters.

    """
    cmdOrig = Command(rawParams)

    for pattern, mode, nThreads, bsU in cmdOrig.paramIter():
        assert(isinstance(pattern, str))
        assert(isinstance(mode, str))
        assert(isinstance(nThreads, int))
        assert(isinstance(bsU, str))

        cmd = cmdOrig.clone()
        cmd.setPattern(pattern)
        cmd.setMode(mode)
        cmd.setNThreads(nThreads)
        cmd.setBsU(bsU)
        cmdStr = cmd.cmdStr()
        print cmdStr #debug

        if cmd.warmup():
            cmdWarmup = cmd.clone()
            #cmdWarmup.setRunPeriod(1)
            totalCmd = '%s > /dev/null' % cmdWarmup.cmdStr()
            if cmd.initCmd() is not None:
                os.system(cmd.initCmd())
            os.system(totalCmd)
            if cmd.exitCmd() is not None:
                os.system(cmd.exitCmd())

        totalCmdTemplate = "%s |tee %s/res |grep Throughput"
        for loop in xrange(0, cmd.nLoop()):
            resDirPath = cmd.resDirPath(loop)
            if not os.path.exists(resDirPath):
                os.makedirs(resDirPath)
            if loop == 0:
                cmdTmp = cmd.clone()
                #cmdTmp.setStoreEachLog(True)
                totalCmd = totalCmdTemplate % (cmdTmp.cmdStr(), resDirPath)
            else:
                totalCmd = totalCmdTemplate % (cmdStr, resDirPath)
            print totalCmd #debug
            if cmd.initCmd() is not None:
                os.system(cmd.initCmd())
            os.system(totalCmd)
            if cmd.sleep() > 0:
                time.sleep(cmd.sleep())
            if cmd.exitCmd() is not None:
                os.system(cmd.exitCmd())

def main():
    rawParams = getParams(sys.argv[1])
    try:
        runExpr(rawParams)
    except ParamsException, msg:
        print msg

if __name__ == '__main__':
    main()
