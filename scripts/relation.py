#!/usr/bin/env python

__all__ = ['Record', 'Relation', 'join']

import sys
import os
import re
import types
import collections
import itertools

import util

class Record:
  """
  Record type.

  self.cols :: [str]
     Name of columns.
  self.raw :: tuple([str])

  """
  def __init__(self, cols, rawRec):
    """
    cols :: [str]
    rawRec :: tuple(str)
    
    """
    assert(isinstance(cols, list))
    assert(isinstance(rawRec, tuple))
    
    self.__cols = cols
    self.__raw = rawRec

  def raw(self):
    """
    return :: tuple([str])
    
    """
    return self.__raw

  def schema(self):
    """
    return :: [str]
    
    """
    return self.__cols

  def show(self):
    """
    return :: str

    """
    return str(self.raw())

  def show2(self):
    """
    return :: str
    
    """
    return '\t'.join(list(self.raw()))

  def size(self):
    """
    return :: int
    
    """
    return len(self.__raw)

  def getKey(self, cols):
    """
    return :: Record

    """
    idxes = self.__getIndexes(cols, self.schema())
    #print idxes
    raw = self.__getKey(idxes, self.raw())
    #print raw
    return Record(cols, raw)

  def __getitem__(self, cols):
    """
    cols :: str | [str] | tuple([str])
      a column or columns
    return :: str | tuple(str)
      item or items found.
    
    """
    assert(isinstance(cols, str) or \
             isinstance(cols, list) or isinstance(cols, tuple))
    if isinstance(cols, str):
      return self[(cols,)][0]
    else:
      ret = []
      for col in cols:
        assert(isinstance(col, str))
        idx = self.__cols.index(col)
        ret.append(self.raw()[idx])
      return tuple(ret)

  def __eq__(self, rhs):
    return self.schema() == rhs.schema() and self.raw() == rhs.raw()

  def __getKey(self, idxes, rec):
    """
    idxes :: [int]
    rec :: Record
    return :: Key
    
    """
    ret = []
    for idx in idxes:
      ret.append(rec[idx])
    return tuple(ret)

  def __getIndexes(self, cols, schema):
    """
    cols :: [str]
      Name of columns to get index.
    schema :: [str]
      Name of all columns as a schema.
    return :: [int]
    
    """
    m = map(lambda col: schema.index(col), cols)
    return m

def testRecord():
  """
  For test.
  
  """
  cols = ['col0', 'col1', 'col2']
  rawRec = ('0', '1', '2')
  rec = Record(cols, rawRec)
  assert(rec.size() == 3)
  assert(rec.schema() == cols)
  assert(rec.getKey(['col0']).raw() == ('0',))
  assert(rec.getKey(['col1']).raw() == ('1',))
  assert(rec.getKey(['col2']).raw()  == ('2',))
  assert(rec.getKey(['col0', 'col1']).raw() == ('0', '1'))
  assert(rec.getKey(['col2', 'col0']).raw() == ('2', '0'))
  assert(rec.raw() == rawRec)
  assert(rec.getKey(['col1']) == Record(['col1'], ('1',)))


class IterableData:
  """
  A iterable data.
  
  """

  def __init__(self, iterable, reuse=False):

    self.__isL = isinstance(iterable, list)
    if self.__isL:
      self.__l = iterable
      self.__g = None
    else:
      assert(isinstance(iterable, collections.Iterable))
      self.__l = None
      self.__g = iterable
    self.__reuse = reuse

  def isL(self):
    return self.__isL
  
  def toL(self):
    if not self.__isL:
      self.__isL = True
      self.__l = list(self.__g)
      self.__g = None
    return self.__l

  def __iter__(self):
    if self.__reuse:
      self.toL()
    if self.__isL:
      for x in self.__l:
        yield x
    else:
      for x in self.__g:
        yield x

  def __str__(self):
    return str(self.toL())

  def __add__(self, rhs): # (+)
    assert(isinstance(rhs, IterableData))
    return IterableData(util.gplus(self.__iter__(), rhs.__iter__()),
                        reuse=self.__reuse)

def testIterableData():

  xs = [0, 1, 2, 3, 4]
  
  ys = IterableData(xs)
  assert(ys.isL())
  assert(list(ys) == xs)
  
  def g():
    for x in xs:
      yield x
  
  ys = IterableData(g())
  assert(ys.isG())
  assert(list(ys) == xs)
  for x in ys:
    raise "ys must be empty."
  ys = IterableData(xs) + IterableData(xs)
  assert(ys.__str__() == '[0, 1, 2, 3, 4, 0, 1, 2, 3, 4]')
  
  
class Relation:
  """
  Relation type.
    rawRec :: tuple([str])
    rawKey :: tuple([str])

  """
  def __init__(self, cols, iterable, name=None, reuse=False):
    """
    cols :: [str]
    iterable :: iterable(rawRec)
    name :: str
      Relation name.
    reuse :: bool
      True if reuse.

    """
    assert(isinstance(cols, list))
    
    self.__cols = cols
    self.__name = name
    self.__reuse = reuse
    self.__idata = IterableData(iterable, reuse=reuse)

  def schema(self):
    assert(isinstance(self.__cols, list))
    return self.__cols

  def name(self):
    return self.__name

  def getL(self):
    """
    return :: [rawRec]
    
    """
    return self.__idata.toL()
  
  def getG(self):
    """
    return :: generator(rawRec)
    
    """
    return self.__idata
    
  def getRecG(self):
    """
    return :: generator(Record)
    
    """
    for rawRec in self.getG():
      yield Record(self.schema(), rawRec)
      
  def getRecL(self):
    """
    return :: [Record]

    """
    return list(self.getRecG())
    
  def size(self):
    return len(self.getL())

  def select(self, indexes, reuse=False):
    """
    indexes :: [int]
        each item must be from 0 to num of records - 1.
    return :: Relation
    
    """
    def gen():
      for idx in indexes:
        yield self.getL()[idx]
    return Relation(self.schema(), gen(), self.__reuse)

  """
  def __getitem__(self, indexes):
    self.select(indexes, reuse=False)
  """

  def filter(self, pred, reuse=False):
    """
    pred :: Record -> bool
    return :: Relation
    
    """
    def predicate(rawRec):
      return pred(Record(self.schema(), rawRec))
    g = itertools.ifilter(predicate, self.getG()) 
    return Relation(self.schema(), g, reuse)

  def project(self, cols, reuse=False):
    """
    cols :: [str]
    return :: Relation
    
    """
    assert(isinstance(cols, list))
    def mapper(rawRec):
      return Record(self.schema(), rawRec).getKey(cols).raw()
    g = itertools.imap(mapper, self.getG())
    return Relation(cols, g, reuse)

  def sort(self, cols=None, key=None, lesser=None, reverse=False, reuse=False):
    """
    cols :: [str]
        Name of columns.
    key :: Record -> a
    lesser :: (Record, Record) -> bool

    reverse :: bool
    reuse :: bool

    a :: any
       Comparable each other.

    return :: Relation
       
    """
    if cols is not None:
      def getKey(rec):
        assert(isinstance(rec, Record))
        return rec[cols].raw()
      g = sorted(self.getRecG(), key=getKey, reverse=reverse)
    elif key is not None:
      g = sorted(self.getRecG(), key=key, reverse=reverse)
    elif lesser is not None:
      def cmpRec(rec0, rec1):
        assert(isinstance(rec0, Record))
        assert(isinstance(rec1, Record))
        if lesser(rec0, rec1):
          return -1
        elif lesser(rec1, rec0):
          return 1
        else:
          return 0
      g = sorted(self.getRecG(), cmp=cmpRec, reverse=reverse)
    else:
      def getKey(rec):
        assert(isinstance(rec, Record))
        return rec.raw()
      g = sorted(self.getRecG(), key=getKey, reverse=reverse)

    return Relation(self.schema(),
                    itertools.imap(lambda rec: rec.raw(), g), reuse=reuse)
  

  def groupbyAsRelation(self, keyCols, valCols=None, reuse=False):
    """
    keyCols :: [str]
        Name of key columns.
    valCols :: [str] | None
        Name of value columns.

    return :: dict(keyColsT :: tuple([str]), Relation)
    
    """
    def op(rel, rec):
      assert(isinstance(rel, Relation))
      assert(isinstance(rec, Record))
      rawRec = rec.raw() if valCols is None else rec.getKey(valCols).raw()
      rel.insert(rawRec)
      return rel
    def cstr():
      cols = self.schema() if valCols is None else valCols
      return Relation(cols, [], reuse=reuse)
    return self.groupby(keyCols, op, cstr)

  def groupby(self, keyCols, op, cstr):
    """
    'group by' operation.
    
    keyCols :: [str]
        Name of key columns.
    op :: (a -> Record -> a)
    cstr :: () -> a
        Constructor for initial value.

    return :: dict(keyColsT :: tuple([str]), a)

    a :: any
    
    """
    def tmpOp(d, rec):
      assert(isinstance(d, dict))
      assert(isinstance(rec, Record))
      rawKey = rec.getKey(keyCols).raw()
      if rawKey not in d:
        d[rawKey] = cstr()
      d[rawKey] = op(d[rawKey], rec)
      return d
    return self.foldl(tmpOp, {})
    

  def foldl(self, op, init):
    """
    Fold a relation from left side.
    op :: a -> Record -> a
    init :: a
    rel :: Relation
    return :: a
    
    a :: any

    """
    return reduce(op, self.getRecG(), init)

  def foldr(self, op, init, rel):
    """
    fold a relation from right side.

    """
    raise "Not yet implemented."

  def map(self, colsFrom, colsTo, mapper, name=None, reuse=False):
    """
    mapper :: rawRec -> rawRec
    
    """
    def g():
      for rec in self.getRecG():
        yield mapper(rec.getKey(colsFrom).raw())
    return Relation(colsTo, g(), name=name, reuse=reuse)

  def mapG(self, mapper, colsFrom=None):
    """
    mapper :: rawRec -> a
    return :: generator(a) | None
    a :: any
    
    """
    if colsFrom is None:
      tmpRel = self
    else:
      tmpRel = self.project(colsFrom)
    return itertools.imap(mapper, tmpRel.getG())

  def mapL(self, mapper, colsFrom=None):
    """
    mapper :: rawRec -> a
    colsFrom :: [str] | None
    return :: [a]
    a :: any
    
    """
    if colsFrom is None:
      tmpRel = self
    else:
      tmpRel = self.project(colsFrom)
    return map(mapper, tmpRel.getL())
  
  def insert(self, rawRec):
    assert(len(rawRec) == len(self.schema()))
    self.getL().append(rawRec)

  def insertRec(self, rec):
    assert(isinstance(rec, Record))
    self.getL().append(rawRec)

  def insertL(self, rawRecL):
    self.__idata = self.__idata + IterableData(rawRecL, reuse=self.__reuse)

  def show(self):
    """
    return :: str
    
    """
    return '#' + '\t'.join(self.schema()) + '\n' + \
        '\n'.join(map(lambda x: '\t'.join(x), self.getL())) + '\n'

  def __str__(self):
    """
    return :: str
    
    """
    return self.show()

def testRelation():
  """
  For test.

  """
  cols = ['c1', 'c2', 'c3']
  rawRecL = [(str(x), str(y), str(z)) for (x, y) in [(0, 1), (1, 0)] for z in range(0, 5)]
  rel = Relation(cols, rawRecL, reuse=True)
  print rel.show()

  # schema
  assert(rel.schema() == cols)

  # map
  mapper = lambda (c1,c2): (str(int(c1) + int(c2)),)
  rel1 = rel.map(['c1', 'c2'], ['c4'], mapper)
  print rel1.show()
  assert(all(map(lambda (x,): x == '1', rel1.getL())))

  # foldl
  def op(i, rec):
    return i + int(rec['c1']) + int(rec['c2']) + int(rec['c3'])
  assert(rel.foldl(op, 0) == 30)
  
  # groupby
  d = rel.groupbyAsRelation(['c1'], ['c2', 'c3'])
  print "aaa"
  print str(d['0',]), str(d['1',]),

  assert(d['0',].size() == 5)
  for c2,c3 in d['0',].getL():
    assert(c2 == '1')
  assert(d['1',].size() == 5)
  for c2,c3 in d['1',].getL():
    assert(c2 == '0')

  # insertL
  a = [('2', '2', '0'), ('2', '2', '1'), ('2', '2', '2')]
  rel.insertL(a)
  print rel
  assert(rel.size() == 13)

  # sort
  rel2 = rel.sort(reverse=True)
  print rel2
  
  
def join(rel0, rel1, key=None, pred=None, columns=None):
  """
  Join two relations.
  
  rel0 :: Relation
  rel1 :: Relation
  key :: tuple([str])
      Name of key columns.
  pred :: Rec -> Rec -> bool
    key will be used rather than pred.
  columns :: [str]
    new names of columns
  
  """
  raise "Not yet implemented."

  
def testJoin():
  pass


def doMain():
  testRelation()

if __name__ == '__main__':
  doMain()
