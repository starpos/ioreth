#!/usr/bin/env python

__all__ = ['Record', 'Key', 'Relation', 'join']

import sys
import os
import re
import itertools

class Record:
  """
  Record type.

  self.rel :: Relation
  self.raw :: tuple([str])

  """
  def __init__(self, rel, rawRecord):
    assert(isinstance(rel, Relation))
    assert(isinstance(rawRecord, tuple))
    
    self.__rel = rel
    self.__raw = rawRecord

  def raw(self):
    """
    return :: tuple([str])
    
    """
    return self.__raw

  def relation(self):
    """
    return :: Relation
    
    """
    return self.__rel

  def show(self):
    """
    return :: str

    """
    return str(self.raw())

  def size(self):
    """
    return :: int
    
    """
    return len(self.__raw)


class Key:
  """
  Key type.
  
  self.record :: Record
  self.cols :: [str]
  self.idxes :: [int]
  self.raw :: tuple([str])
  
  """
  def __init__(self, record, cols=None):
    """
    record :: Record
    cols :: [str]
       Name of columns.
    
    """
    assert(isinstance(record, Record))
    assert(isinstance(cols, list))
    
    self.__record = record
    if cols:
      self.__cols = cols
      self.__idxes = self.__getIndexes(self.__cols)
      self.__raw = self.__getKey(self.__idxes, self.__record.raw())
    else:
      self.__cols = record.relation().schema()
      self.__indexes = range(0, record.size())
      self.__raw = record.raw()

  def raw(self):
    """
    return :: tuple([str])
    
    """
    return self.__raw

  def schema(self):
    return self.__cols
    
  def record(self):
    """
    return :: Record

    """
    return self.__record
  
  def show(self):
    """
    return :: str

    """
    return str(self.raw())

  def show2(self):
    """
    return :: str
    
    """
    return ','.join(list(self.raw()))

  def size(self):
    """
    return :: int
    
    """
    return len(self.__raw)

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

  def __getIndexes(self, cols):
    """
    cols :: [str]
      Name of columns.
    return :: [int]
    
    """
    m = map(lambda col: self.__record.relation().schema().index(col), cols)
    return m


class Relation:
  """
  Relation type.
    rawRec :: tuple([str])
    rawKey :: tuple([str])

  """
  def __init__(self, cols, generatorOrList, name=None, isTmp=True):
    """
    cols :: [str]
    generator :: generator(rawRec) or [rawRec]
    name :: str
      Relation name.
    isTmp :: bool
      True if temporary use.

    """
    assert(isinstance(cols, list))
    
    self.__cols = cols
    self.__name = name
    self.__isTmp = isTmp
    if isinstance(generatorOrList, list):
      self.__recL = generatorOrList
      self.__recG = None
    else:
      self.__recL = None
      self.__recG = generatorOrList

  def schema(self):
    assert(isinstance(self.__cols, list))
    return self.__cols

  def getL(self):
    """
    return :: [rawRec]
    
    """
    if self.__recL is None:
      self.__recL = list(self.__recG)
      self.__recG = None
    return self.__recL
  
  def getG(self):
    """
    return :: generator(rawRec)
    
    """
    if not self.__isTmp:
      self.getL()
    if self.__recL is not None:
      def genRecG():
        for rec in self.__recL:
          yield rec
      return genRecG()
    else:
      return self.__recG

  def size(self):
    return len(self.getL())

  def select(self, indexes, isTmp=True):
    """
    indexes :: [int]
        each item must be from 0 to num of records - 1.
    return :: Relation
    
    """
    def gen():
      for idx in indexes:
        yield self.getL()[idx]
    return Relation(self.schema(), gen(), self.__isTmp)

  def filter(self, pred, isTmp=True):
    """
    pred :: Record -> bool
    return :: Relation
    
    """
    def predicate(rawRec):
      return pred(Record(self, rawRec))
    g = itertools.ifilter(predicate, self.getG()) 
    return Relation(self.schema(), g, isTmp)

  def project(self, cols, isTmp=True):
    """
    cols :: [str]
    return :: Relation
    
    """
    def mapper(rawRec):
      return Key(Record(self, rawRec), cols).raw()
    g = itertools.imap(mapper, self.getG())
    return Relation(tuple(cols), g, isTmp)

  def sort(self, key=None, lesser=None, isTmp=True):
    """
    key :: [str]
        Name of columns.
    lesser :: Rec -> Rec -> bool

    """
    raise "Not yet implemented."

  def groupbyAsRelation(self, keyCols, valCols):
    """
    keyCols :: [str]
        Name of key columns.
    valCols :: [str]
        Name of value columns.

    return :: dict(keyColsT :: tuple([str]), Relation)
    
    """
    def op(rel, rec):
      assert(isinstance(rel, Relation))
      assert(isinstance(rec, Record))
      val = Key(rec, valCols)
      rel.insert(val.raw())
      return rel
    def cstr():
      return Relation(valCols, [])
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
      key = Key(rec, keyCols)
      rawKey = key.raw()
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
    g = itertools.imap(lambda rawRec: Record(self, rawRec), self.getG())
    return reduce(op, g, init)

  def foldr(self, op, init, rel):
    """
    fold a relation from right side.

    """
    raise "Not yet implemented."

  def insert(self, rawRec):
    assert(len(rawRec) == len(self.schema()))
    self.getL().append(rawRec)

  def show(self):
    """
    return :: str
    
    """
    c = cStringIO.StringIO()
    print >>c, self.getL(),
    ret = c.getvalue()
    c.close()
    return ret


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
