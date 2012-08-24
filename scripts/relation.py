#!/usr/bin/env python

__all__ = ['Record', 'Relation', 'joinRelations']

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
    cols :: [str]
    return :: Record

    """
    assert(isinstance(cols, list))
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
    assert(isinstance(cols, list))
    assert(isinstance(schema, list))
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
      True if reuse where all records will be copied.
      Specify False when you will access the records just once.

    """
    assert(isinstance(cols, list))
    
    self.__cols = list(cols) #copy
    self.__name = str(name) #copy
    self.__reuse = reuse
    self.__idata = IterableData(iterable, reuse=reuse)

  def schema(self):
    assert(isinstance(self.__cols, list))
    return self.__cols

  def renameCol(self, oldCol, newCol):
    """
    Rename a column name.
    
    oldCol :: str
      old column name.
    newCol :: str
      new column name.

    If oldCol not exists, ValueError wil be thrown.
    
    """
    assert(isinstance(oldCol, str))
    assert(isinstance(newCol, str))
    i = self.schema().index(oldCol)
    self.schema[i] = newCol

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
        return rec[cols]
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
      def getKey2(rec):
        assert(isinstance(rec, Record))
        return rec.raw()
      g = sorted(self.getRecG(), key=getKey2, reverse=reverse)

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

  def mapR(self, colsTo, mapper, name=None, reuse=False):
    """
    colsTo :: [str]
        Result schema.
    mapper :: Record -> rawRec
    
    """
    def g():
      for rec in self.getRecG():
        yield mapper(rec)
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


def joinTwoRelations(rawKey, relCols0, relCols1, reuse=False):
  """
  Join two relations.
  This will execute sort and merge join.
  
  rawKey :: tuple([str])
      Name of key columns.
      The key columns must be unique in both rel0 and rel1.
  relCols0 :: (Relation, cols)
  relCols1 :: (Relation, cols)
    cols :: [str]
      Target columns.
  reuse :: bool
  return :: Relation
    joined relations.
  
  """
  rel0, cols0 = relCols0
  rel1, cols1 = relCols1
  assert(isinstance(rel0, Relation))
  assert(isinstance(rel1, Relation))
  assert(isinstance(cols0, list))
  assert(isinstance(cols1, list))
  assert(len(cols0) > 0)
  assert(len(cols1) > 0)
  assert(isinstance(rawKey, tuple))
  assert(len(rawKey) > 0)

  # sorted records generator.
  schema0 = list(rawKey) + cols0
  schema1 = list(rawKey) + cols1
  g0 = iter(rel0.sort(cols=list(rawKey)).project(schema0).getG())
  g1 = iter(rel1.sort(cols=list(rawKey)).project(schema1).getG())

  # merged records generator.
  keySize = len(rawKey)
  def joinedRawRecG():
    isEnd = False
    try:
      rawRec0 = g0.next()
      rawRec1 = g1.next()
    except StopIteration:
      isEnd = True
      for _ in []:
        yield
    while not isEnd:
      try:
        if rawRec0[:keySize] == rawRec1[:keySize]:
          yield rawRec0[:keySize] + rawRec0[keySize:] + rawRec1[keySize:]
          rawRec0 = g0.next()
          rawRec1 = g1.next()
        elif rawRec0[:keySize] < rawRec0[:keySize]:
          rawRec0 = g0.next()
        else:
          rawRec1 = g1.next()
      except StopIteration:
        isEnd = True

  retSchema = list(rawKey) + cols0 + cols1
  return Relation(retSchema, joinedRawRecG(), reuse=reuse)


def testJoinRel():
  pass


def joinRelations(rawKey, relColsList, reuse=False):
  """
  Join multiple relations with a key.
  This will execute sort and merge join n-1 times
  where n is len(relColsList)
  
  rawKey :: tuple([str])
    The key must be unique.
  relColsList :: [(rel, cols)]
    rel :: Relation
    cols :: [str]
      target columns.
    Each column name must be unique.
  return :: Relation
    joined relation with the key and target columns.
  
  """
  assert(isinstance(rawKey, tuple))
  for col in rawKey:
    assert(isinstance(col, str))
  assert(isinstance(relColsList, list))
  assert(len(relColsList) >= 2)
  for rel, cols in relColsList:
    assert(isinstance(rel, Relation))
    assert(isinstance(cols, list))
    for col in cols:
      assert(isinstance(col, str))
      
  relCols0 = relColsList[0]
  for relCols1 in relColsList[1:]:
    relCols0 = joinTwoRelations(rawKey, relCols0, relCols1, reuse=False)

  if reuse:
    return Relation(relCols0.schema(), relCols0.getG(), reuse=True)
  else:
    return relCols0


def testJoinRelations():

  cols = ['k1', 'k2', 'v1']
  rawRecL = [(str(x), str(x), str(x)) for x in range(0, 3)]
  rel0 = Relation(cols, rawRecL, reuse=True)
  print rel0.show()
  
  cols = ['k1', 'k2', 'v2']
  rawRecL = [(str(x), str(y), str(x + y))
             for x in range(0, 3)
             for y in range(0, 3)]
  rel1 = Relation(cols, rawRecL, reuse=True)
  print rel1.show()
  
  joined = joinRelations(('k1', 'k2'),
                         [(rel0, ['v1']), (rel1, ['v2'])], reuse=True)
  print joined.show()

  cols = ['k1', 'k2', 'v1', 'v2']
  rawRecL = [(str(x), str(x), str(x), str(x + x)) for x in range(0, 3)]
  answer = Relation(cols, rawRecL, reuse=True)

  assert(all(map(lambda (x,y): x == y, zip(joined.getL(), answer.getL()))))

  
def doMain():
  #testRelation()
  testJoinRelations()

if __name__ == '__main__':
  doMain()
