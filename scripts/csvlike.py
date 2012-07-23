#!/usr/bin/env python

__all__ = ['CsvLike']

import re

from relation import Relation

class CsvLike(Relation):
  """
  CSV-like data.

  1st line must be header starting by '#'
  where column names are listed separated by the separator.
  
  """
  def __init__(self, lineGenerator, sep=None, index={}):
    """
    lineGenerator :: generator(str)
      CSV-like data.
    sep :: str
       separator for str.split().
    index :: dict(tuple(columnName :: str), [recordIndex :: int]))
      Currently unused.

    """
    schemaLine = lineGenerator.next().rstrip()
    schemaLine = re.sub('^#', '', schemaLine)
    schema = schemaLine.split(sep)
    def getRawRecGenerator():
      for line in lineGenerator:
        yield tuple(line.rstrip().split(sep))
    Relation.__init__(self, schema, getRawRecGenerator(), reuse=True)

