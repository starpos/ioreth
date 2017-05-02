#!/bin/sh

version=$(git br  -v |head -n1 |awk '{print $3}')

name=ioreth
tarprefix=${name}_${version}/
tarfile=${name}_${version}.tar

git archive HEAD --format=tar --prefix $tarprefix -o $tarfile
tar f $tarfile --wildcards --delete '*/.gitignore'
gzip $tarfile

