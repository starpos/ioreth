#!/bin/sh

DEBUG=0

teee()
{
  local logid=$1
  if [ $DEBUG -eq 1 ]; then
    tee $logid.log
  else
    cat
  fi
}

get_throughput() 
{
  #echo Bps iops seq/rnd read/write/mix nThreads blockSize iter #3
  echo "#pattern mode nThreads blockSize Bps iops" #5
  grep ^Throughput: ./*/*/*/*/*/res \
|teee 1 |map.py -g 1 -f "lambda x: x.split('/')" \
|teee 2 |project.py -g 2,6,9,10,11,12,13 \
|teee 3 |groupby.py -g 3,4,5,6 -v 1,2 \
|teee 4 |sort.py -g 1,2,n3,n4 \
|teee 5 |cat
}

get_response()
{
  echo "#pattern mode nThreads blockSize response"
  grep "^threadId all" ./*/*/*/*/*/res \
|sed 's/threadId all//' > 1.$$
  grep "^all" ./*/*/*/*/*/res \
|sed 's/all[ ]\+[0-9]\+//' > 2.$$
  cat 1.$$ 2.$$ \
|teee 1 |map.py -g 1 -f "lambda x: x.split('/')" \
|teee 2 |project.py -g 7,13,14,15,16,17 \
|teee 3 |groupby.py -g 2,3,4,5 -v 1 \
|teee 4 |sort.py -g 1,2,n3,n4 \
|cat
  rm -f 1.$$ 2.$$
}

get_histogram()
{
  local d=$1 #directory
  local width=$2 #sec
  #threadId 0 isWrite 0 blockId      38775 startTime 1330336299.661363 response 0.008008
  cat ${d}/*/res \
|grep '^threadId' \
|teee 1 |filter.py -g 1,2 -r 'threadId' '[0-9]+' \
|teee 2 |project.py -g 10 \
|teee 3 |./histogram.py ${width} \
|teee 4 > ${d}/histogram_${width}
}

get_all_histograms()
{
  local d
  local width
  for d in ./seq/*/*/*/ ./rnd/*/*/*/; do
    for width in 0.0001 0.001 0.01; do
      echo $d $width
      get_histogram $d $width
    done
  done
}

#get_throughput > throughput.plot
#get_response > response.plot
get_all_histograms
#get_histogram ./seq/read/4/32768 0.001
