#!/bin/sh

IORES="sudo ./iores"
IOTH="sudo ./ioth"

TARGET_DEVICE=/dev/data/ioexpr
N_LOOP=5
RUN_PERIOD=10
N_THREADS_LIST="1 2 4 8 12 16 24 32"
BLKSIZ_LIST="512 4096 32768 262144"

loop=0
while [ $loop -lt $N_LOOP ]; do
  for n_threads in $N_THREADS_LIST; do
    for blksiz in $BLKSIZ_LIST; do
      echo $loop $n_threads $blksiz 
      sfx=$n_threads/$blksiz/$loop;
      resdir=rnd/read/$sfx; mkdir -p $resdir
      $IORES -b $blksiz -p $RUN_PERIOD -t $n_threads -r    $TARGET_DEVICE > $resdir/res
      resdir=rnd/write/$sfx; mkdir -p $resdir
      $IORES -b $blksiz -p $RUN_PERIOD -t $n_threads -r -w $TARGET_DEVICE > $resdir/res
      resdir=rnd/mix/$sfx; mkdir -p $resdir
      $IORES -b $blksiz -p $RUN_PERIOD -t $n_threads -r -m $TARGET_DEVICE > $resdir/res

      resdir=seq/read/$sfx; mkdir -p $resdir
      $IOTH -b $blksiz -p $RUN_PERIOD -t $n_threads -r    $TARGET_DEVICE > $resdir/res
      resdir=seq/write/$sfx; mkdir -p $resdir
      $IOTH -b $blksiz -p $RUN_PERIOD -t $n_threads -r -w $TARGET_DEVICE > $resdir/res
    done
  done
  loop=`expr $loop + 1`
done

