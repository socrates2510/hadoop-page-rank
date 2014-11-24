#!/bin/bash

MAXCOUNT=$1
count=1


while [ $count -le $MAXCOUNT ]
do
  RANGE=15
  outlinks=$RANDOM
  let "outlinks %=$RANGE"
  
  outseqs=`gshuf -i 1-$MAXCOUNT -n $outlinks`
  outs=`perl -le 'print join ",",@ARGV' ${outseqs[@]}`
  
  echo "${count}	1.0	${outs}"

  let "count += 1"
done

