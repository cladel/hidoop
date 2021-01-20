#!/bin/bash


classes=/home/cla/N7/2A/hidoop/out/production/hidoop/
folder=/home/cla/N7/2A/hidoop/files/
files=(aragon.txt medium.txt big.txt)

cd $classes

echo 'Calling Count...'
for f in ${files[@]}; do 
	echo $f
	java application.Count $folder$f
	mv ${folder}count-res ${folder}$f-ref
done

echo ''
echo 'Launching workers...'
java ordo.WorkerImpl 1 &  p1=$!
java ordo.WorkerImpl 2 &  p2=$!
java ordo.WorkerImpl 3 &  p3=$!
sleep 2
echo ''
echo 'Calling Job...'
for f in ${files[@]}; do 
	echo ''
	echo $f
	java application.MyMapReduce $folder$f
done

kill $p1
kill $p2
kill $p3

echo 'Checking results integrity...'
for f in ${files[@]}; do 
	echo ''
	echo $f
	cmp <(sort $folder$f-ref) <(sort $folder$f-res)
	
done

