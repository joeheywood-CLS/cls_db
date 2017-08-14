#!/bin/bash

dir=`pwd`
mclient -d mcs -i /home/db/cls_db/jsonSchm.sql

for col in  $(find /home/db/ -name '*_build.sql')
do
	echo "**********************"
	echo $col
	mclient -d mcs -i $col
done


