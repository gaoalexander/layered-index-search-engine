#!/bin/bash
now=$(date +"%T")
echo "Start time : $now"
sortCmd="sort -S 50%"
for input in ./data/1_intermediate/postings/*.gz; do
    sortCmd="$sortCmd <(gzcat '$input')"
done
eval "$sortCmd" | gzip -c > ./data/1_intermediate/postings/sorted.gz
now=$(date +"%T")
echo "End time : $now"
