#!/bin/sh
ls -RF1 | grep / | cut -d: -f1 | awk '{if(NR>1&&substr(x,0,1)=="."&&substr($0,0,1)!=".") {printf("%s/",x);} else {x=$0;}print $0;}' > x.tmp
xlen=`wc -l x.tmp | awk '{printf $1}'`	
i="1"
while [ $i -lt $xlen ]
do
fpi=`gawk 'NR=='$i'{printf $0}' x.tmp`	
echo "$fpi"
cp -p index.php "$fpi"  
i=$[$i+1]
done
rm -rf x.tmp
