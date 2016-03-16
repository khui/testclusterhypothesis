qrel=/GW/D5data-2/khui/qrel/completeqrel/diversity/qrels.diversity.wtf
#/GW/D5data-2/khui/qrel/completeqrel/adhoc/qrels.adhoc.wt1234
outputfolder=/GW/D5data-2/khui/qrel/completeqrel/diversity/perquery
#/GW/D5data-2/khui/qrel/completeqrel/adhoc/perquery
for ((i = 101;i <= 300;i++))
do
	cat $qrel | awk -v qid=$i '$1==qid {print}' >> $outputfolder/$i
	echo $i finished
done
