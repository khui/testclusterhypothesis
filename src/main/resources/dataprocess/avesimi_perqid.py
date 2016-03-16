#!/usr/bin/env python
import numpy as np
import pylab as P
from scipy.stats import norm
import operator
from scipy import stats


#cwid-{cwid:similarity}
def readinsimi(simifile):
    qidCwidSimis=dict()
    for line in open(simifile):
        cols=line.split(' ')
        qid=int(cols[0])
        cwidl=cols[1]
        cwidr=cols[2]
        simi=float(cols[-1])
        if qid not in qidCwidSimis:
            qidCwidSimis[qid] = dict()
        if cwidl not in qidCwidSimis[qid]:
            qidCwidSimis[qid][cwidl] = dict()
        if cwidr not in qidCwidSimis[qid]:
            qidCwidSimis[qid][cwidr] = dict()
        if cwidr not in qidCwidSimis[qid][cwidl]:
            qidCwidSimis[qid][cwidl][cwidr]=simi
        if cwidl not in qidCwidSimis[qid][cwidr]:
            qidCwidSimis[qid][cwidr][cwidl]=simi
    return qidCwidSimis


def computeaverage(qidsimilarity):
    qidsimi=dict()
    qidavesimi=dict()
    alen = qidsimilarity.shape[0]
    for index in range(alen):
        qid = qidsimilarity[index, 0]
        similarity = float(qidsimilarity[index, 1])
        if qid not in qidsimi:
            qidsimi[qid] = list()
        qidsimi[qid].append(similarity)
    for qid in qidsimi:
        qidavesimi[qid] = np.mean(qidsimi[qid])
    return qidavesimi, qidsimi

def plotpoint2compare(qidrrsimi_ave, qidirsimi_ave):
    P.figure()
    x=np.array(qidrrsimi_ave.keys())
    y=np.array(qidrrsimi_ave.values())
    P.plot(x, y,'s', linewidth=2, color='red', label='rel-rel')
    x=np.array(qidirsimi_ave.keys())
    y=np.array(qidirsimi_ave.values())
    P.plot(x, y, 'x', linewidth=2, color='blue', label='rel-irrel')
    P.savefig('rr-ir-similarity-perqid.pdf',format='pdf')
    P.show()


qidCwidSimirr=readinsimi("/GW/D5data-2/khui/cw-docvector-termdf/similarity/tid-tfidf-sparsevec/rel-rel")
qidCwidSimiir=readinsimi("/GW/D5data-2/khui/cw-docvector-termdf/similarity/tid-tfidf-sparsevec/rel-irrel")
qidTotalCount=dict()

#absolute statistics
thread4rr=0.3 # stat non-similar pair among rel-rel
thread4ri=0.7 # stat similar pair among rel-irrel
qidNSrr=dict()
qidSri=dict()
for qid in sorted(qidCwidSimirr.keys()):
    if qid not in qidCwidSimiir.keys():
        continue
    if qid not in qidNSrr:
        qidNSrr[qid] = list()
        qidSri[qid] = list()
    for cwid in sorted(qidCwidSimirr[qid].keys()):
        if cwid in qidCwidSimiir[qid].keys():
            relsimi = qidCwidSimirr[qid][cwid].values()
            irrsimi = qidCwidSimiir[qid][cwid].values()
            lowsimi_relrel_percent = sum([s <= thread4rr for s in relsimi]) / float(len(relsimi))
            highsimi_relirr_percent = sum([s >= thread4ri for s in irrsimi]) / float(len(irrsimi))
            qidNSrr[qid].append(lowsimi_relrel_percent)
            qidSri[qid].append(highsimi_relirr_percent)



# relative comparison
qidNoDiffCount=dict()
for qid in sorted(qidCwidSimirr.keys()):
    if qid not in qidCwidSimiir.keys():
        continue
    if qid not in qidTotalCount:
        qidTotalCount[qid] = 0
    if qid not in qidNoDiffCount:
        qidNoDiffCount[qid] = 0
    for cwid in sorted(qidCwidSimirr[qid].keys()):
        if cwid in qidCwidSimiir[qid].keys():
            qidTotalCount[qid]+=1
            relsimi = qidCwidSimirr[qid][cwid].values()
            irrsimi = qidCwidSimiir[qid][cwid].values()
            D, p_value = stats.ttest_ind(relsimi, irrsimi, equal_var=False)
            if p_value > 0.05:
                qidNoDiffCount[qid] += 1

qidNoDiffPercent=dict()
for qid in qidNoDiffCount.keys():
    qidNoDiffPercent[qid] = qidNoDiffCount[qid]/float(qidTotalCount[qid])
sort_qidpercent = sorted(qidNoDiffPercent.items(), key=operator.itemgetter(1), reverse=True)

count=1
qidLowrr=dict()
qidHighri=dict()
print "#","count","qid","relative difference%","low similarity in rel-rel %","high similarity in rel-irr%","nodifference count","total number"
for item in sort_qidpercent:
    qid = item[0]
    ave_lowsimi_rr_percent= np.mean(qidNSrr[qid])
    ave_highsimi_ri_percent = np.mean(qidSri[qid])
    if qid not in qidLowrr:
        qidLowrr[qid]=list()
        qidHighri[qid]=list()
    qidLowrr[qid].append(ave_lowsimi_rr_percent)
    qidHighri[qid].append(ave_highsimi_ri_percent)
    print count, qid, '%.2f%%'%(item[1]*100), '%.2f%%'%(ave_lowsimi_rr_percent*100), '%.2f%%'%(ave_highsimi_ri_percent*100), \
            qidNoDiffCount[qid], qidTotalCount[qid]
    count += 1


print "average low simi for r-r 0.3",np.mean(qidLowrr.values()),"average high simi for r-ir 0.7",np.mean(qidHighri.values())


#
# The hist() function now has a lot more options
#

#
# first create a single histogram
#
#qidrr=np.loadtxt("/GW/D5data-2/khui/cw-docvector-termdf/similarity/tid-tfidf-sparsevec/qidsimilarity.rel-rel",skiprows=-1)
#qidrir=np.loadtxt("/GW/D5data-2/khui/cw-docvector-termdf/similarity/tid-tfidf-sparsevec/qidsimilarity.rel-irr",skiprows=-1)
#
#qidrrsimi_ave, qidrrsimi =computeaverage(qidrr)
#qidirsimi_ave, qidirsimi =computeaverage(qidrir)
#
##plotpoint2compare(qidrrsimi_ave, qidirsimi_ave)
#
#qids = set(qidrrsimi.keys())
#qids.intersection(qidirsimi.keys())
#
#print "qid is being considered:", len(qids)
#
#for qid in qids:
#    rr = qidrrsimi[qid]
#    ri = qidirsimi[qid]
#    #Kolmogorov-Smirnov statistic on 2 samples
#    #D, p_value = stats.ks_2samp(np.array(rr), np.array(ri))
#    #t-test for independent sample with equal variance
#    #D, p_value = stats.ttest_ind(rr, ri)
#    #D, p_value = stats.ttest_ind(rr, ri, equal_var=False)
#    if p_value > 0.05:
#        print qid, D, p_value
#


