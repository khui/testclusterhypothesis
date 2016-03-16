# evaluate the computed similarity.
# given triple (doc_1, doc_2, doc_3) and the corresponding qid,
# where doc_1 and doc_2 are relevant and doc_3 is irrelevant
# we also read in the pre-computed docpair similarity
# if similarity(doc_1, doc_2) > mean(simlarity(doc_1, doc_3), similarity(doc_2, doc_3)) triple is correct
# otherwise, triple is incorrect
# output the number of triples that is correct, incorrect and have incomplete similarity information

import sys, os, getopt
import numpy as np


opts, args = getopt.getopt(sys.argv[1:],"t:o:s:")
for opt, arg in opts:
    if opt == '-t':
        triplef=str(arg)
    if opt == '-s':
        similarityf=str(arg)
    if opt == '-o':
        outfstr=str(arg)

def generateDocPairKey(docid0, docid1):
    sorteddocidpair =  [docid0.rstrip('\n'), docid1.rstrip('\n')]
    sorteddocidpair.sort()
    sortedtuple = tuple(sorteddocidpair)
    return sortedtuple


qidrelrel=dict()
qidrelir=dict()
for line in open(similarityf):
    cols = line.split(' ')
    qid = int(cols[0])
    # the experiments only involve 11-14 trec web track
    if qid <= 100:
        continue
    cwidl = str(cols[1])
    cwidr = str(cols[2])
    # 1: irrel-rel; 2: rel-rel
    reltype = int(cols[3])
    similarity = float(cols[4])
    pairkey=generateDocPairKey(cwidl, cwidr)
    if reltype == 2:
        if qid not in qidrelrel:
            qidrelrel[qid] = dict()
        if pairkey in qidrelrel[qid]:
            print "error: rel-rel pair duplicated", qid, pairkey
            continue
        qidrelrel[qid][pairkey] = similarity
    if reltype == 1:
        if qid not in qidrelir:
            qidrelir[qid] = dict()
        if pairkey in qidrelir[qid]:
            print "error: rel-nonrel pair duplicated", qid, pairkey
            continue
        qidrelir[qid][pairkey] = similarity
print 'finished in readin pairwise similarity', similarityf

incomplete_simi_rr = dict()
incomplete_simi_rir = dict()
correctcount = dict()
incorrectcount = dict()

for line in open(triplef):
    cols = line.split(' ')
    qid = int(cols[0])

    if qid not in correctcount:
        correctcount[qid] = 0
        incorrectcount[qid] = 0
        incomplete_simi_rr[qid] = 0
        incomplete_simi_rir[qid] = 0
    cwid1 = str(cols[1])
    cwid2 = str(cols[2])
    cwid3 = str(cols[3])
    relrel = generateDocPairKey(cwid1, cwid2)
    relir0 = generateDocPairKey(cwid1, cwid3)
    relir1 = generateDocPairKey(cwid2, cwid3)
    if relrel in qidrelrel[qid]:
        rr_simi = qidrelrel[qid][relrel]
    else:
        incomplete_simi_rr[qid] += 1
        continue
    if relir0 in qidrelir[qid]:
        rir_simi0 = qidrelir[qid][relir0]
    else:
        print qid, relir0
        incomplete_simi_rir[qid] += 1
        continue
    if relir1 in qidrelir[qid]:
        rir_simi1 = qidrelir[qid][relir1]
    else:
        print qid, relir1
        incomplete_simi_rir[qid] += 1
        continue
    
    ave_simi = np.average([rir_simi0, rir_simi1])
    if ave_simi < rr_simi:
        correctcount[qid] += 1
    else:
        incorrectcount[qid] += 1
    
print 'Finished in reading in triple information' 

outf = open(outfstr, 'w')
lines = list()
line = ' '.join(['qid', '#correct','#incorrect','#rel-rel-missing', '#rel-irrel-missing','ratio=rel/(rel+irrel)'])
lines.append(line)
for qid in sorted(correctcount.keys()):
    ratio = correctcount[qid] / float(correctcount[qid] + incorrectcount[qid])
    line = ' '.join([str(qid), str(correctcount[qid]), \
            str(incorrectcount[qid]), \
            str(incomplete_simi_rr[qid]), \
            str(incomplete_simi_rir[qid]), \
            '%.4f'%ratio])
    lines.append(line)
    outf.write(line + '\n')
    outf.flush()
outf.close()
print 'output ', len(lines), 'lines'







