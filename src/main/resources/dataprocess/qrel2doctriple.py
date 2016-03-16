# given a qrel as input, we generate doc triple for each query as following:
# (doc_1, doc_2, doc_3), where doc_1 and doc_2 are relevant and doc_3 is irrelevant
# here we treat diversity qrel as adhoc: only consider the first subtopic, or we directly use adhoc
# qrel

import sys, os, getopt


opts, args = getopt.getopt(sys.argv[1:],"q:o:")
for opt, arg in opts:
    if opt == '-q':
        qrelf=str(arg)
    if opt == '-o':
        outdir=str(arg)

qidrel=dict()
qidnrel=dict()
for line in open(qrelf):
    cols = line.split(' ')
    qid = int(cols[0])
    jud = int(cols[3])
    cwid = str(cols[2])
    if jud > 0:
        if qid not in qidrel:
            qidrel[qid] = list()
        if cwid in qidrel[qid]:
            print "error: rel doc duplicated", cwid
            continue
        qidrel[qid].append(cwid)
    if jud == 0:
        if qid not in qidnrel:
            qidnrel[qid] = list()
        if cwid in qidnrel[qid]:
            print "error: non-rel doc duplicated", cwid
            continue
        qidnrel[qid].append(cwid)

print 'finished in readin qrel', qrelf

for qid in sorted(qidrel.keys()):
    outf = open(outdir + "/" + str(qid), 'w')
    rels = qidrel[qid]
    nrels = qidnrel[qid]
    triples = list()
    for i in range(len(rels)):
        for j in range(i+1, len(rels)):
            for nrcwid in nrels:
                line = ' '.join([str(qid), rels[i], rels[j], nrcwid])
                triples.append(line)
    outf.write('\n'.join(triples))
    outf.close()
    print 'output ', len(triples), 'lines for', qid







