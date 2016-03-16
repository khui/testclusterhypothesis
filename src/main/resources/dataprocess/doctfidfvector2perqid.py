import sys, os, getopt


opts, args = getopt.getopt(sys.argv[1:],"q:d:o:")
for opt, arg in opts:
    if opt=='-q':
        qrelf=str(arg)
    if opt=='-d':
        docvecf=str(arg)
    if opt=='-o':
        outdir=str(arg)


# read in qrel
# qid 0 cwid label
# {qid-[cwid]}
def readqrel(qrelf):
    qidcwids=dict()
    linecount = 0
    for line in open(qrelf):
        cols = line.split(' ')
        qid = int(cols[0])
        cwid = str(cols[2])
        if qid not in qidcwids:
            qidcwids[qid] = list()
        if cwid not in qidcwids[qid]:
            qidcwids[qid].append(cwid)
            linecount += 1
    return qidcwids, linecount

# read in docvector
# cwid term:tfidf ...
# {cwid-[line]}
def readdocvector(docvecf):
    cwidLines=dict()
    for line in open(docvecf):
        cols = line.split(' ')
        if len(cols) > 1:
            cwid = cols[0]
            cwidLines[cwid] = line
        else:
            print "wrong line:", line
    return cwidLines

qidcwids, totallinenum=readqrel(qrelf)
cwidlines=readdocvector(docvecf)
print "qidcwids from qrel totalcwids:", totallinenum
print "cwidlines from docvector", len(cwidlines)

for qid in qidcwids:
    outf = open(outdir + "/" + str(qid), 'w')
    for cwid in qidcwids[qid]:
        if cwid in cwidlines:
            outf.write(cwidlines[cwid])
        else:
            print cwid, "not in docvector file"
    outf.close()
