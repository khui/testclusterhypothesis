import os, sys, getopt
import os.path
import numpy as np
import multiprocessing
from random import shuffle
import gensim
from gensim.models import Word2Vec
# for timing
from contextlib import contextmanager
from timeit import default_timer
import time 
import datetime

opts,args=getopt.getopt(sys.argv[1:],'i:d:q:o:c:t:')
for opt,arg in opts:
    if opt in ('-d','--datadir'):
        datadir=str(arg)
    if opt in ('-i','--lineno2cwid'):
        cwiddir=str(arg)
    if opt in ('-q','--qid'):
        qid=str(arg)
    if opt in ('-o','--outdir'):
        outdir=str(arg)

googlenewscorpus="/GW/D5data-2/khui/w2vpretrained/GoogleNews-vectors-negative300.bin"
questionwordbenchmark="/GW/D5data-2/khui/w2vpretrained/questions-words.txt"
qid=101
proximityThres=5
datadir="/GW/D5data-2/khui/cw-docvector-termdf/ExtractCwdocs/alldocs/doccontent"
cwiddir="/GW/D5data-2/khui/cw-docvector-termdf/ExtractCwdocs/alldocs/idxcwid"
term2qtermdist="/GW/D5data-2/khui/cw-docvector-termdf/ExtractCwdocs/term2querytermdists"
outdir="/scratch/GW/pool0/khui/doc2vec/wordvectors"
# each line corresponds to one document
datafile=datadir + "/" + str(qid) + "/part-00000"
lineno2cwidf=cwiddir + "/" + str(qid) + "/part-00000"
alldocs = []  # will hold all docs in original order
alltags = []
with open(datafile) as data, open(lineno2cwidf) as cwid:
    for line_no, line in zip(cwid,data):
        docid=line_no.rstrip().split(" ")[1]
        words = gensim.utils.to_unicode(line.rstrip(), errors='strict').split()
        alldocs.append(words)
        alltags.append(docid)
doc_list = alldocs[:]  # for reshuffling per pass

print('Input %d docs for query %s ' % (len(doc_list), qid))

cores = multiprocessing.cpu_count()
assert gensim.models.doc2vec.FAST_VERSION > -1, "this will be painfully slow otherwise"



#load_word2vec_format(googlenewscorpus, binary=True)
model = Word2Vec(size=300, window=5, min_count=5, workers=cores)
model.intersect_word2vec_format(googlenewscorpus, binary=True)

model.build_vocab(alldocs)

@contextmanager
def elapsed_timer():
    start = default_timer()
    elapser = lambda: default_timer() - start
    yield lambda: elapser()
    end = default_timer()
    elapser = lambda: end-start

def cwidvec2str(cwid, vec):
    line=list()
    line.append(cwid)
    for idx, val in enumerate(vec):
        line.append(str(idx) + ":" + '%.6f'%val)
    return ' '.join(line)


alpha, min_alpha, passes = (0.025, 0.001, 20)
alpha_delta = (alpha - min_alpha) / passes

print("START query %s at %s" % (qid, datetime.datetime.now()))

for epoch in range(passes):
    shuffle(doc_list)  # shuffling gets best results
    duration = 'na'
    model.alpha, model.min_alpha = alpha, alpha
    with elapsed_timer() as elapsed:
        model.train(doc_list)
        duration = '%.1f' % elapsed()
        #print("%i passes : %s %ss" % (epoch + 1, name, duration))
    if (epoch + 1) % 5 == 0:
        print('%s: completed pass %i at alpha %f' % (qid, epoch + 1, alpha))
    alpha -= alpha_delta

print "loaded ", len(model.vocab)


query="ritz carlton".split()
ms=model.most_similar(positive=query,topn=20)
print ms



#for (name, train_model),sname in zip(models_by_name.items(),manualModelName):
#    outdirqid=outdir +"/"+sname
#    if not os.path.exists(outdirqid):
#        os.makedirs(outdirqid)
#    train_model.save(outdirqid+"/"+str(qid))
#    model = Doc2Vec.load(outdirqid+"/"+str(qid))
#print("Finished query %s at %s" % (qid, str(datetime.datetime.now())))
