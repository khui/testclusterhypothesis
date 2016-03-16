import os, sys, getopt
import os.path
import numpy as np
import multiprocessing
from random import shuffle
from collections import namedtuple, OrderedDict, defaultdict
import gensim
from gensim.models import Doc2Vec
import gensim.models.doc2vec
from gensim.test.test_doc2vec import ConcatenatedDoc2Vec
# for timing
from contextlib import contextmanager
from timeit import default_timer
import time 
import datetime


opts,args=getopt.getopt(sys.argv[1:],'i:')
for opt,arg in opts:
    if opt in ('-i','--inputfile'):
        datafile=str(arg)


qid=datafile.split('/')[-1].split('-')[0]
outdir=""


CWDocument = namedtuple('CWDocument', 'words tags')
alldocs = []  # will hold all docs in original order
alltags = []
with open(datafile) as alldata:
    for line_no, line in enumerate(alldata):
        cols=line.rstrip().split("\t")
        cwidTws=cols[1].split(" ")
        docid=cwidTws[0]
        termWeights=cwidTws[1:]
        alltags.append(docid)
        terms=list()
        for tw in termWeights:
            terms.append(tw.split(":")[0])
        words = gensim.utils.to_unicode(' '.join(terms), errors='ignore').split()
        tags = [docid] # `tags = [tokens[0]]` would also work at extra memory cost
        alldocs.append(CWDocument(words, tags))

doc_list = alldocs[:]  # for reshuffling per pass

print('Input %d docs for query %s ' % (len(doc_list), qid))



cores = multiprocessing.cpu_count()
assert gensim.models.doc2vec.FAST_VERSION > -1, "this will be painfully slow otherwise"

simple_models = [
    # PV-DM w/concatenation - window=5 (both sides) approximates paper's 10-word total window size
    Doc2Vec(dm=1, dm_concat=1, size=100, window=5, negative=5, hs=0, min_count=2, workers=cores),
    # PV-DBOW 
    Doc2Vec(dm=0, size=100, negative=5, hs=0, min_count=2, workers=cores),
    # PV-DM w/average
    Doc2Vec(dm=1, dm_mean=1, size=100, window=10, negative=5, hs=0, min_count=2, workers=cores),
]

# speed setup by sharing results of 1st model's vocabulary scan
simple_models[0].build_vocab(alldocs)  # PV-DM/concat requires one special NULL word so it serves as template
#print(simple_models[0])
for model in simple_models[1:]:
    model.reset_from(simple_models[0])
    #print(model)

models_by_name = OrderedDict((str(model), model) for model in simple_models)

models_by_name['dbow+dmm'] = ConcatenatedDoc2Vec([simple_models[1], simple_models[2]])
models_by_name['dbow+dmc'] = ConcatenatedDoc2Vec([simple_models[1], simple_models[0]])

manualModelName=['dmc','dbow','dmm','dbow+dmm','dbow+dmc']

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
    for name, train_model in models_by_name.items():
        # train
        duration = 'na'
        train_model.alpha, train_model.min_alpha = alpha, alpha
        with elapsed_timer() as elapsed:
            train_model.train(doc_list)
            duration = '%.1f' % elapsed()
        #print("%i passes : %s %ss" % (epoch + 1, name, duration))
    if (epoch + 1) % 5 == 0:
        print('%s: completed pass %i at alpha %f' % (qid, epoch + 1, alpha))
    alpha -= alpha_delta


i=0
for name, model in models_by_name.items():
    lines=list()
    subdir = outdir + "/" + manualModelName[i]
    if not os.path.exists(subdir):
        os.makedirs(subdir)
    outf = open(subdir + "/" + qid,'w')
    i += 1
    for cwid in alltags:
        lines.append(cwidvec2str(cwid, model.docvecs[cwid]))
    outf.write('\n'.join(lines))
    outf.close()

print("Finished query %s at %s" % (qid, str(datetime.datetime.now())))
