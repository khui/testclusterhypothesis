package de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec;

import de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec.UniqQDRecord.QueryDocTerms;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TFloatList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.TObjectDoubleMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer;
import org.nd4j.linalg.api.ndarray.INDArray;

/**
 *
 * @author khui
 */
public abstract class WordV2DocV implements Callable<Map<String, float[]>> {

    private static final Logger logger = Logger.getLogger(WordV2DocV.class);

    protected static WordVectors w2v = null;

    protected final List<QueryDocTerms> querydocterms;

    protected final int topN;

    public WordV2DocV(List<QueryDocTerms> querydocterms, int topN) throws IOException {
        if (w2v == null) {
            loadW2v();
        }
        this.topN = topN;
        this.querydocterms = querydocterms;
    }

    protected abstract float[] terms2docvector(List<String> terms, int topN);

    protected float[] avgTermVector(List<INDArray> terms, int topN) {
        float[] docvector = null;
        int addedTermNum = 0;
        for (INDArray wordvec : terms) {
            if (docvector == null) {
                docvector = new float[wordvec.length()];
            }
            weightMultiply(docvector, wordvec, 1d / Math.min(terms.size(), topN));
            addedTermNum++;
            if (addedTermNum >= topN) {
                break;
            }
        }
        if (docvector != null) {
            return docvector;
        } else {
            return null;
        }
    }

    protected float[] concatTermVector(List<INDArray> terms, int topN) {
        TFloatList docvector = new TFloatArrayList();
        int addedTermNum = 0;
        for (INDArray term : terms) {
            for (int i = 0; i < term.length(); i++) {
                docvector.add(term.getFloat(i));
            }
            addedTermNum++;
            if (addedTermNum >= topN) {
                break;
            }
        }
        return docvector.toArray();
    }

    protected float[] weightedAvgTermVector(List<INDArray> terms, TDoubleList weights, int topN) {
        float[] docvector = null;
        INDArray wordvec;
        int addedTermNum = 0;
        Integer[] sortedIdx = sortIdx(weights);
        for (int idx : sortedIdx) {
            wordvec = terms.get(idx);
            if (docvector == null) {
                docvector = new float[wordvec.length()];
            }
            weightMultiply(docvector, wordvec, weights.get(idx));
            addedTermNum++;
            if (addedTermNum >= topN) {
                break;
            }
        }
        if (docvector != null) {
            return docvector;
        } else {
            return null;
        }
    }

    protected void weightMultiply(float[] docvector, INDArray wvectorIND, double weight) {
        for (int i = 0; i < docvector.length; i++) {
            docvector[i] += weight * wvectorIND.getFloat(i);
        }
    }

    protected void convert2wordvecs(TObjectDoubleMap<String> termWeight, List<INDArray> vectors, TDoubleList weights) {
        for (String term : termWeight.keySet()) {
            if (w2v.hasWord(term)) {
                vectors.add(w2v.getWordVectorMatrix(term));
                if (weights != null) {
                    weights.add(termWeight.get(term));
                }
            }
        }
    }

    protected void convert2wordvecs(List<String> termWeight, List<INDArray> vectors) {
        for (String term : termWeight) {
            if (w2v.hasWord(term)) {
                vectors.add(w2v.getWordVectorMatrix(term));
            }
        }
    }

    protected Integer[] sortIdx(final TDoubleList array) {
        Integer[] idxes = new Integer[array.size()];
        for (int i = 0; i < array.size(); i++) {
            idxes[i] = i;
        }
        Arrays.sort(idxes, new Comparator<Integer>() {
            // sort in descending order
            @Override
            public int compare(Integer idx1, Integer idx2) {
                if (array.get(idx1) > array.get(idx2)) {
                    return -1;
                } else if (array.get(idx1) < array.get(idx2)) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        return idxes;
    }

    /**
     * Doc term use unified topn to fetch topn terms, in computing doc vector,
     * we use different topK for different method. Thus the unified topn should
     * be replaced with topK in the method id
     */
    private String doctermid2docvecid(String doctermid) {
        String[] params = doctermid.split("-");
        if (params.length != 3) {
            logger.error("doctermid is in wrong format: " + doctermid);
        }
        String docvecid = params[0] + "-" + params[1] + "-" + topN;
        return docvecid;
    }

    @Override
    public Map<String, float[]> call() throws Exception {
        Map<String, float[]> qidcwidDocvector = new HashMap<>();
        int nulldocvecCount = 0;
        int qid = -1;
        try {
            for (QueryDocTerms qdp : querydocterms) {
                String veckey = qdp.getKey();
                if (qid < 0) {
                    qid = qdp.qid;

                }
                List<String> tokenizedTerms = tokenizeTerms(qdp.getTokenStr());
                float[] docvec = terms2docvector(tokenizedTerms, topN);
                if (docvec != null) {
                    qidcwidDocvector.put(veckey, docvec);
                } else {
                    nulldocvecCount++;
                }
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }

        if (nulldocvecCount > 5) {
            logger.warn(qid + " has null doc vector: " + nulldocvecCount + " out of " + querydocterms.size());
        }
        return qidcwidDocvector;
    }

    protected List<String> tokenizeTerms(String docterms) {
        List<String> tokenizedTerms = Arrays.asList(docterms.split(" "));
                //new ArrayList<>();
        /*Tokenizer tokenizer = w2v.
                //.getTokenizerFactory().create(docterms);
        while (tokenizer.hasMoreTokens()) {
            tokenizedTerms.add(tokenizer.nextToken());
        }*/
        return tokenizedTerms;
    }

    public static void loadW2v() throws IOException {
        // read in the offline trained word vector
        String googlebin = "/GW/D5data-2/khui/w2vpretrained/gzfiles/GoogleNews-vectors-negative300.bin.gz";
        long startTime = System.currentTimeMillis();
        w2v = WordVectorSerializer.loadGoogleModel(new File(googlebin), true);
        logger.info("load the Google News word2vec successfully in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    }
}
