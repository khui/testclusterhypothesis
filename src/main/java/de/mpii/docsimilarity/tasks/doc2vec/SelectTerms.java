package de.mpii.docsimilarity.tasks.doc2vec;

import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.IRScorer;
import de.mpii.docsimilarity.mr.utils.io.ReadQuery;
import de.mpii.docsimilarity.mr.utils.io.ReadTermTermidMap;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.deeplearning4j.util.MathUtils;

/**
 *
 * @author khui
 */
public class SelectTerms {

    private static final Logger logger = Logger.getLogger(SelectTerms.class);

    protected final DecimalFormat dfFormater = new DecimalFormat("#.00000000");

    // static for the sake of expensive load in operation for the  CleanContentTxt class
    protected static CleanContentTxt cleanpipeline = null;
    protected final TObjectIntMap<String> termTermid = new TObjectIntHashMap<>();
    protected final TIntObjectMap<String> termidTerm = new TIntObjectHashMap<>();
    protected final TIntObjectMap<Set<String>> termidRawterms = new TIntObjectHashMap<>();
    protected final TIntIntMap termidDf = new TIntIntHashMap();
    protected final TIntLongMap termidCf = new TIntLongHashMap();
    protected final boolean toStem;
    protected final TIntList queryTermids;
    protected final int qid;
    protected final TIntDoubleMap tidIdf = new TIntDoubleHashMap();

    protected SelectTerms(String termdfpath, String queryPath, int qid, CleanContentTxt inclean, boolean toStem) throws FileNotFoundException {
        this(termdfpath, queryPath, qid, toStem);
        this.cleanpipeline = inclean;
    }

    protected SelectTerms(String termdfpath, String queryPath, int qid, boolean toStem) throws FileNotFoundException {
        ReadTermTermidMap.readTermDfCf(termdfpath, termTermid, termidTerm, termidDf, termidCf, termidRawterms, toStem);
        if (queryPath != null) {
            this.queryTermids = ReadQuery.readQuery(queryPath, qid, termTermid, false);
        } else {
            this.queryTermids = null;
        }
        this.toStem = toStem;
        this.qid = qid;
    }

    public static Integer[] sortDescendingly(final TIntDoubleMap termweight) {
        int[] doctids = termweight.keys();
        Integer[] doctidsBoxed = ArrayUtils.toObject(doctids);
        // sort the term according to the weight score
        // in descending order
        Arrays.sort(doctidsBoxed, new Comparator<Integer>() {
            @Override
            public int compare(Integer tid1, Integer tid2) {
                double prox1 = termweight.get(tid1);
                double prox2 = termweight.get(tid2);
                if (prox1 > prox2) {
                    return -1;
                } else if (prox1 < prox2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        return doctidsBoxed;
    }

    protected String toString(String docid, int num2select, Integer[] doctidsBoxed, TIntDoubleMap termweight) {
        StringBuilder sb = new StringBuilder();
        sb.append(docid).append(" ");
        if (toStem) {
            for (int i = 0; i < Math.min(num2select, doctidsBoxed.length); i++) {
                String[] rawTerms = termidRawterms.get(doctidsBoxed[i]).toArray(new String[0]);
                for (String rterm : rawTerms) {
                    sb.append(rterm).append(" ");
                }
            }
        } else {
            for (int i = 0; i < Math.min(num2select, doctidsBoxed.length); i++) {
                sb.append(termidTerm.get(doctidsBoxed[i])).append(":").append(dfFormater.format(termweight.get(doctidsBoxed[i]))).append(" ");
            }
        }
        return sb.toString().trim();
    }

    /**
     * no matter the weight is nonnegative or not, the weights are always sum up
     * to normalize
     *
     * @param termweight
     * @return
     */
    protected double norm2one(TIntDoubleMap termweight) {
        double totalDocWeight = sum(termweight.values());
        if (totalDocWeight == 0) {
            logger.error("totalDocWeight is 0, we dont normalize, directly return");
            return totalDocWeight;
        }
        for (int tid : termweight.keys()) {
            double normTfClarity = termweight.get(tid) / totalDocWeight;
            termweight.put(tid, normTfClarity);
        }
        return totalDocWeight;
    }

    protected static void minmaxNorm(TIntDoubleMap termweight) {
        if (termweight.values().length == 0) {
            logger.error("zero size in values, we dont normalize, directly return");
            return;
        }
        double min = MathUtils.min(termweight.values());
        double max = MathUtils.max(termweight.values());
        double diff = max - min;
        if (diff == 0) {
            logger.error("max min are the same, we dont normalize, directly return");
            return;
        }
        for (int tid : termweight.keys()) {
            double normTfClarity = (termweight.get(tid) - min) / diff;
            termweight.put(tid, normTfClarity);
        }
    }

    protected static void minmaxNorm(TObjectDoubleMap termweight) {
        if (termweight.values().length == 0) {
            logger.error("zero size in values, we dont normalize, directly return");
            return;
        }
        double min = MathUtils.min(termweight.values());
        double max = MathUtils.max(termweight.values());
        double diff = max - min;
        if (diff == 0) {
            logger.error("max min are the same, we dont normalize, directly return");
            return;
        }
        for (Object tid : termweight.keys()) {
            double normTfClarity = (termweight.get(tid) - min) / diff;
            termweight.put(tid, normTfClarity);
        }
    }

    public static double sum(double[] vals) {
        double sum = 0;
        for (double val : vals) {
            sum += val;
        }
        return sum;
    }

    protected TIntDoubleMap statTfidf(List<String> terms, int totaldocnum) {
        TIntDoubleMap tidTfidf = new TIntDoubleHashMap();
        double idf;
        double doclen = terms.size();
        for (String term : terms) {
            if (term.length() > GlobalConstants.MAX_TOKEN_LENGTH) {
                continue;
            }
            if (!termTermid.containsKey(term)) {
                continue;
            }
            int tid = termTermid.get(term);
            if (!tidIdf.containsKey(tid)) {
                double df = termidDf.get(tid);
                idf = IRScorer.idf(df, totaldocnum);
                tidIdf.put(tid, idf);
            } else {
                idf = tidIdf.get(tid);
            }
            tidTfidf.adjustOrPutValue(tid, idf / doclen, idf / doclen);
        }
        return tidTfidf;
    }

    protected void getTopKTerms(int did, TIntDoubleMap tidWeightDoc, TIntObjectMap<TIntDoubleMap> didTidTfidf, int topk) {
        Integer[] tids = sortDescendingly(tidWeightDoc);
        didTidTfidf.put(did, new TIntDoubleHashMap());
        for (int tid : tids) {
            didTidTfidf.get(did).adjustOrPutValue(tid, tidWeightDoc.get(tid), tidWeightDoc.get(tid));
            topk--;
            if (topk <= 0) {
                break;
            }
        }

    }

}
