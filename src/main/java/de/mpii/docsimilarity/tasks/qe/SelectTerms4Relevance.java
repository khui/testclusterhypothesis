package de.mpii.docsimilarity.tasks.qe;

import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.IRScorer;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

/**
 * for each term we compute the ratio between its tfidf in the relevant document
 * subset and in all the returned documents
 *
 * @author khui
 */
public class SelectTerms4Relevance extends SelectQETerms {

    private static final Logger logger = Logger.getLogger(SelectTerms4Relevance.class);

    private final Map<String, TIntList> cwidJudge;

    public SelectTerms4Relevance(String termdfpath, String queryPath, int qid, int docnumInCol, long termnumInCol, Map<String, TIntList> cwidJudge, CleanContentTxt inclean) throws FileNotFoundException {
        super(termdfpath, queryPath, qid, docnumInCol, termnumInCol, inclean);
        this.cwidJudge = cwidJudge;
    }

    @Override
    public TIntDoubleMap getQueryRelatedTerms(int qid, Map<String, Document> webpages, int numOfTerms2Select) {
        TIntDoubleMap tidTfidfD = new TIntDoubleHashMap();
        TIntDoubleMap tidTfidfR = new TIntDoubleHashMap();
        TIntDoubleMap tidRatio = new TIntDoubleHashMap();
        TIntIntMap tidDf = new TIntIntHashMap();
        TIntDoubleMap tidIdf = new TIntDoubleHashMap();
        double idf;
        for (String cwid : webpages.keySet()) {
            if (!cwidJudge.containsKey(cwid)) {
                continue;
            }
            List<String> docterms = cleanpipeline.cleanTxtList(webpages.get(cwid), true);
            tidIdf.clear();
            for (String term : docterms) {
                if (term.length() > GlobalConstants.MAX_TOKEN_LENGTH) {
                    logger.error(term + " is too long to be included");
                    continue;
                }
                if (!termTermid.containsKey(term)) {
                    continue;
                }
                int tid = termTermid.get(term);
                if (tidIdf.containsKey(tid)) {
                    idf = tidIdf.get(tid);
                } else {
                    idf = IRScorer.idf(termidDf.get(tid), docnumInCol);
                    tidDf.adjustOrPutValue(tid, 1, 1);
                    tidIdf.put(tid, idf);
                }
                tidTfidfD.adjustOrPutValue(tid, idf, idf);
                if (cwidJudge.get(cwid).contains(1)) {
                    tidTfidfR.adjustOrPutValue(tid, idf, idf);
                }
            }
        }
        for (int tid : tidDf.keys()) {
            if (tidDf.get(tid) < 3 || (!tidTfidfR.containsKey(tid))) {
                continue;
            }
            double relTfidf = tidTfidfR.get(tid);
            double totalTfidf = tidTfidfD.get(tid);
            tidRatio.put(tid, relTfidf / totalTfidf);
        }
        return tidRatio;
    }

}
