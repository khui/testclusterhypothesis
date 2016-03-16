package de.mpii.docsimilarity.tasks.qe;

import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.IRScorer;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

/**
 * Select terms among all labeled documents to expand the query.
 *
 *
 *
 * @author khui
 */
public class QueryClarityTerms extends SelectQETerms {

    private static final Logger logger = Logger.getLogger(QueryClarityTerms.class);

    protected final double log2inv = 1d / Math.log(2);

    public QueryClarityTerms(Path termdfpath, Path queryPath, int qid, int docnumInCol, long termnumInCol, CleanContentTxt inclean) throws FileNotFoundException {
        super(termdfpath.toString(), queryPath.toString(), qid, docnumInCol, termnumInCol, inclean);
    }

    /**
     * sort the terms from all documents for a query according to their weight:
     * tfidf etc...
     *
     * @param qid
     * @param webpages
     * @param numOfTerms2Select
     * @return
     */
    @Override
    public TIntDoubleMap getQueryRelatedTerms(int qid, Map<String, Document> webpages, int numOfTerms2Select) {
        TIntDoubleMap qeTermWeight = new TIntDoubleHashMap();
        // consider the tf*idf as weight for each term in all documents for a query
        double topicalitysum = 1d;
        TIntDoubleMap pTQ = new TIntDoubleHashMap();
        TIntIntMap tidDocnum = new TIntIntHashMap();
        double pQDNorm = 0;
        // pre-processing
        for (Document webpage : webpages.values()) {
            List<String> docterms = cleanpipeline.cleanTxtList(webpage, true);
            pQDNorm += computePTQ(docterms, pTQ, tidDocnum);
        }
        // compute relative distance w.r.t. global statistics
        // compute contribution of t to clarity
        // TP(t) = P(t|q) log_2{\frac{P(t|q)}{P(t|C)}}
        TIntDoubleMap topicalities = topicality(pTQ, tidDocnum, pQDNorm);
        // normalize the topicalities
        // topicalitysum = sum(topicalities.values());
        // logger.info("topicalitysum " + topicalitysum);
        // sort the terms according to the weight
        Integer[] sortedTid = sortDescendingly(topicalities);
        // return the top terms with highest weight
        for (int i = 0; i < Math.min(sortedTid.length, numOfTerms2Select); i++) {
            double topicality = topicalities.get(sortedTid[i]);
            double normTopicality = topicality / topicalitysum;
            qeTermWeight.put(sortedTid[i], normTopicality);
        }
        return qeTermWeight;
    }


    /**
     * compute relative distance w.r.t. global statistics compute contribution
     * of t to clarity TP(t) = P(t|q) log_2{\frac{P(t|q)}{P(t|C)}}
     *
     * @param pTQ
     * @param tidDocnum
     * @param pQDNorm
     * @return
     */
    protected TIntDoubleMap topicality(TIntDoubleMap pTQ, TIntIntMap tidDocnum, double pQDNorm) {
        TIntDoubleMap topicalities = new TIntDoubleHashMap();
        for (int tid : pTQ.keys()) {
            // consider the terms appear in at least two documents in the total labeled collection
            if (tidDocnum.get(tid) < 2) {
                continue;
            }
            double ptq = pTQ.get(tid) / pQDNorm;
            double ptc = (double) termidCf.get(tid) / termnumInCol;
            if (ptc > ptq) {
                continue;
            }
            double topicality = ptq * Math.log(ptq / ptc) * log2inv;
            topicalities.put(tid, topicality);
        }
        return topicalities;
    }

    /**
     * compute the query clarity refer to the methods described in following
     * papers: Dang & Croft, Term Level Search Result Diversification, SIGIR13;
     * Cronen-Townsend etc., Predicting Query Performance, SIGIR02
     *
     * P(t|q) = \sum{P(t|d) P(d|q)} ~ \sum{P(t|d)P(q|d)P(d)} ~
     * \sum{P(t|d)\prod{\P(t_q|d)}P(d)}
     *
     *
     * @param docterms
     * @param tidPtd
     * @param tidDocnum
     * @return 
     */
    protected double computePTQ(List<String> docterms, TIntDoubleMap tidPtd, TIntIntMap tidDocnum) {
        TIntObjectMap<TIntList> doctidPoses = new TIntObjectHashMap<>();
        for (int pos = 0; pos < docterms.size(); pos++) {
            String term = docterms.get(pos);
            if (term.length() > GlobalConstants.MAX_TOKEN_LENGTH) {
                continue;
            }
            if (!termTermid.containsKey(term)) {
                continue;
            }
            int tid = termTermid.get(term);
            if (!doctidPoses.containsKey(tid)) {
                doctidPoses.put(tid, new TIntArrayList());
            }
            doctidPoses.get(tid).add(pos);
        }
        TIntDoubleMap tidPtdDoc = new TIntDoubleHashMap();
        double pQD = 1;
        for (int tid : doctidPoses.keys()) {
            // compute weight for each terms
            int tf = doctidPoses.get(tid).size();
            int doclen = docterms.size();
            long cf = termidCf.get(tid);
            // compute p(t|d) p(q|d)
            // compute probability with Dirichlet smooth
            double pTD = IRScorer.lmdProb(tf, doclen, cf, termnumInCol);
            tidPtdDoc.put(tid, pTD);
            // likelyhood of P(Q|D) 
            if (queryTermids.contains(tid)) {
                pQD *= pTD;
            }
        }
        for (int tid : tidPtdDoc.keys()) {
            double pTD = tidPtdDoc.get(tid);
            pTD *= pQD;
            // rewrite to termPtd
            tidPtd.adjustOrPutValue(tid, pTD, pTD);
            tidDocnum.adjustOrPutValue(tid, 1, 1);
        }
        return pQD;
    }

}
