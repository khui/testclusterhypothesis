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
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.jsoup.nodes.Document;

/**
 *
 * @author khui
 */
public class QueryProximityClarityTerm extends QueryClarityTerms {

    private final int queryWidth;

    public QueryProximityClarityTerm(Path termdfpath, Path queryPath, int qid, int width, int docnumInCol, long termnumInCol, CleanContentTxt inclean) throws FileNotFoundException {
        super(termdfpath, queryPath, qid, docnumInCol, termnumInCol, inclean);
        this.queryWidth = width;
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
        double topicalitysum = 1d;
        TIntDoubleMap qeTermWeight = new TIntDoubleHashMap();
        // consider the tf*idf as weight for each term in all documents for a query
        TIntDoubleMap pTQAll = new TIntDoubleHashMap();
        TIntDoubleMap pTQ = new TIntDoubleHashMap();
        TIntIntMap tidDocnum = new TIntIntHashMap();
        TIntSet proximityTids = new TIntHashSet();
        double pQDNorm = 0;
        // pre-processing
        for (Document webpage : webpages.values()) {
            List<String> docterms = cleanpipeline.cleanTxtList(webpage, true);
            pQDNorm += computePTQ(docterms, proximityTids, pTQAll, tidDocnum);
        }
        for (int tid : pTQAll.keys()) {
            // only include the terms appear within the query term window
            // with given width
            if (proximityTids.contains(tid)) {
                pTQ.put(tid, pTQAll.get(tid));
            }
        }
        // compute relative distance w.r.t. global statistics
        // compute contribution of t to clarity
        // TP(t) = P(t|q) log_2{\frac{P(t|q)}{P(t|C)}}
        TIntDoubleMap topicalities = topicality(pTQ, tidDocnum, pQDNorm);
        // normalize the topicalities
        // topicalitysum = sum(topicalities.values());
        //double topicalitysum = sum(topicalities.values());
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
     * compute the query clarity refer to the methods described in following
     * papers: Dang & Croft, Term Level Search Result Diversification, SIGIR13;
     * Cronen-Townsend etc., Predicting Query Performance, SIGIR02
     *
     * P(t|q) = \sum{P(t|d) P(d|q)} ~ \sum{P(t|d)P(q|d)P(d)} ~
     * \sum{P(t|d)\prod{\P(t_q|d)}P(d)}
     *
     *
     * @param docterms
     * @param proximityTids
     * @param tidPtd
     * @param tidDocnum
     * @return
     */
    protected double computePTQ(List<String> docterms, TIntSet proximityTids, TIntDoubleMap tidPtd, TIntIntMap tidDocnum) {
        TIntObjectMap<TIntList> doctidPoses = new TIntObjectHashMap<>();
        TIntList qtidOffset = new TIntArrayList();

        for (int pos = 0; pos < docterms.size(); pos++) {
            String term = docterms.get(pos);
            if (term.length() > GlobalConstants.MAX_TOKEN_LENGTH) {
                continue;
            }
            if (!termTermid.containsKey(term)) {
                continue;
            }
            int tid = termTermid.get(term);
            if (queryTermids.contains(tid)) {
                qtidOffset.add(pos);
            }
            if (!doctidPoses.containsKey(tid)) {
                doctidPoses.put(tid, new TIntArrayList());
            }
            doctidPoses.get(tid).add(pos);
        }
        TIntDoubleMap tidPtdDoc = new TIntDoubleHashMap();
        double pQD = 1;
        for (int tid : doctidPoses.keys()) {
            boolean termInWindowWidth = false;
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
            termInWindowWidth = computeProximity(doctidPoses.get(tid), qtidOffset);
            if (termInWindowWidth) {
                proximityTids.add(tid);
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

    private boolean computeProximity(TIntList poses, TIntList qtermPos) {
        int mindiff = Integer.MAX_VALUE;
        for (int docpos : poses.toArray()) {
            for (int qpos : qtermPos.toArray()) {
                int diff = Math.abs(docpos - qpos);
                if (diff < mindiff) {
                    mindiff = diff;
                    if (mindiff <= this.queryWidth) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}
