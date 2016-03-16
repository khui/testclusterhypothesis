package de.mpii.docsimilarity.tasks.qe;

import com.google.common.primitives.Doubles;
import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.IRScorer;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

/**
 * Select terms among all labeled relevant documents to expand the query.
 *
 *
 *
 * @author khui
 */
public class DiscriminatingTerms extends SelectQETerms {

    private static final Logger logger = Logger.getLogger(DiscriminatingTerms.class);

    private static class Constants {

        static String TFIDF = "tfidf";
        static String DFIDF = "dfidf";
        // term probability in relevant document subset / term probability in complete document collection
        static String PROBRATIO = "probabilityRatio";

    }

    public DiscriminatingTerms(Path termdfpath, Path queryPath, int qid, int docnumInCol, long termnumInCol, CleanContentTxt inclean) throws FileNotFoundException {
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
        TIntDoubleMap termidWeightRaw = new TIntDoubleHashMap();
        // pre-processing
        for (Document webpage : webpages.values()) {
            List<String> docterms = cleanpipeline.cleanTxtList(webpage, true);
            addDocument(docterms, termidWeightRaw, Constants.PROBRATIO);
        }
        // compute relative distance w.r.t. global statistics
        TIntDoubleMap termidWeightRelDist = new TIntDoubleHashMap();
        for (int termid : termidWeightRaw.keys()) {
            // filter the the rare/miss-spell terms
            if (termidDf.get(termid) - webpages.size() > GlobalConstants.DF_MIN2FILTER) {
                // we dont divide by number of documents, due to it will not incluence the order
                double avgtfnweight = termidWeightRaw.get(termid);
                double globalprob = (double) termidCf.get(termid) / termnumInCol;
                termidWeightRelDist.put(termid, avgtfnweight / globalprob);
            }
        }
        // normalize the weight
        double[] weights = termidWeightRelDist.values();
        double minweight = Doubles.min(weights);
        double maxweight = Doubles.max(weights);
        for (int termid : termidWeightRelDist.keys()) {
            double weight = termidWeightRelDist.get(termid);
            double normweight = (weight - minweight) / (maxweight - minweight);
            termidWeightRelDist.put(termid, normweight);
        }
        // sort the terms according to the weight
        Integer[] sortedTid = sortDescendingly(termidWeightRelDist);
        // return the top terms with highest weight
        for (int i = 0; i < Math.min(sortedTid.length, numOfTerms2Select); i++) {
            qeTermWeight.put(sortedTid[i], termidWeightRelDist.get(sortedTid[i]));
        }
        return qeTermWeight;
    }

    private void addDocument(List<String> docterms, TIntDoubleMap termidWeight, String weightname) {
        TIntObjectMap<TIntList> doctidPoses = new TIntObjectHashMap<>();
        for (int pos = 0; pos < docterms.size(); pos++) {
            //String term = CleanContentTxt.porterStemming(docterms.get(pos));
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
        double termWeight = 0;
        for (int tid : doctidPoses.keys()) {
            // compute weight for each terms
            int df = termidDf.get(tid);
            int tf = doctidPoses.get(tid).size();
            int doclen = docterms.size();
            long cf = termidCf.get(tid);
            if (weightname.equals(Constants.TFIDF)) {
                termWeight = IRScorer.tfIdf(tf, df, doclen, docnumInCol);
            } else if (weightname.equals(Constants.DFIDF)) {
                termWeight = IRScorer.idf(df, docnumInCol);
            } else if (weightname.equals(Constants.PROBRATIO)) {
                termWeight = IRScorer.lmdProb(tf, doclen, cf, termnumInCol);
            } else {
                logger.error("unrecgonized weightname: " + weightname);
            }
            termidWeight.adjustOrPutValue(tid, termWeight, termWeight);
        }
    }

}
