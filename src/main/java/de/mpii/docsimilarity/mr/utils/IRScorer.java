package de.mpii.docsimilarity.mr.utils;

/**
 *
 * @author khui
 */
public class IRScorer implements java.io.Serializable{

    public static double tfIdf(double tf, double df, double doclen, int docnumInCol) {
        if (df > 0) {
            double ntf = tf / doclen;
            double idf = idf(df, docnumInCol);
            return ntf * idf;
        } else {
            return 0;
        }
    }

    public static double idf(double df, int docnumInCol) {
        double idf = Math.log(docnumInCol / df);
        return idf;
    }

    /**
     * (tf + mu * (cf/|C|)) / (dl + mu)
     *
     * @param tf
     * @param doclen
     * @param cf
     * @param collectionTerms
     * @return
     */
    public static double lmdProb(double tf, int doclen, long cf, long collectionTerms) {
        int mu = 2000;
        double prob = (tf + mu * (cf / collectionTerms)) / (doclen + mu);
        return prob;
    }

    /**
     * (tf + mu * (cf/|C|)) / (dl + mu)
     *
     * @param tf
     * @param doclen
     * @param cf
     * @param collectionTerms
     * @return
     */
    public static double lmdProb(double tf, int doclen, double cf, double collectionTerms) {
        int mu = 2000;
        double prob = (tf + mu * (cf / collectionTerms)) / (doclen + mu);
        return prob;
    }

    public static double termDistinct(double tf, int doclen, double cf, double collectionTerms) {
        double probTerm = lmdProb(tf, doclen, cf, collectionTerms);
        double probColl = cf / collectionTerms;
        return probDivergence(probTerm, probColl);
    }

    /**
     * P(t|d) * log(P(t|d)/P(t|C))
     *
     * @param prob
     * @param prob_background
     * @return
     */
    public static double probDivergence(double prob, double prob_background) {
        double log2inv = 1d / Math.log(2);
        return prob * Math.log(prob / prob_background) * log2inv;
    }

}
