package de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec;

import de.mpii.docsimilarity.MaxRepSelection;
import de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec.UniqQDRecord.QueryDocTerms;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.deeplearning4j.models.word2vec.Word2Vec;
import java.io.IOException;
import org.nd4j.linalg.api.ndarray.INDArray;

/**
 *
 * @author khui
 */
public class WordV2DocVImpls {

    private static final Logger logger = Logger.getLogger(WordV2DocVImpls.class);

    public static class AvgTermVectors extends WordV2DocV {

        public AvgTermVectors(List<QueryDocTerms> querydocterms, int topN) throws IOException {
            super(querydocterms, topN);
        }

        @Override
        protected float[] terms2docvector(List<String> termWeight, int topN) {
            List<INDArray> wordvectors = new ArrayList<>();
            convert2wordvecs(termWeight, wordvectors);
            return avgTermVector(wordvectors, topN);
        }

    }

    public static class ConcatTermVectors extends WordV2DocV {

        public ConcatTermVectors(List<QueryDocTerms> querydocterms, int topN) throws IOException {
            super(querydocterms, topN);
        }

        @Override
        protected float[] terms2docvector(List<String> termWeight, int topN) {
            List<INDArray> wordvectors = new ArrayList<>();
            convert2wordvecs(termWeight, wordvectors);
            return concatTermVector(wordvectors, topN);
        }

    }

    public class MaxRepSelectedTerms extends WordV2DocV {

        private final double similarityThreshold;

        public MaxRepSelectedTerms(Word2Vec w2v, List<QueryDocTerms> querydocterms, int topN, double similarityThread) throws IOException {
            super(querydocterms, topN);
            this.similarityThreshold = similarityThread;
        }

        @Override
        protected float[] terms2docvector(List<String> termWeight, int topN) {
            List<INDArray> wordvectors = new ArrayList<>();
            convert2wordvecs(termWeight, wordvectors);
            float[] docvec = null;
            MaxRepSelection mpselect = new MaxRepSelection(wordvectors, similarityThreshold);
            int[] idxes = mpselect.selectMaxRep(topN);
            for (int idx : idxes) {
                if (docvec == null) {
                    docvec = new float[wordvectors.get(idx).length()];
                }
                weightMultiply(docvec, wordvectors.get(idx), 1d / idxes.length);
            }
            if (docvec != null) {
                return docvec;
            } else {
                return null;
            }
        }
    }
}
