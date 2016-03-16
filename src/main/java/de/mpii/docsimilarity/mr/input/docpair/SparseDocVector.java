package de.mpii.docsimilarity.mr.input.docpair;

import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.QidCwidsRelSimilarity;
import de.mpii.docsimilarity.mr.utils.io.ReadQrel;
import de.mpii.docsimilarity.mr.utils.io.ReadSparseVector;
import de.mpii.docsimilarity.mr.utils.VectorPairWritable;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * org.apache.mahout.math.RandomAccessSparseVector.java
 * org.apache.mahout.math.hadoop.similarity.VectorDistanceMapper
 * org.apache.mahout.common.distance.CosineDistanceMeasure
 *
 * @author khui
 */
public class SparseDocVector extends FileInputFormat<QidCwidsRelSimilarity, VectorPairWritable> {

    private static final Logger logger = Logger.getLogger(SparseDocVector.class);

    @Override
    public RecordReader<QidCwidsRelSimilarity, VectorPairWritable> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        return new PairwiseVectorRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public static class PairwiseVectorRecordReader extends RecordReader<QidCwidsRelSimilarity, VectorPairWritable> {

        private Map<String, TIntList> cwidRelSubtopics = null;
        private Map<String, TIntDoubleMap> cwidTidTfidf = null;
        private List<String[]> cwidPairs;
        private int cwidArrayIndex = 0;
        private int qid;
        private QidCwidsRelSimilarity key;
        private VectorPairWritable value;
        private long cardinality;

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext tac) {
            FileSplit split = (FileSplit) genericSplit;
            cardinality = tac.getConfiguration().getLong(GlobalConstants.CARDINALITY_SPARSEVEC, -1);

            try {
                Path path = split.getPath();
                Configuration conf = tac.getConfiguration();
                qid = Integer.parseInt(path.getName().split("-")[0]);
                String qrelyear;

                if (qid < 51) {
                    qrelyear = "wt09";
                    cardinality
                            = (cardinality > 0 ? cardinality : GlobalConstants.CARDINALITY_CW09);
                } else if (qid < 101) {
                    qrelyear = "wt10";
                    cardinality
                            = (cardinality > 0 ? cardinality : GlobalConstants.CARDINALITY_CW09);
                } else if (qid < 151) {
                    qrelyear = "wt11";
                    cardinality
                            = (cardinality > 0 ? cardinality : GlobalConstants.CARDINALITY_CW09);
                } else if (qid < 201) {
                    qrelyear = "wt12";
                    cardinality
                            = (cardinality > 0 ? cardinality : GlobalConstants.CARDINALITY_CW09);
                } else if (qid < 251) {
                    qrelyear = "wt13";
                    cardinality
                            = (cardinality > 0 ? cardinality : GlobalConstants.CARDINALITY_CW12);
                } else if (qid < 301) {
                    qrelyear = "wt14";
                    cardinality
                            = (cardinality > 0 ? cardinality : GlobalConstants.CARDINALITY_CW12);
                } else {
                    logger.error("unrecognized path:" + path.getName());
                    return;
                }
                logger.info("Use cardinality " + cardinality);

                // read in qrel file
                Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
                if (localFiles != null) {
                    for (Path file : localFiles) {
                        String pathstr = file.getName();
                        if (pathstr.endsWith(qrelyear)) {
                            cwidRelSubtopics = ReadQrel.readInAdhocQrel(file, qid);
                        }
                    }
                }
                // read in the split: the doc vector for processing
                logger.warn(path.toString() + " is processed");
                cwidTidTfidf = ReadSparseVector.tidTfidfMap(conf, path);
                String[] cwids = cwidTidTfidf.keySet().toArray(new String[0]);
                cwidPairs = new ArrayList<>();
                for (int i = 0; i < cwids.length; i++) {
                    for (int j = i + 1; j < cwids.length; j++) {
                        cwidPairs.add(new String[]{cwids[i], cwids[j]});
                    }
                }
                logger.info("Queryid " + qid + " has documents: " + cwidTidTfidf.size());
            } catch (Exception ex) {
                logger.error("", ex);
            }

        }

        @Override
        public boolean nextKeyValue() {
            try {
                while (true) {
                    if (cwidArrayIndex >= cwidPairs.size()) {
                        return false;
                    }
                    String[] cwids = cwidPairs.get(cwidArrayIndex);
                    cwidArrayIndex++;
                    if (cwidRelSubtopics.containsKey(cwids[0]) && cwidRelSubtopics.containsKey(cwids[1])) {
                        int rel0 = cwidRelSubtopics.get(cwids[0]).contains(1) ? 1 : 0;
                        int rel1 = cwidRelSubtopics.get(cwids[1]).contains(1) ? 1 : 0;
                        key = new QidCwidsRelSimilarity(cwids[0], cwids[1], qid, rel0 + rel1);
                        value = new VectorPairWritable(cwidTidTfidf.get(cwids[0]), cwidTidTfidf.get(cwids[1]), cardinality);
                        return true;
                    } else {
                        logger.error("Not all cwids are in qrel: " + cwids[0] + " " + cwids[1]);
                    }
                }
            } catch (Exception ex) {
                logger.error("", ex);
                return false;
            }
        }

        @Override
        public QidCwidsRelSimilarity getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public VectorPairWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (cwidPairs == null) {
                return 0f;
            }
            if (cwidPairs.isEmpty()) {
                return 0f;
            }
            int pairnum = cwidPairs.size();
            return (cwidArrayIndex + 1) / (float) pairnum;
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static Vector convert2vector(TIntDoubleMap doc, long cardinality) {
        Vector docVector;
        if (cardinality > Integer.MAX_VALUE) {
            cardinality = Integer.MAX_VALUE;
        }
        if (cardinality <= GlobalConstants.CARDINALITY_THRES_DENSEVEC) {
            docVector = new DenseVector((int) cardinality);
        } else {
            docVector = new RandomAccessSparseVector((int) cardinality);
        }
        try {
            // the vector index is supposed to start from 0
            for (int tid : doc.keySet().toArray()) {
                docVector.set(tid, doc.get(tid));
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return docVector;
    }

    public static class Convert2Vector {

        TObjectIntMap<String> termIndex = new TObjectIntHashMap<>();

        public Convert2Vector(TObjectDoubleMap<String> doc1, TObjectDoubleMap<String> doc2) {
            int index = 0;
            for (String term : doc1.keySet()) {
                if (!termIndex.containsKey(term)) {
                    termIndex.put(term, index);
                    index++;
                }
            }
            for (String term : doc2.keySet()) {
                if (!termIndex.containsKey(term)) {
                    termIndex.put(term, index);
                    index++;
                }
            }
        }

        public Vector convert2vector(TObjectDoubleMap<String> doc) {
            Vector docVector = new RandomAccessSparseVector(termIndex.size());
            try {
                for (String term : doc.keySet()) {
                    docVector.set(termIndex.get(term), doc.get(term));
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }
            return docVector;
        }

    }

}
