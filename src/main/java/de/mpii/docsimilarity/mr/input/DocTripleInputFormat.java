package de.mpii.docsimilarity.mr.input;

import de.mpii.docsimilarity.mr.tasks.eval.EvaluateDocTriple;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.deeplearning4j.util.MathUtils;

/**
 * for each (doc_1, doc_2, doc_3) triple for given query, doc_1 and doc_2 are
 * relevant and doc_3 is non-relevant read in pairwise similarity from
 * pre-computed file for all three pairs if average(simi(doc_1, doc_3),
 * simi(doc_2, doc3)) < simi(doc_1, doc_2) issue (qid, 1), otherwise (qid, 0)
 * @author khui
 */
public class DocTripleInputFormat extends FileInputFormat<IntWritable, IntWritable> {

    private static final Logger logger = Logger.getLogger(DocTripleInputFormat.class);

    @Override
    public RecordReader<IntWritable, IntWritable> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        return new DocTripleRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public class DocTripleRecordReader extends RecordReader<IntWritable, IntWritable> {

        private final TIntObjectMap<Map<String, TObjectDoubleMap<String>>> relrelSimi = new TIntObjectHashMap<>();

        private final TIntObjectMap<Map<String, TObjectDoubleMap<String>>> relirSimi = new TIntObjectHashMap<>();

        private BufferedReader br;

        private long start;
        private long pos;
        private long end;
        private Seekable filePosition;

        private final IntWritable key = new IntWritable();

        private final IntWritable value = new IntWritable();

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            // for the split                 
            FileSplit split = (FileSplit) is;
            Path splitpath = split.getPath();
            String filename = splitpath.getName();
            // get distributed file: doc pairwise similarity
            Configuration conf = tac.getConfiguration();
            String pairwiseSimilarity = conf.get(EvaluateDocTriple.Constants.SIMILARITYFILENAME);
            Path[] paths = DistributedCache.getLocalCacheFiles(conf);
            FileSystem fs = FileSystem.getLocal(conf);
            int targetQid = Integer.parseInt(filename);
            try {
                // read in the similarity file
                for (Path path : paths) {
                    if (path.getName().equals(pairwiseSimilarity)) {
                        if (fs.exists(path)) {
                            br = new BufferedReader(new InputStreamReader(fs.open(path)));
                            while (br.ready()) {
                                String line = br.readLine().trim();
                                String[] cols = line.split(" ");
                                if (cols.length != 5) {
                                    logger.error("Illegal format of input line and we skipped: " + line);
                                    continue;
                                }
                                int qid = Integer.parseInt(cols[0]);
                                if (targetQid != qid) {
                                    continue;
                                }
                                String cwidl = cols[1];
                                String cwidr = cols[2];
                                int qrelType = Integer.parseInt(cols[3]);
                                double similarity = Double.parseDouble(cols[4]);
                                if (qrelType == 2) {
                                    if (!relrelSimi.containsKey(qid)) {
                                        relrelSimi.put(qid, new HashMap<String, TObjectDoubleMap<String>>());
                                    }
                                    if (!relrelSimi.get(qid).containsKey(cwidl)) {
                                        relrelSimi.get(qid).put(cwidl, new TObjectDoubleHashMap<String>());
                                    }
                                    relrelSimi.get(qid).get(cwidl).put(cwidr, similarity);
                                }
                                if (qrelType == 1) {
                                    if (!relirSimi.containsKey(qid)) {
                                        relirSimi.put(qid, new HashMap<String, TObjectDoubleMap<String>>());
                                    }
                                    if (!relirSimi.get(qid).containsKey(cwidl)) {
                                        relirSimi.get(qid).put(cwidl, new TObjectDoubleHashMap<String>());
                                    }
                                    relirSimi.get(qid).get(cwidl).put(cwidr, similarity);
                                }
                            }
                            logger.info("FINISH read in " + path.getName() + " for qid " + targetQid + " : " + relrelSimi.get(targetQid).size() + ", " + relirSimi.get(targetQid).size());
                            br.close();
                        } else {
                            logger.error(path.getName() + " does not exists!");
                        }
                    }
                }
                // read in the split
                fs = splitpath.getFileSystem(conf);
                FSDataInputStream fileIn = fs.open(split.getPath());
                br = new BufferedReader(new InputStreamReader(fileIn));
                start = split.getStart();
                pos = start;
                end = start + split.getLength();
                filePosition = fileIn;
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (br.ready()) {
                String line = br.readLine();
                String[] cols = line.split(" ");
                int qid = Integer.parseInt(cols[0]);
                String cwid1 = cols[1];
                String cwid2 = cols[2];
                String cwid3 = cols[3];
                double relrel = getPairwiseSimilarity(qid, relrelSimi, cwid1, cwid2);
                key.set(qid);
                if (relrel >= 0) {
                    double relir0 = getPairwiseSimilarity(qid, relirSimi, cwid1, cwid3);
                    if (relir0 >= 0) {
                        double relir1 = getPairwiseSimilarity(qid, relirSimi, cwid2, cwid3);
                        if (relir1 >= 0) {

                            double summarize_irpairs = MathUtils.max(new double[]{relir0, relir1});
                            //MathUtils.mean(relir0, relir1);
                            pos = filePosition.getPos();
                            if (summarize_irpairs < relrel) {
                                // trueCount
                                value.set(1);
                            } else {
                                // falseCount
                                value.set(0);
                            }
                        } else {
                            // nonrelPairMiss
                            value.set(-1);
                        }
                    } else {
                        // nonrelPairMiss
                        value.set(-1);
                    }
                } else {
                    // relPairMiss
                    value.set(-2);
                }
                return true;
            }
            return false;
        }

        private double getPairwiseSimilarity(int qid, TIntObjectMap<Map<String, TObjectDoubleMap<String>>> simi, String cwid1, String cwid2) {
            double similarity = -1;
            if (simi.containsKey(qid)) {
                if (simi.get(qid).containsKey(cwid1)) {
                    if (simi.get(qid).get(cwid1).containsKey(cwid2)) {
                        similarity = simi.get(qid).get(cwid1).get(cwid2);
                    }
                }
                if (similarity < 0) {
                    if (simi.get(qid).containsKey(cwid2)) {
                        if (simi.get(qid).get(cwid2).containsKey(cwid1)) {
                            similarity = simi.get(qid).get(cwid2).get(cwid1);
                        }
                    }
                }
            }
            return similarity;
        }

        @Override
        public IntWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public IntWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        @Override
        public void close() throws IOException {
            br.close();
        }

    }

}
