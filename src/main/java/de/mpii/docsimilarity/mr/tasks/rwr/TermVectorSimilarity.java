package de.mpii.docsimilarity.mr.tasks.rwr;

import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.io.ReadTermTermidMap;
import de.mpii.docsimilarity.mr.utils.io.ReadWordVector;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.TIntSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Given the vector for each term, we pre-compute the similarity for each term
 * pair. The mahout NamedVector is used, and we generate the symmetric matrix
 * for term-term pairwise similarity. Cosine similarity is used.
 *
 * The computed similarity vector is too huge to be used, e.g., 30G for cw12
 *
 * @author khui
 */
public class TermVectorSimilarity extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(TermVectorSimilarity.class);

    private static class Constants {

        public static String SEP = "/";
        public static String SIMILARITY_OUTDIR = "W2VSimiMatrix";
        public static String TERMVEC_FILE = "termvecfile";
        // the dimension used in word vector, e.g., for word2vec, by default is 300
        public static String VEC_CARDINALITY = "cardinality";
        public static String COL_NAME = "datasetname";
        public static String TERMSTAT_FILE = "termstatfile";
        public static double SIMILARITY_MINI_THRESHOLD = 0.35;
    }

    private static enum Counter {

        ROWNUM2MATRIX, TERMPAIRCOUNT, OMITTERMPAIRCOUNT, SIMROWNUM, ERRORS
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("t", "termstat", true, "termstat file");
        options.addOption("o", "outputdir", true, "output directory or the similarity matrix");
        options.addOption("c", "cardinality", true, "cardinality for the term vector");
        options.addOption("l", "log4jconf", true, "log4jconf");
        options.addOption("n", "dataname", true, "name of the dataset");
        options.addOption("h", "help", false, "print this message");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h") || cmd.getOptions().length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(0);
        }

        String dataname = cmd.getOptionValue('n');
        String termstatdir = cmd.getOptionValue('t');
        String outputdir = cmd.getOptionValue('o');
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * start setting up job
         */
        Job job = new Job(getConf(), TermVectorSimilarity.class.getSimpleName() + " compute pairwise term similarity on " + dataname + " with threahold: " + Constants.SIMILARITY_MINI_THRESHOLD);
        job.setJarByClass(TermVectorSimilarity.class);
        job.setNumReduceTasks(1);

        // put the termvector file into the cache
        Path termvectorPath = new Path(termstatdir + Constants.SEP + "term-w2v." + dataname);

        job.addCacheFile(termvectorPath.toUri());
        job.addCacheFile(new Path(termstatdir + Constants.SEP + "term-df-cf-sort-" + dataname).toUri());
        // set variable
        job.getConfiguration().set(Constants.COL_NAME, dataname);
        job.getConfiguration().set(Constants.TERMSTAT_FILE, "term-df-cf-sort-" + dataname);
        job.getConfiguration().set(Constants.TERMVEC_FILE, "term-w2v." + dataname);
        // recursively add the files to input
        FileInputFormat.addInputPath(job, termvectorPath);
        // set output directory for the symmetric matrix of term similarity
        Path outputpath = new Path(outputdir + Constants.SEP + "termsimilarity." + Constants.SIMILARITY_MINI_THRESHOLD + "." + dataname);
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.toString() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }
        // set input ouput format and mapper reducer
        // each line is a split
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 1500);
        // use the same key for all map, and the value is a namedvector wrapped by vectorwritable
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        // set output format
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // set mapper and reducer
        job.setMapperClass(PairwiseTermSimilarityMapper.class);
        job.setReducerClass(ConcatVector2Matrix.class);
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    public static class PairwiseTermSimilarityMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {

        // use nullwritable as mapper key may cause low efficiency in reducer, thus we use 
        // a fixed integer as the mapper output key
        private final IntWritable rowTermid = new IntWritable();

        private final VectorWritable colVector = new VectorWritable();

        private final TObjectIntMap<String> termTermid = new TObjectIntHashMap<>();

        private final TIntObjectMap<NamedVector> termidVector = new TIntObjectHashMap<>();

        private final TIntObjectMap<String> termidTerm = new TIntObjectHashMap<>();

        private int termVectorCardinality;

        private int simiMatrixCardinality;

        private String colname;

        @Override
        public void setup(Context context) throws IOException {
            try {
                String termvecFile = context.getConfiguration().get(Constants.TERMVEC_FILE);
                String termstatFile = context.getConfiguration().get(Constants.TERMSTAT_FILE);
                termVectorCardinality = context.getConfiguration().getInt(Constants.VEC_CARDINALITY, 300);
                colname = context.getConfiguration().get(Constants.COL_NAME);
                // we will have symmetric matrix of pairwise term similarity
                // ultimately, and we use the global termTermid as idx
                switch (colname) {
                    case "cw09":
                        simiMatrixCardinality = GlobalConstants.CARDINALITY_CW09;
                        break;
                    case "cw12":
                        simiMatrixCardinality = GlobalConstants.CARDINALITY_CW12;
                        break;
                }
                Path termvecPath = null, termstatPath = null;
                Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (localFiles != null) {
                    for (Path file : localFiles) {
                        String pathstr = file.getName();
                        if (pathstr.endsWith(termvecFile)) {
                            termvecPath = file;
                            logger.info(file.toString() + " will be used");
                        } else {
                            if (pathstr.endsWith(termstatFile)) {
                                termstatPath = file;
                                logger.info(file.toString() + " will be used");
                            }
                        }
                    }
                } else {
                    logger.fatal("local files read in failed");
                }
                ReadTermTermidMap.readTermDfCf(termstatPath.toString(), termTermid, termidTerm, false);
                ReadWordVector.readTermVector(termvecPath.toString(), termTermid, termidVector, termVectorCardinality);
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) {
            try {
                String line = value.toString();
                String[] cols = line.split(" ");
                String term = cols[0];
                int xTid = termTermid.get(term);
                rowTermid.set(xTid);
                Vector xVector = ReadWordVector.convert2VecSingleline(cols, termVectorCardinality);
                Vector yVector;

                Vector simiRow = new RandomAccessSparseVector(simiMatrixCardinality, 110000);
                for (int ytid : termidVector.keys()) {
                    if (ytid <= xTid) {
                        // only compute pariwise similarity after the current 
                        // term, avoiding repeat computation
                        continue;
                    }
                    yVector = termidVector.get(ytid);
                    double similarity = cosineSimilarity(xVector, yVector);
                    if (similarity > Constants.SIMILARITY_MINI_THRESHOLD) {
                        //logger.info(term + " " + termidTerm.get(ytid) + " " + similarity);
                        simiRow.set(ytid, similarity);
                        context.getCounter(Counter.TERMPAIRCOUNT).increment(1);
                    } else {
                        context.getCounter(Counter.OMITTERMPAIRCOUNT).increment(1);
                    }
                }
                // the name of the column vector is the termid of current term
                colVector.set(simiRow);
                context.write(rowTermid, colVector);
                context.getCounter(Counter.SIMROWNUM).increment(1);
            } catch (Exception ex) {
                logger.error("", ex);
                context.getCounter(Counter.ERRORS).increment(1);
            }
        }

        private double cosineSimilarity(Vector xVector, Vector yVector) {
            double similarity = -1;
            try {
                CosineDistanceMeasure cs = new CosineDistanceMeasure();
                similarity = (float) (1 - cs.distance(xVector, yVector));
            } catch (Exception ex) {
                logger.error("", ex);
            }
            return similarity;
        }

    }

    public static class ConcatVector2Matrix extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<VectorWritable> rows, Context context) {
            try {

                for (VectorWritable row : rows) {
                    context.getCounter(Counter.ROWNUM2MATRIX).increment(1);
                    context.write(key, row);
                }
                logger.info("All rows have been writen to the matrix: " + key.get());
            } catch (Exception ex) {
                logger.error("", ex);
                context.getCounter(Counter.ERRORS).increment(1);
            }
        }

    }

    public static class WordSimilarity {

        private final Map<Integer, Vector> xtidytidSimilarity = new HashMap<>();

        private final TIntDoubleMap tidSimiNorm = new TIntDoubleHashMap();

        public WordSimilarity(Configuration conf, String seqSimilarityFile, TIntSet termidInUse, int simiMatrixCardinality) {
            loadTermSimilarity(conf, seqSimilarityFile, termidInUse, simiMatrixCardinality);
        }

        public Matrix getTermSimiMatrix(int cardinality) {
            logger.info("Start to copy to matrix: " + cardinality);
            Matrix m = new SparseMatrix(cardinality, cardinality, xtidytidSimilarity, true);
            logger.info("Finished copy to matrix: " + cardinality);
            return m;
        }

        public double similarity(int tidx, int tidy) {
            int tidz;
            // tidx should less than tidy
            if (tidx > tidy) {
                tidz = tidx;
                tidx = tidy;
                tidy = tidz;
            } else if (tidx == tidy) {
                logger.error(tidx + " " + tidy + " is illegal.");
                return -1;
            } else if (!contains(tidx) || !contains(tidy)) {
                logger.error(tidx + " " + tidy + " is not valid.");
                return -1;
            }
            Vector simiVec = xtidytidSimilarity.get(tidx);
            return simiVec.get(tidy);
        }

        public boolean contains(int tid) {
            return xtidytidSimilarity.containsKey(tid);
        }

        public Map<Integer, Vector> loadTermSimilarity(Configuration conf, String seqSimilarityFile, TIntSet termidsInUse, int simiMatrixCardinality) {
            try {
                FileSystem fs = FileSystem.getLocal(conf);
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(seqSimilarityFile), conf);
                IntWritable termid = new IntWritable();
                VectorWritable vecSimilarity = new VectorWritable();
                while (reader.next(termid, vecSimilarity)) {
                    int xtid = termid.get();
                    if (termidsInUse.contains(xtid)) {
                        Vector vec = vecSimilarity.get();
                        for (Vector.Element ele : vec.nonZeroes()) {
                            int ytid = ele.index();
                            if (termidsInUse.contains(ytid)) {
                                double similarity = ele.get();
//                                if (similarity < 0.6) {
//                                    continue;
//                                }
                                if (!xtidytidSimilarity.containsKey(xtid)) {
                                    xtidytidSimilarity.put(xtid, new RandomAccessSparseVector(simiMatrixCardinality));
                                }
                                if (!xtidytidSimilarity.containsKey(ytid)) {
                                    xtidytidSimilarity.put(ytid, new RandomAccessSparseVector(simiMatrixCardinality));
                                }
                                xtidytidSimilarity.get(xtid).set(ytid, similarity);
                                xtidytidSimilarity.get(ytid).set(xtid, similarity);
                            }
                        }
                    }
                }
                reader.close();
                for (int tid : termidsInUse.toArray()) {
                    if (!xtidytidSimilarity.containsKey(tid)) {
                        xtidytidSimilarity.put(tid, new RandomAccessSparseVector(simiMatrixCardinality));
                    }
                    xtidytidSimilarity.get(tid).set(tid, 1);
                }
                logger.info("Similarity matrix has been loaded for " + xtidytidSimilarity.size() + " terms.");
            } catch (Exception ex) {
                logger.error("", ex);
            }
            return xtidytidSimilarity;
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TermVectorSimilarity(), args);
        System.exit(exitCode);
    }

}
