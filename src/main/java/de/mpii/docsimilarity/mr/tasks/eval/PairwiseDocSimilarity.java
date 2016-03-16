package de.mpii.docsimilarity.mr.tasks.eval;

import de.mpii.docsimilarity.mr.input.docpair.SparseDocVector;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.QidCwidsRelSimilarity;
import de.mpii.docsimilarity.mr.utils.VectorPairWritable;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class PairwiseDocSimilarity extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(PairwiseDocSimilarity.class);

    public static class Constants {

        public static String SEP = "/";
        public static String SIMILARITY = "avgw2v";
    }

    private static enum Counter {

        TOTAL, RELREL, ERRORS, RELIRR, IRRIRR, MAPPERQIDS, REDUCERQIDSRELREL, REDUCERQIDSRELNONR
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("q", "qrelfile", true, "qrel file");
        options.addOption("d", "docvectorfile", true, "docvector file");
        options.addOption("c", "cardinality", true, "cardinality for converting mahout vector");
        options.addOption("o", "outputdir", true, "output directory");
        options.addOption("k", "rank", true, "rank number 20..200");
        options.addOption("l", "log4jconf", true, "log4jconf");
        options.addOption("n", "expname", true, "specific expanme");
        options.addOption("h", "help", false, "print this message");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h") || cmd.getOptions().length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(0);
        }

        String expname = cmd.getOptionValue('n');
        String docvectordir = cmd.getOptionValue('d');
        String outputdir = cmd.getOptionValue('o');
        String qrelfiledir = cmd.getOptionValue('q');
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * start setting up job
         */
        Job job = new Job(getConf(), PairwiseDocSimilarity.class.getSimpleName() + " compute doc pairwise similarity " + expname);
        //job.getConfiguration().setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        job.setJarByClass(PairwiseDocSimilarity.class);
        job.setNumReduceTasks(1);
        for (String qrelyear : new String[]{"wt09", "wt10", "wt11", "wt12", "wt13", "wt14"}) {
            DistributedCache.addCacheFile(new Path(qrelfiledir + "/qrels.adhoc." + qrelyear).toUri(), job.getConfiguration());
        }
        // recursively add the files to input
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus[] status_list = fs.listStatus(new Path(docvectordir));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                String filename = status.getPath().getName();
                if (!filename.startsWith("_")) {
                    FileInputFormat.addInputPath(job, status.getPath());
                }
            }
        }
        Path outputpath = null;
        if (cmd.hasOption('k')) {
            int rank = Integer.parseInt(cmd.getOptionValue('k'));
            outputpath = new Path(outputdir + Constants.SEP + Constants.SIMILARITY + Constants.SEP + "rank" + rank);
        } else {
            outputpath = new Path(outputdir + Constants.SEP + Constants.SIMILARITY);
        }

        if (cmd.hasOption('c')) {
            job.getConfiguration().setInt(GlobalConstants.CARDINALITY_SPARSEVEC, Integer.parseInt(cmd.getOptionValue('c')));
        }

        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);
        job.setInputFormatClass(SparseDocVector.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(QidCwidsRelSimilarity.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(QidCwidsRelSimilarity.class);

        job.setMapperClass(DocSimilarityMapper.class);
        job.setReducerClass(LabeltypeReducer.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    public static class DocSimilarityMapper extends Mapper<QidCwidsRelSimilarity, VectorPairWritable, IntWritable, QidCwidsRelSimilarity> {

        private final IntWritable labeltype = new IntWritable();

        private final TIntSet qids = new TIntHashSet();

        @Override
        public void map(QidCwidsRelSimilarity key, VectorPairWritable value, Context context) {
            try {
                int qid = key.getQid();

                labeltype.set(key.getLabelType());
                if (labeltype.get() == 1 || labeltype.get() == 2) {
                    double similarity = value.cosineSimilarity();
                    key.setSimilarity(similarity);
                    context.write(labeltype, key);
                    if (!qids.contains(qid)) {
                        qids.add(qid);
                        context.getCounter(Counter.MAPPERQIDS).increment(1);
                    }
                }
                if (labeltype.get() == 1) {
                    context.getCounter(Counter.RELIRR).increment(1);
                } else if (labeltype.get() == 2) {
                    context.getCounter(Counter.RELREL).increment(1);
                } else {
                    context.getCounter(Counter.IRRIRR).increment(1);
                }
                context.getCounter(Counter.TOTAL).increment(1);
            } catch (Exception ex) {
                logger.error("", ex);
                context.getCounter(Counter.ERRORS).increment(1);
            }
        }

    }

    public static class LabeltypeReducer extends Reducer<IntWritable, QidCwidsRelSimilarity, NullWritable, QidCwidsRelSimilarity> {

        private final TIntObjectMap<TIntSet> labelQids = new TIntObjectHashMap<>();

        @Override
        public void reduce(IntWritable key, Iterable<QidCwidsRelSimilarity> values, Context context) {
            try {
                List<QidCwidsRelSimilarity> listOfvalue = new ArrayList<>();
                for (QidCwidsRelSimilarity val : values) {
                    int qid = val.getQid();
                    if (!labelQids.containsKey(key.get())) {
                        labelQids.put(key.get(), new TIntHashSet());
                    }
                    if (!labelQids.get(key.get()).contains(qid)) {
                        labelQids.get(key.get()).add(qid);
                        if (key.get() == 1) {
                            context.getCounter(Counter.REDUCERQIDSRELNONR).increment(1);
                        } else if (key.get() == 2) {
                            context.getCounter(Counter.REDUCERQIDSRELREL).increment(1);
                        }
                    }

                    listOfvalue.add(new QidCwidsRelSimilarity(val));
                }

                if (key.get() == 1) {
                    Collections.sort(listOfvalue, new DescendingComparator());
                } else if (key.get() == 2) {
                    Collections.sort(listOfvalue, new AscendingComparator());
                }
                for (QidCwidsRelSimilarity value : listOfvalue) {
                    context.write(NullWritable.get(), value);
                }
            } catch (Exception ex) {
                logger.error("", ex);
                context.getCounter(Counter.ERRORS).increment(1);
            }
        }

        private class AscendingComparator implements Comparator<QidCwidsRelSimilarity> {

            @Override
            public int compare(QidCwidsRelSimilarity o1, QidCwidsRelSimilarity o2) {
                if (o1.getSimilarity() > o2.getSimilarity()) {
                    return 1;
                } else if (o1.getSimilarity() < o2.getSimilarity()) {
                    return -1;
                } else {
                    return 0;
                }
            }

        }

        private class DescendingComparator implements Comparator<QidCwidsRelSimilarity> {

            @Override
            public int compare(QidCwidsRelSimilarity o1, QidCwidsRelSimilarity o2) {
                if (o1.getSimilarity() > o2.getSimilarity()) {
                    return -1;
                } else if (o1.getSimilarity() < o2.getSimilarity()) {
                    return 1;
                } else {
                    return 0;
                }
            }

        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PairwiseDocSimilarity(), args);
        System.exit(exitCode);
    }

}
