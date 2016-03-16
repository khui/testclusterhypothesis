package de.mpii.docsimilarity.mr.tasks.eval;

import de.mpii.docsimilarity.mr.input.DocTripleInputFormat;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
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
import org.apache.hadoop.io.Text;
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
public class EvaluateDocTriple extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(EvaluateDocTriple.class);

    private static final DecimalFormat df = new DecimalFormat(".0000");

    public static class Constants {

        public static String SEP = "/";
        public static String SIMILARITYFILENAME = "similarityFileName";
        public static String VECTORIZEMETHOD = "rwrTfidf";

    }

    private static enum Counter {

        TOTAL, RELPAIRMISS, ERRORS, NONRELPAIRMISS, TRUET, FALSET
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("t", "tripledir", true, "doc triple directory");
        options.addOption("s", "similarityfile", true, "pairwise doc similarity file");
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
        String outputdir = cmd.getOptionValue('o');
        String log4jconf = cmd.getOptionValue('l');
        String tripledir = cmd.getOptionValue('t');
        String similarityf = cmd.getOptionValue('s');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * start setting up job
         */
        Job job = new Job(getConf(), EvaluateDocTriple.class.getSimpleName() + ": " + expname);
        job.setJarByClass(EvaluateDocTriple.class);
        job.setNumReduceTasks(1);
        // add the similarity file into the distributed cash
        Path similarityPath = new Path(similarityf);
        DistributedCache.addCacheFile(similarityPath.toUri(), job.getConfiguration());
        String similarityFilename = similarityPath.getName();
        job.getConfiguration().set(Constants.SIMILARITYFILENAME, similarityFilename);
        // recursively add the files to input
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus[] status_list = fs.listStatus(new Path(tripledir));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }

        Path outputpath = null;
        if (cmd.hasOption('k')) {
            int rank = Integer.parseInt(cmd.getOptionValue('k'));
            outputpath = new Path(outputdir + Constants.SEP + Constants.VECTORIZEMETHOD + Constants.SEP + rank);
        } else {
            outputpath = new Path(outputdir + Constants.SEP + Constants.VECTORIZEMETHOD);
        }

        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);
        job.setInputFormatClass(DocTripleInputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(DocTriplePrecision.class);
        job.setReducerClass(Merger.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    public static class DocTriplePrecision extends Mapper<IntWritable, IntWritable, IntWritable, Text> {

        private int trueCount = 0;

        private int falseCount = 0;

        private int nonrelPairMiss = 0;

        private int relPairMiss = 0;

        private int qid = -1;

        @Override
        public void map(IntWritable key, IntWritable value, Context context) {
            try {
                if (qid != key.get()) {
                    logger.error("qid not equal to key: " + qid + ", " + key.get());
                    qid = key.get();
                }
                int label = value.get();
                switch (label) {
                    case 1:
                        trueCount++;
                        context.getCounter(Counter.TRUET).increment(1);
                        break;
                    case 0:
                        falseCount++;
                        context.getCounter(Counter.FALSET).increment(1);
                        break;
                    case -1:
                        nonrelPairMiss++;
                        context.getCounter(Counter.NONRELPAIRMISS).increment(1);
                        break;
                    case -2:
                        relPairMiss++;
                        context.getCounter(Counter.RELPAIRMISS).increment(1);
                        break;
                }
                context.getCounter(Counter.TOTAL).increment(1);

            } catch (Exception ex) {
                logger.error("", ex);
                context.getCounter(Counter.ERRORS).increment(1);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            double ratio = trueCount / (double) (trueCount + falseCount);
            sb.append(qid).append(" ");
            sb.append(trueCount).append(" ");
            sb.append(falseCount).append(" ");
            sb.append(relPairMiss).append(" ");
            sb.append(nonrelPairMiss).append(" ");
            sb.append(df.format(ratio));
            context.write(new IntWritable(qid), new Text(sb.toString()));
            logger.info(sb.toString());
        }

    }

    public static class Merger extends Reducer<IntWritable, Text, NullWritable, Text> {

        private final TIntObjectMap<String> qidLine = new TIntObjectHashMap<>();

        private double ratioSum = 0;

        private int relPairMissSum = 0;

        private int nrelPairMissSum = 0;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) {
            try {
                for (Text value : values) {
                    String line = value.toString();
                    int qid = key.get();
                    String[] cols = line.split(" ");
                    double ratio = Double.parseDouble(cols[cols.length - 1]);
                    int relPairMiss = Integer.parseInt(cols[1]);
                    int nonrelPairMiss = Integer.parseInt(cols[2]);
                    qidLine.put(qid, line);
                    ratioSum += ratio;
                    relPairMissSum += relPairMiss;
                    nrelPairMissSum += nonrelPairMiss;
                }

            } catch (Exception ex) {
                logger.error("", ex);
                context.getCounter(Counter.ERRORS).increment(1);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int[] qids = qidLine.keys();
            Arrays.sort(qids);
            Text line = new Text();
            for (int qid : qids) {
                line.set(qidLine.get(qid));
                context.write(NullWritable.get(), line);
            }
            double nonrelPairMiss = (double) nrelPairMissSum / qidLine.size();
            double relPairMiss = (double) relPairMissSum / qidLine.size();
            double ratio = ratioSum / qidLine.size();
            StringBuilder sb = new StringBuilder();
            sb.append(0).append(" ");
            sb.append(df.format(relPairMiss)).append(" ");
            sb.append(df.format(nonrelPairMiss)).append(" ");
            sb.append(df.format(ratio));
            line.set(sb.toString());
            context.write(NullWritable.get(), line);
            // pass back to show the summary results in the log directly
            sb = new StringBuilder();
            sb.append(df.format(relPairMiss)).append(" ");
            sb.append(df.format(nonrelPairMiss)).append(" ");
            sb.append(df.format(ratio));
            context.getConfiguration().set(IntegratedDoctripleEval.Constants.AVE_PRECISION, sb.toString());

        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new EvaluateDocTriple(), args);
        System.exit(exitCode);
    }

}
