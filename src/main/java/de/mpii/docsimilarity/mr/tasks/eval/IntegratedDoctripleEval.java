package de.mpii.docsimilarity.mr.tasks.eval;

import de.mpii.docsimilarity.mr.input.DocTripleInputFormat;
import de.mpii.docsimilarity.mr.input.docpair.SparseDocVector;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.QidCwidsRelSimilarity;
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
public class IntegratedDoctripleEval extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(IntegratedDoctripleEval.class);

    public static class Constants {

        public static String SEP = "/";

        public static String SIMILARITY = "similarity";

        public static String EVALRESULT = "eval";

        public static String AVE_PRECISION = "averageprecision";
    }

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
        options.addOption("t", "tripledir", true, "doc triple directory");
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
        String tripledir = cmd.getOptionValue('t');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * job phase 1: compute doc similarity
         */
        Job job = new Job(getConf(), IntegratedDoctripleEval.class.getSimpleName() + " integrated doc triple similarity evaluation phase 1: " + expname);
        job.setJarByClass(IntegratedDoctripleEval.class);
        job.setNumReduceTasks(1);

        for (String qrelyear : new String[]{"wt09", "wt10", "wt11", "wt12", "wt13", "wt14"}) {
            DistributedCache.addCacheFile(new Path(qrelfiledir + "/qrels.adhoc." + qrelyear).toUri(), job.getConfiguration());
        }
        // recursively add the files to input
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (!fs.exists(new Path(docvectordir))) {
            logger.error("Input directory non-exist: " + docvectordir);
            return 0;
        }
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
            // for LSA
            int rank = Integer.parseInt(cmd.getOptionValue('k'));
            outputpath = new Path(outputdir + Constants.SEP + Constants.SIMILARITY + Constants.SEP + "rank" + rank);
        } else {
            outputpath = new Path(outputdir + Constants.SEP + Constants.SIMILARITY);
        }

        if (cmd.hasOption('c')) {
            job.getConfiguration().setInt(GlobalConstants.CARDINALITY_SPARSEVEC, Integer.parseInt(cmd.getOptionValue('c')));
        }

        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.toString() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);
        job.setInputFormatClass(SparseDocVector.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(QidCwidsRelSimilarity.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(QidCwidsRelSimilarity.class);

        job.setMapperClass(PairwiseDocSimilarity.DocSimilarityMapper.class);
        job.setReducerClass(PairwiseDocSimilarity.LabeltypeReducer.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Similarity computation job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        /**
         * based on the similarity, eval on the doc triple
         */
        job = new Job(getConf(), IntegratedDoctripleEval.class.getSimpleName() + " integrated doc triple similarity evaluation phase 2: " + expname);
        job.setJarByClass(IntegratedDoctripleEval.class);
        job.setNumReduceTasks(1);
        // add the similarity file into the distributed cash
        Path similarityPath = new Path(outputpath.toString() + Constants.SEP + "part-r-00000");
        DistributedCache.addCacheFile(similarityPath.toUri(), job.getConfiguration());
        String similarityFilename = similarityPath.getName();
        job.getConfiguration().set(EvaluateDocTriple.Constants.SIMILARITYFILENAME, similarityFilename);
        // recursively add the files to input
        fs = FileSystem.get(job.getConfiguration());
        status_list = fs.listStatus(new Path(tripledir));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }

        if (cmd.hasOption('k')) {
            // for LSA
            int rank = Integer.parseInt(cmd.getOptionValue('k'));
            outputpath = new Path(outputdir + Constants.SEP + Constants.EVALRESULT + EvaluateDocTriple.Constants.SEP + rank);
        } else {
            outputpath = new Path(outputdir + Constants.SEP + Constants.EVALRESULT);
        }

        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.toString() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);
        job.setInputFormatClass(DocTripleInputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(EvaluateDocTriple.DocTriplePrecision.class);
        job.setReducerClass(EvaluateDocTriple.Merger.class);

        startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        String summaryResult = job.getConfiguration().get(Constants.AVE_PRECISION);
        System.out.println("SUMMARY RESULTS for " + expname + "\t" + summaryResult);
        logger.info("Doc triple eval job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new IntegratedDoctripleEval(), args);
        System.exit(exitCode);
    }

}
