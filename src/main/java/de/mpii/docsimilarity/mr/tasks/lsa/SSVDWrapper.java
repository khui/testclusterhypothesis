package de.mpii.docsimilarity.mr.tasks.lsa;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 *
 * @author khui
 */
public class SSVDWrapper extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(SSVDWrapper.class);

    public static class Constants {

        public static String SEP = "/";
        public static String PLAINTXTFILE = "plaintxt";
        public static String OUTDIR = "ssvdout";
    }

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("i", "seqfiledir", true, "seq file directory");
        options.addOption("o", "outputdir", true, "output directory");
        options.addOption("m", "tmpdir", true, "tmp directory");
        options.addOption("k", "rank", true, "rank number 20..200");
        options.addOption("t", "reducedtasknumber", true, "reduced task number:95%..195%");
        options.addOption("l", "log4jconf", true, "log4jconf");
        options.addOption("h", "help", false, "print this message");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h") || cmd.getOptions().length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(0);
        }

        String tmpdir = cmd.getOptionValue('m');
        String inputdir = cmd.getOptionValue('i');
        String outputdir = cmd.getOptionValue('o');
        int rank = Integer.parseInt(cmd.getOptionValue('k'));
        int reduceTasks = Integer.parseInt(cmd.getOptionValue('t'));
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        List<Path> inputpaths = new ArrayList<>();
        Configuration conf = getConf();
        conf.set("mapred.local.dir", tmpdir);
        String libdir = "/user/khui/lib";
        addJars(libdir, conf);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status_list = fs.listStatus(new Path(inputdir));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                String fname = status.getPath().getName();
                if (!fname.startsWith("_")) {
                    inputpaths.add(status.getPath());
                }
            }
        }

//        SSVDSolver solver = new SSVDSolver(conf, inputpaths.toArray(new Path[0]), new Path(outputdir + Constants.SEP + Constants.OUTDIR + Constants.SEP + rank), 10000, rank, 10,
//                reduceTasks);
//        solver.setQ(1);
//        solver.setcUHalfSigma(false);
//        solver.setcVHalfSigma(false);
//        solver.setOverwrite(true);
//        solver.run();
        /**
         * convert the U matrix to plain text cwid-{tid:tfidf}
         */
        Job job = new Job(conf, SSVDWrapper.class.getSimpleName() + " seq2sparsevector");
        job.setJarByClass(SSVDWrapper.class);
        job.setNumReduceTasks(298);
        // recursively add the files to input
        fs = FileSystem.get(job.getConfiguration());
        status_list = fs.listStatus(new Path(outputdir + Constants.SEP + Constants.OUTDIR + Constants.SEP + rank + Constants.SEP + "U"));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                String filename = status.getPath().getName();
                if (!filename.startsWith("_")) {
                    FileInputFormat.addInputPath(job, status.getPath());
                }
            }
        }
        //
        Path outputpath = new Path(outputdir + Constants.SEP + Constants.PLAINTXTFILE + Constants.SEP + rank);
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Seq2SparseVector.class);
        job.setReducerClass(SparseVectorPerQuery.class);

        for (int qid = 1; qid <= 300; qid++) {
            MultipleOutputs.addNamedOutput(job, String.valueOf(qid), TextOutputFormat.class, NullWritable.class, Text.class);
        }

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * read in the sequence file output from mahout ssvd, convert the sequence
     * file to plain text file as input for pairwisedocsimilarity task
     */
    private static class Seq2SparseVector extends Mapper<Text, VectorWritable, IntWritable, Text> {

        private final IntWritable qidWritable = new IntWritable();

        private final Text line = new Text();

        private final DecimalFormat df = new DecimalFormat("#.000000");

        @Override
        public void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String qidcwid = key.toString();
            int qid = Integer.parseInt(qidcwid.split("\\.")[0]);
            qidWritable.set(qid);
            String cwid = qidcwid.split("\\.")[1];
            sb.append(cwid).append(" ");
            Iterable<Vector.Element> elements = value.get().nonZeroes();
            for (Vector.Element kv : elements) {
                int termid = kv.index();
                double weight = kv.get();
                sb.append(termid + 1).append(":").append(df.format(weight)).append(" ");
            }
            line.set(sb.toString());
            context.write(qidWritable, line);
        }

    }

    public static class SparseVectorPerQuery extends Reducer<IntWritable, Text, NullWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            MultipleOutputs mos = new MultipleOutputs(context);
            int qid = key.get();
            for (Text line : values) {
                mos.write(String.valueOf(qid), NullWritable.get(), line);
            }
            logger.info(qid + " finished");
            mos.close();
        }

    }

    private void addJars(String libdir, Configuration conf) throws IOException {
        logger.warn("uploading jars from hdfs, make sure the jars in " + libdir + " are updated");
        int jarcount = 0;
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status_list = fs.listStatus(new Path(libdir));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                String fname = status.getPath().getName();
                if (fname.endsWith("jar")) {
                    Path path2add = new Path(libdir + "/" + fname);
                    DistributedCache.addArchiveToClassPath(path2add, conf, fs);
                    jarcount++;
                }
            }
        }
        logger.info(jarcount + " jars successfully uploaded");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SSVDWrapper(), args);
        System.exit(exitCode);
    }

}
