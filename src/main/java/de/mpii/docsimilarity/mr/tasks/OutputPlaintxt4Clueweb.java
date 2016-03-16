package de.mpii.docsimilarity.mr.tasks;

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord;
import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * refers org.clueweb.clueweb12.app.ComputeTermStatistics
 *
 * @author khui
 */
public class OutputPlaintxt4Clueweb extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(OutputPlaintxt4Clueweb.class);

    private static class Constants {

        static String SEP = "/";
        static String CW_PLAINTXT = "Plaintxt";
        static String COL_NAME = "colname";
        static final int MAX_DOC_LENGTH = 512 * 1024; // Skip document if long than this.
        static final int MAX_TOKEN_LENGTH = 64;       // Throw away tokens longer than this.

    }

    private static enum Counter {

        TOTAL, PAGES, ERRORS, SKIPPEDPAGES, SKIPPEDTERMS, DFFILTERED
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("o", "outdir", true, "output directory");
        options.addOption("i", "inputdir", true, "collection directory");
        options.addOption("l", "log4jconf", true, "log4jconf");
        options.addOption("n", "dataname", true, "name of collection to deal with");
        options.addOption("h", "help", false, "print this message");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h") || cmd.getOptions().length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(0);
        }

        String inputdir = cmd.getOptionValue('i');
        String outputdir = cmd.getOptionValue('o');
        // "cw09 or cw12 or cw4y"
        String colname = cmd.getOptionValue('n');
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * start setting up job
         */
        Job job = new Job(getConf(), OutputPlaintxt4Clueweb.class.getSimpleName() + " extract plaintxt for judged cwdocs " + colname);
        job.setJarByClass(OutputPlaintxt4Clueweb.class);
        job.setNumReduceTasks(201);

        // recursively add the files to input
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus[] status_list = fs.listStatus(new Path(inputdir));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                String filename = status.getPath().getName();
                if (!filename.startsWith("_")) {
                    FileInputFormat.addInputPath(job, status.getPath());
                }
            }
        }
        // set output path
        Path outputpath = new Path(outputdir + Constants.SEP + Constants.CW_PLAINTXT + "-" + colname);
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }
        FileOutputFormat.setOutputPath(job, outputpath);
        // set map reduce tasks
        job.setInputFormatClass(SequenceFileInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(ReadCluewebRecords.class);
        job.setReducerClass(ConcatenateLines.class);

        for (int qid = 1; qid <= 300; qid++) {
            MultipleOutputs.addNamedOutput(job, String.valueOf(qid), TextOutputFormat.class, NullWritable.class, Text.class);
        }

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * input: cwid - cwrecord output: qid - cwid term list
     */
    private static class ReadCluewebRecords extends Mapper<IntWritable, BytesWritable, IntWritable, Text> {

        private final Text doctxt = new Text();

        private int qid;

        private String cwrecordClass;

        private CleanContentTxt cleanpipeline;

        @Override
        public void setup(Context context) {
            String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
            qid = Integer.parseInt(fname.split("-")[0]);
            if (qid >= 1 && qid <= 200) {
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord";
            } else if (qid > 200 && qid <= 300) {
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord";
            } else {
                logger.error("Unrecognized qid: " + qid);
                return;
            }
            logger.info("Processing " + qid);
            cleanpipeline = new CleanContentTxt(Constants.MAX_TOKEN_LENGTH);
        }

        @Override
        public void map(IntWritable key, BytesWritable doc, Context context) throws IOException {
            String docid;
            byte[] cwrecord = doc.getBytes();
            try {
                ClueWebWarcRecord cwdoc = (ClueWebWarcRecord) Class.forName(cwrecordClass).newInstance();
                cwdoc.readFields(new DataInputStream(new ByteArrayInputStream(cwrecord)));
                docid = cwdoc.getDocid();
                if (docid != null) {
                    context.getCounter(Counter.PAGES).increment(1);
                    String content = cwdoc.getContent();
                    Document webpage = Jsoup.parse(content);
                    if (qid > 0) {
                        String cwidLine = docid + "\n" + cleanpipeline.cleanTxtList(webpage, docid, false);
                        doctxt.set(cwidLine);
                        context.write(key, doctxt);
                    }
                }

            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
    }

    private static class ConcatenateLines extends Reducer<IntWritable, Text, NullWritable, Text> {

        @Override
        public void reduce(IntWritable queryid, Iterable<Text> lines, Context context) {
            MultipleOutputs mos = new MultipleOutputs(context);
            int qid = queryid.get();
            try {
                Iterator<Text> lineiterator = lines.iterator();
                while (lineiterator.hasNext()) {
                    mos.write(String.valueOf(qid), NullWritable.get(), lineiterator.next());
                }
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new OutputPlaintxt4Clueweb(), args);
        System.exit(exitCode);
    }

}
