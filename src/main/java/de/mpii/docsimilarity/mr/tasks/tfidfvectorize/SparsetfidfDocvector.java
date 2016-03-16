package de.mpii.docsimilarity.mr.tasks.tfidfvectorize;

import de.mpii.docsimilarity.mr.input.SequenceFileInputFormatNoSplit;
import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord;
import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.IRScorer;
import de.mpii.docsimilarity.mr.utils.io.ReadTermTermidMap;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
public class SparsetfidfDocvector extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(SparsetfidfDocvector.class);

    private static final DecimalFormat decimalformat = new DecimalFormat("#.00000");

    private static String colname;

    private static class Constants {

        static String SEP = "/";
        static String OUTSUBDIR = "tfidfoneshot";
        static String CWIDS_FILE_NAME = "cwidsfilename";
        static String COL_NAME = "colname";
        static String TERMSTAT_FILENAME = "termstatfile";

    }

    private static enum Counter {

        TOTAL, PAGES, ERRORS, SKIPPEDPAGES, SKIPPEDTERMS, DFFILTERED, NULLBODY
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("o", "outdir", true, "output directory");
        options.addOption("i", "collection", true, "collection directory");
        options.addOption("t", "termstat", true, "termstat file");
        options.addOption("d", "cwids2process", true, "file of cwids for processing");
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
        // "cw09 or cw12"
        colname = cmd.getOptionValue('n');
        String termstatdir = cmd.getOptionValue('t');
        //String cwidsfile = cmd.getOptionValue('d');
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * start setting up job
         */
        Job job = new Job(getConf(), SparsetfidfDocvector.class.getSimpleName() + " generate tfidf sparce docvector for " + colname);
        job.setJarByClass(SparsetfidfDocvector.class);
        job.setNumReduceTasks(5);

        // add cache files
        DistributedCache.addCacheFile(new Path(termstatdir + Constants.SEP + "term-df-cf-sort-cw09").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(termstatdir + Constants.SEP + "term-df-cf-sort-cw12").toUri(), job.getConfiguration());

        // set the variables
        job.getConfiguration().set(GlobalConstants.CWIDS_FILE_NAME_PREFIX, "cwidsqid.");
        job.getConfiguration().set(Constants.TERMSTAT_FILENAME, "term-df-cf-sort-");
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
        Path outputpath = new Path(outputdir + Constants.SEP + Constants.OUTSUBDIR + Constants.SEP + "perqid");
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }
        FileOutputFormat.setOutputPath(job, outputpath);

        job.setInputFormatClass(SequenceFileInputFormatNoSplit.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        for (int qid = 1; qid <= 300; qid++) {
            MultipleOutputs.addNamedOutput(job, String.valueOf(qid), TextOutputFormat.class, NullWritable.class, Text.class);
        }

        job.setMapperClass(VectorizeDoc.class);
        job.setReducerClass(ConcatenateLines.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        return 0;
    }

    private static class VectorizeDoc extends Mapper<IntWritable, BytesWritable, IntWritable, Text> {

        protected final Text selectedTerms = new Text();

        protected final IntWritable queryid = new IntWritable();

        protected int qid;

        protected String cwrecordClass;

        protected String colname, qrelyear;

        protected int docfrequency;

        protected long cardinality;

        protected Path termstatpath = null;

        protected final CleanContentTxt cleanpipeline = new CleanContentTxt(GlobalConstants.MAX_TOKEN_LENGTH);

        // only useful for running on complete dataset
        private static final Set<String> docids = new HashSet<>(150000);

        private static final TIntIntMap termidDf = new TIntIntHashMap(6000000);

        private static final TObjectIntMap<String> termTermid = new TObjectIntHashMap<>(6000000);

        @Override
        public void setup(Context context) throws IOException {
            config(context);
            ReadTermTermidMap.readTermDfCf(termstatpath.toString(), termTermid, null, termidDf, null, false);
        }

        @Override
        public void map(IntWritable key, BytesWritable doc, Mapper.Context context) throws IOException {
            String docid;
            byte[] cwrecord = doc.getBytes();
            try {
                ClueWebWarcRecord cwdoc = (ClueWebWarcRecord) Class.forName(cwrecordClass).newInstance();
                cwdoc.readFields(new DataInputStream(new ByteArrayInputStream(cwrecord)));
                docid = cwdoc.getDocid();
                if (docid != null) {
                    String content = cwdoc.getContent();
                    context.getCounter(Counter.TOTAL).increment(1);
                    if (content.length() > GlobalConstants.MAX_DOC_LENGTH) {
                        logger.error("Skipping " + docid + " for its over-threshold length: " + content.length());
                        context.getCounter(Counter.SKIPPEDPAGES).increment(1);
                        return;
                    }
                    Document webpage = Jsoup.parse(content);
                    if (qid > 0) {
                        if (webpage.body() == null) {
                            logger.error("Skipping " + docid + ": it has null body.");
                            context.getCounter(Counter.NULLBODY).increment(1);
                            return;
                        }
                        String vectorstr = vectorizedoc(docid, webpage, context);
                        selectedTerms.set(vectorstr);
                        context.write(queryid, selectedTerms);
                        context.getCounter(Counter.PAGES).increment(1);
                    }
                }

            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

        private String vectorizedoc(String docid, Document webpage, Context context) {
            TIntDoubleMap termTf = new TIntDoubleHashMap();
            List<String> docterms = cleanpipeline.cleanTxtList(webpage, docid, false);
            StringBuilder sb = new StringBuilder();
            sb.append(docid).append(" ");
            double doclen = docterms.size();
            for (String term : docterms) {
                if (term.length() > GlobalConstants.MAX_TOKEN_LENGTH) {
                    context.getCounter(Counter.SKIPPEDTERMS).increment(1);
                    continue;
                }
                if (termTermid.containsKey(term) && term.length() > 1) {
                    termTf.adjustOrPutValue(termTermid.get(term), 1, 1);
                }
            }
            int[] termids = termTf.keySet().toArray(new int[0]);
            Arrays.sort(termids);
            for (int termid : termids) {
                double tfidf = IRScorer.tfIdf(termTf.get(termid), termidDf.get(termid), doclen, docfrequency);
                if (tfidf > 0 && !Double.isNaN(tfidf) && !Double.isInfinite(tfidf)) {
                    sb.append(termid).append(":").append(decimalformat.format(tfidf)).append(" ");
                }
            }
            return sb.toString();
        }

        private void readInCwids(Path cwidsPath) throws FileNotFoundException {
            Scanner in = new Scanner(new BufferedReader(new FileReader(cwidsPath.toString())));
            try {
                while (in.hasNext()) {
                    String line = in.nextLine();
                    if (line.startsWith("clueweb")) {
                        docids.add(line);
                    }
                }
                logger.info("read in " + docids.size() + " cwids");
            } catch (Exception ex) {
                logger.error("", ex);
            }

        }

        private void config(Context context) {
            String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
            qid = Integer.parseInt(fname.split("-")[0]);
            queryid.set(qid);
            if (qid < 51) {
                qrelyear = "wt09";
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord";
                colname = "cw09";
                docfrequency = GlobalConstants.DOCFREQUENCY_CW09;
                cardinality = GlobalConstants.CARDINALITY_CW09;
            } else if (qid < 101) {
                qrelyear = "wt10";
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord";
                colname = "cw09";
                docfrequency = GlobalConstants.DOCFREQUENCY_CW09;
                cardinality = GlobalConstants.CARDINALITY_CW09;
            } else if (qid < 151) {
                qrelyear = "wt11";
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord";
                colname = "cw09";
                docfrequency = GlobalConstants.DOCFREQUENCY_CW09;
                cardinality = GlobalConstants.CARDINALITY_CW09;
            } else if (qid < 201) {
                qrelyear = "wt12";
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord";
                colname = "cw09";
                docfrequency = GlobalConstants.DOCFREQUENCY_CW09;
                cardinality = GlobalConstants.CARDINALITY_CW09;
            } else if (qid < 251) {
                qrelyear = "wt13";
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord";
                colname = "cw12";
                docfrequency = GlobalConstants.DOCFREQUENCY_CW12;
                cardinality = GlobalConstants.CARDINALITY_CW12;
            } else if (qid < 301) {
                qrelyear = "wt14";
                cwrecordClass = "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord";
                colname = "cw12";
                docfrequency = GlobalConstants.DOCFREQUENCY_CW12;
                cardinality = GlobalConstants.CARDINALITY_CW12;
            } else {
                logger.error("Unrecognized qid: " + qid);
                return;
            }
            logger.info("Processing " + qid);
            try {
                Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String termstatFilename = context.getConfiguration().get(Constants.TERMSTAT_FILENAME) + colname;
                if (localFiles != null) {
                    for (Path file : localFiles) {
                        String pathstr = file.getName();
                        if (pathstr.endsWith(termstatFilename)) {
                            termstatpath = file;
                            logger.info(file.toString() + " will be used");
                        }
                    }
                } else {
                    logger.fatal("local files read in failed");
                }
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

    }

    private static class ConcatenateLines extends Reducer<IntWritable, Text, NullWritable, Text> {

        @Override
        public void reduce(IntWritable queryid, Iterable<Text> lines, Context context) {
            MultipleOutputs mos = new MultipleOutputs(context);
            try {
                int qid = queryid.get();
                for (Text line : lines) {
                    mos.write(String.valueOf(qid), NullWritable.get(), line);
                }
                logger.info(qid + " finished");
                mos.close();
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SparsetfidfDocvector(), args);
        System.exit(exitCode);
    }

}
