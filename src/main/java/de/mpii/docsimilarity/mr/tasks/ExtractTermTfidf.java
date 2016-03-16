package de.mpii.docsimilarity.mr.tasks;

import de.mpii.docsimilarity.mr.input.SequenceFileInputFormatNoSplit;
import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord;
import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.IRScorer;
import de.mpii.docsimilarity.tasks.doc2vec.SelectTerms;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Extract term:tfidf from all document being labeled in TREC Webtrack. In
 * particular, we extract term:tfidf from qrel 11,12,13,14
 *
 * The output contains one term\t tfidf pair per line.
 *
 * Total extracted term number: 755319
 *
 * @author khui
 */
public class ExtractTermTfidf extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(ExtractTermTfidf.class);

    private static String colname;

    private static class Constants {

        static String SEP = "/";
        static String OUTSUBDIR = "termTfidf";
        static String TERMSTAT_FILENAME = "termstatfile";
        static String COL_NAME = "colname";

    }

    private static enum Counter {

        TOTAL, PAGES, ERRORS, SKIPPEDPAGES, NULLBODY
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("o", "outdir", true, "output directory");
        options.addOption("i", "collection", true, "collection directory");
        options.addOption("t", "termstat", true, "termstat file");
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
        // "cw09, cw12 or cw4y"
        colname = cmd.getOptionValue('n');
        String termstatdir = cmd.getOptionValue('t');
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * start setting up job
         */
        Job job = new Job(getConf(), ExtractTermTfidf.class.getSimpleName() + ": extract term-tfidf from all labeled docs on " + colname);
        job.setJarByClass(ExtractTermTfidf.class);
        job.setNumReduceTasks(1);

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
        Path outputpath = new Path(outputdir + Constants.SEP + Constants.OUTSUBDIR + "-" + colname);
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.toString() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }
        FileOutputFormat.setOutputPath(job, outputpath);
        // set map reduce tasks
        job.setInputFormatClass(SequenceFileInputFormatNoSplit.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(ExtractDocTerms.class);
        job.setCombinerClass(TermTfidfReducer.class);
        job.setReducerClass(TermTfidfReducer.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    private static class ExtractDocTerms extends Mapper<IntWritable, BytesWritable, Text, DoubleWritable> {

        protected UniqTermTfidf termSelector;

        protected final Text term = new Text();

        protected final DoubleWritable tfidf = new DoubleWritable();

        protected int qid;

        protected String cwrecordClass;

        protected String colname, qrelyear;

        protected int docfrequency;

        protected long cardinality;

        protected Path termstatpath = null;

        protected final CleanContentTxt cleanpipeline = new CleanContentTxt(GlobalConstants.MAX_TOKEN_LENGTH);

        @Override
        public void setup(Mapper.Context context) {
            try {
                config(context);
                termSelector = new UniqTermTfidf(termstatpath.toString(), qid, cleanpipeline, false);
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
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
                        List<String> docterms = cleanpipeline.cleanTxtList(webpage, docid, true);
                        TObjectDoubleMap<String> termTfidf = termSelector.extractTermTfidf(docterms);
                        context.getCounter(Counter.PAGES).increment(1);
                        for (String termstr : termTfidf.keySet()) {
                            term.set(termstr);
                            tfidf.set(termTfidf.get(termstr));
                            context.write(term, tfidf);
                        }
                    }
                }

            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

        private class UniqTermTfidf extends SelectTerms {

            public UniqTermTfidf(String termdfpath, int qid, CleanContentTxt inclean, boolean toStem) throws FileNotFoundException {
                super(termdfpath, null, qid, inclean, toStem);
            }

            public TObjectDoubleMap<String> extractTermTfidf(List<String> docterms) {
                TObjectDoubleMap<String> termTfidf = new TObjectDoubleHashMap<>();
                TIntDoubleMap tidIdf = new TIntDoubleHashMap();
                double idf;
                for (String term : docterms) {
                    if (term.length() > GlobalConstants.MAX_TOKEN_LENGTH) {
                        continue;
                    }
                    if (!termTermid.containsKey(term)) {
                        continue;
                    }
                    int tid = termTermid.get(term);
                    if (!tidIdf.containsKey(tid)) {
                        double df = termidDf.get(tid);
                        idf = IRScorer.idf(df, docfrequency);
                        tidIdf.put(tid, idf);
                    } else {
                        idf = tidIdf.get(tid);
                    }
                    termTfidf.adjustOrPutValue(term, idf, idf);
                }
                return termTfidf;
            }

        }

        protected void config(Context context) {
            String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
            qid = Integer.parseInt(fname.split("-")[0]);
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

    private static class TermTfidfReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable tfidfWritable = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> tfidfs, Context context) {
            double tfidf = 0;
            try {
                Iterator<DoubleWritable> tfidfIterator = tfidfs.iterator();
                while (tfidfIterator.hasNext()) {
                    tfidf += tfidfIterator.next().get();
                }
                tfidfWritable.set(tfidf);
                context.write(key, tfidfWritable);
            } catch (IOException | InterruptedException ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            } catch (Exception ex) {
                logger.error("", ex);
                context.getCounter(Counter.ERRORS).increment(1);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ExtractTermTfidf(), args);
        System.exit(exitCode);
    }
}
