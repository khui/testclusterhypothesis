package de.mpii.docsimilarity.mr.tasks.qe;

import de.mpii.docsimilarity.mr.input.SequenceFileInputFormatNoSplit;
import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord;
import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.io.ReadQrel;
import de.mpii.docsimilarity.tasks.qe.SelectQETerms;
import de.mpii.docsimilarity.tasks.qe.SelectTerms4Relevance;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntDoubleMap;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
public class SelectQExpTermWeight extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(SelectQExpTermWeight.class);

    private static String colname;

    private static class Constants {

        static String SEP = "/";
        static String OUTFILENAME = "relevanceTfidfRatio";
        static String TERMSTAT_FILENAME = "termstatfile";
        static String QREL_FILE_NAME = "qrelfilename";
        static String NUM_TERM2SELECT_FROMDOC = "numofterms2select";
        static String COL_NAME = "colname";
        static String SELECTED_DOC_TERMS = "docTerms";
        static String WINDOWS_LEN = "windowwidth";

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
        options.addOption("t", "termstat", true, "term-df-cf file");
        options.addOption("q", "adhocqrelfile", true, "adhoc qrel file");
        options.addOption("l", "log4jconf", true, "log4jconf");
        options.addOption("n", "dataname", true, "name of collection to deal with");
        options.addOption("w", "windowlens", true, "the length of the window, one side");
        options.addOption("p", "proximityTermNum", true, "number of proximity terms to select");
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
        String qreldir;
        if (cmd.hasOption('q')) {
            qreldir = cmd.getOptionValue('q');
        } else {
            qreldir = "/user/khui/qrel";
        }
        String log4jconf = cmd.getOptionValue('l');
        int windowslen = Integer.parseInt(cmd.getOptionValue('w'));
        int numOfterms2select = Integer.parseInt(cmd.getOptionValue('p'));
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * phase 1: select terms that can best distinguish relevant/irrelevant
         * terms
         */
        Job job = new Job(getConf(), SelectQExpTermWeight.class.getSimpleName() + ": select query expansion terms on " + colname);
        job.setJarByClass(SelectQExpTermWeight.class);
        job.setNumReduceTasks(1);

        // add cache files
        DistributedCache.addCacheFile(new Path(termstatdir + Constants.SEP + "term-df-cf-sort-cw09").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(termstatdir + Constants.SEP + "term-df-cf-sort-cw12").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(qreldir + Constants.SEP + "qrels.adhoc.wt09").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(qreldir + Constants.SEP + "qrels.adhoc.wt10").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(qreldir + Constants.SEP + "qrels.adhoc.wt11").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(qreldir + Constants.SEP + "qrels.adhoc.wt12").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(qreldir + Constants.SEP + "qrels.adhoc.wt13").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(qreldir + Constants.SEP + "qrels.adhoc.wt14").toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path("/user/khui/query/wtff.xml").toUri(), job.getConfiguration());
        // set the variables
        job.getConfiguration().set(GlobalConstants.CWIDS_FILE_NAME_PREFIX, "cwidsqid.");
        job.getConfiguration().set(Constants.TERMSTAT_FILENAME, "term-df-cf-sort-");
        job.getConfiguration().set(Constants.QREL_FILE_NAME, "qrels.adhoc.");
        job.getConfiguration().setInt(Constants.NUM_TERM2SELECT_FROMDOC, numOfterms2select);
        job.getConfiguration().setInt(Constants.WINDOWS_LEN, windowslen);
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

        // set output path for query terms
        String outputdirStr = outputdir + Constants.SEP + "TEMPDIR";

        Path outputPath = new Path(outputdirStr);
        if (FileSystem.get(getConf()).exists(outputPath)) {
            logger.warn(outputPath.toUri() + " is being removed");
            FileSystem.get(getConf()).delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        // set map reduce tasks
        job.setInputFormatClass(SequenceFileInputFormatNoSplit.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SelectQueryTerms.class);
        job.setReducerClass(ConcatenateLines.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

//        /**
//         * rename and move the generated expanded query term file
//         */
//        Path outputFilePath = new Path(outputdir + Constants.SEP + Constants.OUTFILENAME);
//        fs.rename(new Path(outputdirStr + Constants.SEP + "part-r-00000"), outputFilePath);
//        //fs.deleteOnExit(outputPath);
//        logger.info("Copy query expand term file finished.");
        return 0;
    }

    /**
     * input: cwid - cwrecord output: qid - cwid term:weight list
     */
    private static class SelectQueryTerms extends Mapper<IntWritable, BytesWritable, IntWritable, Text> {

        private final Text querytermweights = new Text();

        private final IntWritable queryid = new IntWritable();

        private int numOfTerms2Select;

        private Map<String, TIntList> cwidRels = null;

        private final Map<String, Document> cwidWebpages = new HashMap<>();

        private int qid;

        private String cwrecordClass;

        private String colname, qrelyear;

        private int docfrequency;

        private long cardinality;

        private Path termstatpath = null, querypath = null;

        private SelectQETerms termSelector;

        @Override
        public void setup(Context context) throws FileNotFoundException {
            String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
            CleanContentTxt cleanpipeline = new CleanContentTxt(GlobalConstants.MAX_TOKEN_LENGTH);
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
                String qrelfilename = context.getConfiguration().get(Constants.QREL_FILE_NAME) + qrelyear;
                numOfTerms2Select = context.getConfiguration().getInt(Constants.NUM_TERM2SELECT_FROMDOC, 30);
                if (localFiles != null) {
                    for (Path file : localFiles) {
                        String pathstr = file.getName();
                        if (pathstr.endsWith(termstatFilename)) {
                            termstatpath = file;
                            logger.info(file.toString() + " will be used");
                        } else if (pathstr.endsWith("wtff.xml")) {
                            querypath = file;
                            logger.info(file.toString() + " will be used");
                        } else if (pathstr.endsWith(qrelfilename)) {
                            cwidRels = ReadQrel.readInAdhocQrel(file, qid);
                        }
                    }
                } else {
                    logger.fatal("local files read in failed");
                }

            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
            termSelector = new SelectTerms4Relevance(termstatpath.toString(), querypath.toString(), qid, docfrequency, cardinality, cwidRels, cleanpipeline);
            //new QueryClarityTerms(termstatpath, querypath, qid, docfrequency, cardinality, cleanpipeline);
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
                        cwidWebpages.put(docid, webpage);
                        context.getCounter(Counter.PAGES).increment(1);
                    }
                }

            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

        @Override
        public void cleanup(Context context) throws FileNotFoundException, IOException, InterruptedException {
            TIntDoubleMap selectedQexpTerms = termSelector.getQueryRelatedTerms(qid, cwidWebpages, numOfTerms2Select);
            logger.info(selectedQexpTerms.size() + " terms selected.");
            querytermweights.set(termSelector.toString(selectedQexpTerms, numOfTerms2Select));
            context.write(queryid, querytermweights);
        }
    }

    private static class ConcatenateLines extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable queryid, Iterable<Text> lines, Context context) {
            try {
                Iterator<Text> lineiterator = lines.iterator();
                while (lineiterator.hasNext()) {
                    context.write(queryid, lineiterator.next());
                }
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SelectQExpTermWeight(), args);
        System.exit(exitCode);
    }

}
