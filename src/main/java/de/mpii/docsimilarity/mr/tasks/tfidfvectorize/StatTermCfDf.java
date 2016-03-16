package de.mpii.docsimilarity.mr.tasks.tfidfvectorize;

import de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09InputFormat;
import de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12InputFormat;
import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord;
import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.DFSUtils;
import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
public class StatTermCfDf extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(StatTermCfDf.class);

    private static final String HADOOP_DF_MIN_OPTION = "df.min";
    private static final String HADOOP_DF_MAX_OPTION = "df.max";

    private static class Constants {

        static String SEP = "/";
        static String OUTNAME = "term-df-cf";
    }

    private static enum Counter {

        TOTAL, PAGES, ERRORS, SKIPPEDPAGES, SKIPPEDTERMS, EXTREMEDF, NULL_AFTER_CLEAN
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("r", "root", true, "root directory");
        options.addOption("c", "collection", true, "collection directory");
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

        String inputcols = cmd.getOptionValue('c');
        String outputdir = cmd.getOptionValue('r');
        // "cw09 or cw12"
        String colname = cmd.getOptionValue('n');
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        /**
         * start setting up job
         */
        Job job = new Job(getConf(), StatTermCfDf.class.getSimpleName() + " Phase 1 : compute cf, df for " + colname);
        job.setJarByClass(StatTermCfDf.class);
        job.setNumReduceTasks(32);

        Path[] inputPaths = DFSUtils.recurse(inputcols, "warc.gz", job.getConfiguration());

        Path outputpath = new Path(outputdir + Constants.SEP + Constants.OUTNAME + "-" + colname);
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }

        FileInputFormat.setInputPaths(job, inputPaths);
        FileOutputFormat.setOutputPath(job, outputpath);
        switch (colname) {
            case "cw09":
                job.setInputFormatClass(ClueWeb09InputFormat.class);
                break;
            case "cw12":
                job.setInputFormatClass(ClueWeb12InputFormat.class);
                break;
            default:
                logger.error(colname + " is unsupported data name");
                System.exit(1);
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocFCollectionF.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DocFCollectionF.class);

        job.setMapperClass(Term2CfDfMapper.class);
        job.setCombinerClass(TermCfDfCombiner.class);
        job.setReducerClass(TermCfDfReducer.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        logger.info("Job Phase 1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        /**
         * merge the output and sort the term in descending order of df
         */
        Job mergejob = new Job(getConf(), StatTermCfDf.class.getSimpleName() + " Phase 2: merge & sort for " + colname);
        mergejob.setJarByClass(StatTermCfDf.class);
        mergejob.setNumReduceTasks(1);
        FileSystem fs = FileSystem.get(mergejob.getConfiguration());
        FileStatus[] status_list = fs.listStatus(outputpath);
        if (status_list != null) {
            for (FileStatus status : status_list) {
                String fileName = status.getPath().getName();
                if (fileName.startsWith("_")) {
                    continue;
                }
                FileInputFormat.addInputPath(mergejob, status.getPath());
            }
        }
        outputpath = new Path(outputdir + Constants.SEP + Constants.OUTNAME + "-sort-" + colname);
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }
        FileOutputFormat.setOutputPath(mergejob, outputpath);
        mergejob.setInputFormatClass(TextInputFormat.class);
        mergejob.setMapOutputKeyClass(Text.class);
        mergejob.setMapOutputValueClass(DocFCollectionF.class);
        mergejob.setOutputKeyClass(Text.class);
        mergejob.setOutputValueClass(DocFCollectionF.class);
        mergejob.setMapperClass(ReadInDfCf.class);
        mergejob.setReducerClass(MergeNSortOnDf.class);
        startTime = System.currentTimeMillis();
        mergejob.waitForCompletion(true);
        logger.info("Job Phase 2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        return 0;
    }

    private static class Term2CfDfMapper extends Mapper<Text, ClueWebWarcRecord, Text, DocFCollectionF> {

        private final Text termtxt = new Text();

        private CleanContentTxt cleanpipeline;

        private final DocFCollectionF cfDf = new DocFCollectionF();

        @Override
        public void setup(Context context) {
            cleanpipeline = new CleanContentTxt(GlobalConstants.MAX_TOKEN_LENGTH);
        }

        @Override
        public void map(Text key, ClueWebWarcRecord doc, Context context) {
            String docid = key.toString();
            if (docid != null) {
                try {
                    context.getCounter(Counter.PAGES).increment(1);
                    String content = doc.getContent();
                    if (content.length() > GlobalConstants.MAX_DOC_LENGTH) {
                        logger.error("Skipping " + docid + " for its over-threshold length: " + content.length());
                        context.getCounter(Counter.SKIPPEDPAGES).increment(1);
                        return;
                    }
                    Document webpage = Jsoup.parse(content);
                    TObjectIntMap<String> termTf = new TObjectIntHashMap<>();
                    List<String> terms = cleanpipeline.cleanTxtList(webpage, docid, false);
                    if (terms != null) {
                        for (String term : terms) {
                            if (term.length() > GlobalConstants.MAX_TOKEN_LENGTH) {
                                context.getCounter(Counter.SKIPPEDTERMS).increment(1);
                                continue;
                            }
                            termTf.adjustOrPutValue(term, 1, 1);
                        }
                    } else {
                        context.getCounter(Counter.NULL_AFTER_CLEAN).increment(1);
                    }
                    for (String term : termTf.keys(new String[0])) {
                        termtxt.set(term);
                        cfDf.setCf(termTf.get(term));
                        cfDf.setDf(1);
                        context.write(termtxt, cfDf);
                    }
                } catch (Exception ex) {
                    context.getCounter(Counter.ERRORS).increment(1);
                    logger.error("", ex);
                }

            }
        }

    }

    private static class TermCfDfReducer extends Reducer<Text, DocFCollectionF, Text, DocFCollectionF> {

        private int dfMin, dfMax;

        private final DocFCollectionF cfDfOut = new DocFCollectionF();

        @Override
        public void setup(Reducer.Context context) {
            dfMin = context.getConfiguration().getInt(HADOOP_DF_MIN_OPTION, GlobalConstants.DF_MIN2FILTER);
            dfMax = context.getConfiguration().getInt(HADOOP_DF_MAX_OPTION, Integer.MAX_VALUE);
        }

        @Override
        public void reduce(Text key, Iterable<DocFCollectionF> dfs, Context context) {
            int df = 0;
            long cf = 0;
            DocFCollectionF cfDfIn;
            try {
                Iterator<DocFCollectionF> dfIterator = dfs.iterator();
                while (dfIterator.hasNext()) {
                    cfDfIn = dfIterator.next();
                    df += cfDfIn.getDf();
                    cf += cfDfIn.getCf();
                }
                if (df <= dfMax && df >= dfMin) {
                    cfDfOut.setCf(cf);
                    cfDfOut.setDf(df);
                    context.write(key, cfDfOut);
                } else {
                    context.getCounter(Counter.EXTREMEDF).increment(1);
                }
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

    }

    private static class TermCfDfCombiner extends Reducer<Text, DocFCollectionF, Text, DocFCollectionF> {

        private final DocFCollectionF cfDf = new DocFCollectionF();

        @Override
        public void reduce(Text key, Iterable<DocFCollectionF> dfs, Context context) {
            int df = 0;
            long cf = 0;

            try {
                Iterator<DocFCollectionF> dfIterator = dfs.iterator();
                while (dfIterator.hasNext()) {
                    cf += dfIterator.next().getCf();
                    df++;
                }
                cfDf.setCf(cf);
                cfDf.setDf(df);
                context.write(key, cfDf);
            } catch (Exception ex) {
                context.getCounter(Counter.ERRORS).increment(1);
                logger.error("", ex);
            }
        }

    }

    private static class ReadInDfCf extends Mapper<LongWritable, Text, Text, DocFCollectionF> {

        private final Text term = new Text();

        private final DocFCollectionF cfDf = new DocFCollectionF();

        @Override
        public void map(LongWritable offset, Text line, Context context) {
            int df;
            long cf;
            try {
                String[] cols = line.toString().split("\t");
                if (cols.length == 3) {
                    term.set(cols[0]);
                    df = Integer.parseInt(cols[1]);
                    cf = Long.parseLong(cols[2]);
                    cfDf.setCf(cf);
                    cfDf.setDf(df);
                    context.write(term, cfDf);
                } else {
                    logger.error("incorrect column number: " + line);
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }

    }

    private static class MergeNSortOnDf extends Reducer<Text, DocFCollectionF, Text, DocFCollectionF> {

        private final TObjectIntMap<String> termDf = new TObjectIntHashMap<>(6000000);

        private final TObjectLongMap<String> termCf = new TObjectLongHashMap<>(6000000);

        private final Text termtxt = new Text();

        private final DocFCollectionF cfdfWritable = new DocFCollectionF();

        @Override
        public void reduce(Text term, Iterable<DocFCollectionF> cfdfs, Context context) {
            Iterator<DocFCollectionF> it = cfdfs.iterator();
            DocFCollectionF cfdf;
            while (it.hasNext()) {
                cfdf = it.next();
                termDf.put(term.toString(), cfdf.getDf());
                termCf.put(term.toString(), cfdf.getCf());
            }
        }

        @Override
        public void cleanup(Context context) {
            try {
                String[] terms = termDf.keySet().toArray(new String[0]);
                // sort termTf in descending order of their collection frequency
                Arrays.sort(terms, new Comparator<String>() {

                    @Override
                    public int compare(String t, String u) {
                        long dft = termDf.get(t);
                        long dfu = termDf.get(u);
                        if (dfu < dft) {
                            return -1;
                        } else if (dfu > dft) {
                            return +1;
                        }
                        return 0;
                    }
                });

                for (String term : terms) {
                    termtxt.set(term);
                    cfdfWritable.setCf(termCf.get(term));
                    cfdfWritable.setDf(termDf.get(term));
                    context.write(termtxt, cfdfWritable);
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }

        }
    }

    private static class DocFCollectionF implements Writable {

        private long cf;

        private int df;

        public void setCf(long cf) {
            this.cf = cf;
        }

        public void setDf(int df) {
            this.df = df;
        }

        public long getCf() {
            return this.cf;
        }

        public int getDf() {
            return this.df;
        }

        @Override
        public void write(DataOutput d) throws IOException {
            d.writeLong(cf);
            d.writeInt(df);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            this.cf = di.readLong();
            this.df = di.readInt();
        }

        @Override
        public String toString() {
            return (String.valueOf(this.df) + "\t" + String.valueOf(this.cf));
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StatTermCfDf(), args);
        System.exit(exitCode);
    }

}
