package de.mpii.docsimilarity.mr.tasks.lsa;

import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import de.mpii.docsimilarity.mr.utils.io.ReadSparseVector;
import gnu.trove.map.TIntDoubleMap;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 *
 * @author khui
 */
public class DocTermMatrix2Seq extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(DocTermMatrix2Seq.class);

    public static class Constants {

        public static String SEP = "/";
        public static String SEQFILE = "lsaseq";
        public static String CARDINALITY = "cardinality";
        public static String OUTDIR = "outputdir";
    }

    private static enum Counter {

        TOTAL, ERRORS,
    };

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("d", "docvectorfile", true, "docvector file");
        options.addOption("o", "outputdir", true, "output directory");
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
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        /**
         * start setting up job
         */
        Job job = new Job(getConf(), DocTermMatrix2Seq.class.getSimpleName() + " convert doc-term matrix to sequense file " + expname);
        job.setJarByClass(DocTermMatrix2Seq.class);
        job.setNumReduceTasks(0);

        // recursively add the files to input
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus[] status_list = fs.listStatus(new Path(docvectordir));
        if (status_list != null) {
            for (FileStatus status : status_list) {
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }

        job.getConfiguration().set(Constants.OUTDIR, outputdir + Constants.SEP + Constants.SEQFILE);
        Path outputpath = new Path(outputdir + Constants.SEP + Constants.SEQFILE);
        if (FileSystem.get(getConf()).exists(outputpath)) {
            logger.warn(outputpath.getName() + " is being removed");
            FileSystem.get(getConf()).delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);
        job.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setMapperClass(DoctermMatrix2Seq.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        logger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    private static class DoctermMatrix2Seq extends Mapper<LongWritable, Text, Text, VectorWritable> {

        private int cardinality;

        private String inputfilename;

        private SequenceFile.Writer writer;

        private int qid;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            inputfilename = ((FileSplit) context.getInputSplit()).getPath().getName();
            qid = Integer.parseInt(inputfilename.split("-")[0]);
            cardinality = getCardinality(qid);
            String outputdir = conf.get(Constants.OUTDIR);
            Path path = new Path(outputdir + Constants.SEP + inputfilename);
            FileSystem fs = path.getFileSystem(conf);
            logger.info(path.toString());
            // for each document ,we write as a key value pair, the key is the cwid, the
            // value is the randomacesssparsevector from {tid:tfidf}
            writer = SequenceFile.createWriter(fs, conf, path, Text.class, VectorWritable.class);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, TIntDoubleMap> cwidTermTfidf = ReadSparseVector.tidTfidfMapLine(value.toString());
            Text cwidTxt = new Text();
            VectorWritable vectorwritable = new VectorWritable();
            if (cwidTermTfidf.size() != 1) {
                logger.error("cwidTermTfidf size is not equal 1: " + cwidTermTfidf.size());
                return;
            }
            for (String cwid : cwidTermTfidf.keySet()) {
                String qidcwid = String.valueOf(qid) + "." + cwid;
                cwidTxt.set(qidcwid);
                Vector docvector = convert2vector(cwidTermTfidf.get(cwid), cardinality);
                vectorwritable.set(docvector);
                writer.append(cwidTxt, vectorwritable);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            writer.close();
        }

        public Vector convert2vector(TIntDoubleMap doc, int cardinality) {
            Vector docVector = new RandomAccessSparseVector(cardinality);
            try {
                for (int tid : doc.keySet().toArray()) {
                    docVector.set(tid, doc.get(tid));
                }
            } catch (Exception ex) {
                logger.error("", ex);
            }
            return docVector;
        }

    }

    public static int getCardinality(int qid) {
        int cardinality;
        if (qid < 51) {
            cardinality = GlobalConstants.DOCFREQUENCY_CW09;// term number for cw09
        } else if (qid < 101) {
            cardinality = GlobalConstants.DOCFREQUENCY_CW09;// term number for cw09
        } else if (qid < 151) {
            cardinality = GlobalConstants.DOCFREQUENCY_CW09;// term number for cw09
        } else if (qid < 201) {
            cardinality = GlobalConstants.DOCFREQUENCY_CW09;// term number for cw09
        } else if (qid < 251) {
            cardinality = GlobalConstants.DOCFREQUENCY_CW12;// term number for cw12
        } else if (qid < 301) {
            cardinality = GlobalConstants.DOCFREQUENCY_CW12;// term number for cw12
        } else {
            cardinality = GlobalConstants.DOCFREQUENCY_CW12;
        }
        return cardinality;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DocTermMatrix2Seq(), args);
        System.exit(exitCode);
    }

}
