package de.mpii.docsimilarity.mr.tasks.lsa;

import java.io.IOException;
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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * refer to the SequenceFileReadDemo from
 * http://examples.oreilly.com/0636920010388/ch04.zip
 *
 * @author khui
 */
public class ProcessOutputFromSSVD extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(ProcessOutputFromSSVD.class);

    @Override
    public int run(String[] args) throws Exception {
        // command-line parsing
        Options options = new Options();
        options.addOption("i", "seqfiledir", true, "seq file directory");
        options.addOption("o", "outputdir", true, "output directory");
        options.addOption("l", "log4jconf", true, "log4jconf");
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
        String log4jconf = cmd.getOptionValue('l');
        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path indirpath = new Path(inputdir);
        FileStatus[] status_list = fs.listStatus(indirpath);
        int fileprocessedCount = 0;
        if (status_list != null) {
            for (FileStatus status : status_list) {
                String fname = status.getPath().getName();
                if (!fname.startsWith("_")) {
                    processSeqFile(fs, status.getPath(), conf);
                    logger.info("Finished processing " + status.getPath().toString());
                    fileprocessedCount++;
                    if (fileprocessedCount > 0) {
                        break;
                    }
                }
            }
        }

        return 0;
    }

    private void processSeqFile(FileSystem fs, Path path, Configuration conf) throws IOException {
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ProcessOutputFromSSVD(), args);
        System.exit(exitCode);
    }
}
