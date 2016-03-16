package de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec;

import de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec.UniqQDRecord.MethodUnit;
import de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec.UniqQDRecord.QueryDocTerms;
import de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec.UniqQDRecord.QueryDocVectors;
import de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec.WordV2DocVImpls.AvgTermVectors;
import de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec.WordV2DocVImpls.ConcatTermVectors;
import gnu.trove.list.TFloatList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Given documents, with their representative termWeight, generate the doc
 * vector with the average of the selected termWeight' vector
 *
 * @author khui
 */
public class ComputeWordV2DocV extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(ComputeWordV2DocV.class);

    public static class Constants {

        static String SEP = "/";
        //Avg, Concat
        static String VECTORIZE_METHOD = "Avg";

    }

    private final String hdfsDoctermsDir = "/user/khui/doc2vec/selectedTerms";
    private final String localDoctermDir = "/GW/D5data-2/khui/cw-docvector-termdf/wvector2docvector/selectedTerms";
    private final String hdfsDocvecDir = "/user/khui/doc2vec/averageW2v";
    private final String localDocvecDir = "/GW/D5data-2/khui/cw-docvector-termdf/wvector2docvector/averageW2v";

    /**
     * This method is to concatenate different unit document vector from
     * different methods to generate longer doc vector. Given the method name
     * and parameters for the ultimate concatenated vector, we firstly generate
     * the unit doc vector for each of the component method, afterwards
     * concatenate the vector according to the alphabetical order of the unit
     * method name.
     *
     * The format for the input combination method is:
     * unitmethodname1-windowlen1-termnum1.unitmethodname2-windowlen2-termnum2....
     * for multiple methods, or unitmethodname-windowlen-termnum for single
     * method. The generated concatenating vector is named correspondingly as
     * follows:
     * unitmethodname1_unitmethodname2-windowlen1_windowlen2-termnum1_termnum2
     *
     * @param combMethodParams
     * @param fs
     * @param w2v
     * @throws IOException
     * @throws FileNotFoundException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void concatenateVectors(String combMethodParams, FileSystem fs, ExecutorService executor) throws IOException, FileNotFoundException, InterruptedException, ExecutionException {
        logger.info("Start processing " + combMethodParams);
        String[] unitMethodParams = combMethodParams.split("\\.");
        int unitNumber = unitMethodParams.length;
        List<MethodUnit> methodUnits = new ArrayList<>();
        TIntObjectMap<List<QueryDocTerms>> qidDocTerms;
        // setup executor and completionservice
        CompletionService<Map<String, float[]>> service = new ExecutorCompletionService(executor);
        int taskCounter = 0;
        String outSuffix = null;
        // setup methodunit for all component method
        for (String methodstr : unitMethodParams) {
            methodUnits.add(new MethodUnit(methodstr, Constants.VECTORIZE_METHOD));
        }
        // each methodunit will convert the corresponding terms to vector
        // afterward, we concatenate all different vectors
        for (MethodUnit methodUnit : methodUnits) {
            outSuffix = methodUnit.getOutdirSuffix();
            String docvecdir = localDocvecDir + Constants.SEP + outSuffix;
            // if the required docvector already exists, later on we can directly read the doc vector in
            if (new File(docvecdir).exists()) {
                logger.info(methodUnit.getMethodParam() + " is ready");
                continue;
            }
            // copy the doc terms from hdfs to local
            methodUnit.readInDocterms(fs, hdfsDoctermsDir, localDoctermDir);
            // read in the doc terms
            qidDocTerms = readDocSelectedTerms(methodUnit.getIndirSuffix(), methodUnit.getMethodParam());
            // keep submitting tasks
            for (int qid : qidDocTerms.keys()) {
                if (qidDocTerms.get(qid).isEmpty()) {
                    logger.error("For " + qid + " the qidDocTerms is empty.");
                }
                switch (Constants.VECTORIZE_METHOD) {
                    case "Avg":
                        service.submit(new AvgTermVectors(qidDocTerms.get(qid), methodUnit.getTopN()));
                        break;
                    case "Concat":
                        service.submit(new ConcatTermVectors(qidDocTerms.get(qid), methodUnit.getTopN()));
                        break;
                }
                taskCounter++;
            }
            logger.info(methodUnit.getMethodParam() + " is submitted " + taskCounter);
        }
        outputDocvecUnit(service, taskCounter);
        logger.info("All requried docvectors are ready.");
        // read in all requried docvector for unique method
        TIntObjectMap<Map<String, List<QueryDocVectors>>> qidCwidQdv = new TIntObjectHashMap<>();
        for (MethodUnit methodUnit : methodUnits) {
            outSuffix = methodUnit.getOutdirSuffix();
            String docvecdir = localDocvecDir + Constants.SEP + outSuffix;
            String methodParam = methodUnit.getMethodParam();
            readinDocvecDir(qidCwidQdv, docvecdir, methodParam);
        }
        PrintStream ps = null;
        QueryDocVectors qdv;

        if (unitNumber > 1) {
            // concatenate to generate higer dimensional vector
            for (int qid : qidCwidQdv.keys()) {
                String outdir = null;
                for (String cwid : qidCwidQdv.get(qid).keySet()) {
                    if (qidCwidQdv.get(qid).get(cwid).size() != unitNumber) {
                        logger.error("Wrong number of unit doc vector available: " + qid + " " + cwid + " " + qidCwidQdv.get(qid).get(cwid).size());
                        continue;
                    }
                    qdv = new QueryDocVectors(Constants.VECTORIZE_METHOD, cwid, qid, qidCwidQdv.get(qid).get(cwid));
                    if (outdir == null) {
                        outSuffix = qdv.getOutdirSuffix();
                        outdir = localDocvecDir + Constants.SEP + outSuffix + Constants.SEP;
                        if (!new File(outdir).exists()) {
                            new File(outdir).mkdirs();
                        }
                        ps = new PrintStream(outdir + qid);
                    }
                    ps.println(qdv.toString());
                }
                ps.close();
            }
        }
        logger.info("Finished concatenating " + combMethodParams);
        writeToHdfs(fs, outSuffix);
    }

    private boolean writeToHdfs(FileSystem fs, String outSuffix) throws IOException {
        String hdfsoutdir = hdfsDocvecDir + Constants.SEP + outSuffix;
        String localoutdir = localDocvecDir + Constants.SEP + outSuffix;
        if (fs.exists(new Path(hdfsoutdir))) {
            logger.error(hdfsoutdir + " already existed and is removed");
            fs.delete(new Path(hdfsoutdir), true);
        }
        // copy the docterms file from hdfs
        if (!new File(localoutdir).exists()) {
            logger.error(localoutdir + " not exists!");
            return false;
        }
        fs.copyFromLocalFile(new Path(localoutdir), new Path(hdfsoutdir));
        logger.info("Finished copy2Hdfs into: " + hdfsoutdir);
        return true;
    }

    private void readinDocvecDir(TIntObjectMap<Map<String, List<QueryDocVectors>>> qidCwidQdv, String docvecdir, String methodParam) throws FileNotFoundException, IOException {
        File dir = new File(docvecdir);
        BufferedReader br;
        TFloatList vecElements = new TFloatArrayList();
        for (File f : dir.listFiles()) {
            int qid = Integer.parseInt(f.getName());
            if (!qidCwidQdv.containsKey(qid)) {
                qidCwidQdv.put(qid, new HashMap<String, List<QueryDocVectors>>());
            }
            br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
            while (br.ready()) {
                String line = br.readLine();
                String[] cols = line.split(" +");
                if (cols.length < 2) {
                    System.err.println(docvecdir + " " + qid + ": no vector computed " + line);
                    continue;
                }
                String cwid = cols[0];
                for (int i = 1; i < cols.length; i++) {
                    String[] idxval = cols[i].trim().split(":");
                    if (idxval.length < 2) {
                        System.err.println(docvecdir + " " + qid + ": illegal format: " + cols[i]);
                        continue;
                    }
                    float element = Float.parseFloat(idxval[1]);
                    vecElements.add(element);
                }
                if (!qidCwidQdv.get(qid).containsKey(cwid)) {
                    qidCwidQdv.get(qid).put(cwid, new ArrayList<QueryDocVectors>());
                }
                qidCwidQdv.get(qid).get(cwid).add(new QueryDocVectors(methodParam, Constants.VECTORIZE_METHOD, cwid, qid, vecElements.toArray()));
                vecElements.clear();
            }
            br.close();
        }
        logger.info("Readin " + docvecdir);
    }

    private TIntObjectMap<List<QueryDocTerms>> readDocSelectedTerms(String inputSuffix, String methodParam) throws FileNotFoundException, IOException {
        String inputfile = localDoctermDir + Constants.SEP + inputSuffix + Constants.SEP + "part-r-00000";
        if (!new File(inputfile).exists()) {
            logger.warn(inputfile + " does not exist.");
            inputfile = localDoctermDir + Constants.SEP + inputSuffix + Constants.SEP + "docTerms/part-r-00000";
        }
        if (!new File(inputfile).exists()) {
            logger.error(inputfile + " does not exist.");
            return null;
        }
        // read in from local directory
        List<QueryDocTerms> qdps = new ArrayList<>();
        List<String> docterms = new ArrayList<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile))));
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split("\t");
            int qid = Integer.parseInt(cols[0]);
            String[] cwidTerms = cols[1].split(" ");
            String cwid = cwidTerms[0];
            if (cwidTerms.length < 2) {
                System.err.println(qid + " " + cwidTerms[0] + ": no terms selected " + (cwidTerms.length - 1));
            }
            for (int i = 1; i < cwidTerms.length; i++) {
                // terms could be in term or term:weight format
                // will be parsed in querydocumentpair object
                docterms.add(cwidTerms[i]);
            }
            qdps.add(new QueryDocTerms(methodParam, Constants.VECTORIZE_METHOD, cwid, qid, docterms));
            docterms.clear();
        }
        br.close();
        logger.info("Input query document pairs with proximity terms: " + qdps.size());
        return categorizeTasks(qdps);
    }

    private TIntObjectMap<List<QueryDocTerms>> categorizeTasks(List<QueryDocTerms> qdps) {
        // read in the selected termWeight and categorise them according to query id
        TIntObjectMap<List<QueryDocTerms>> qidQdps = new TIntObjectHashMap<>();
        for (QueryDocTerms qdp : qdps) {
            int qid = qdp.qid;
            if (!qidQdps.containsKey(qid)) {
                qidQdps.put(qid, new ArrayList<QueryDocTerms>());
            }
            qidQdps.get(qid).add(qdp);
        }
        return qidQdps;
    }

    private void outputDocvecUnit(CompletionService<Map<String, float[]>> service, int taskCount) throws FileNotFoundException, IOException, InterruptedException, ExecutionException {
        PrintStream ps = null;
        // fetch the finished list of doc vector, output to different files for different queries
        while (taskCount > 0) {
            Future<Map<String, float[]>> futures = service.poll(100, TimeUnit.MILLISECONDS);
            if (futures != null) {
                // key is delimited by dot
                Map<String, float[]> dockeyDocvector = futures.get();
                try {
                    int qid = -1;
                    if (dockeyDocvector.isEmpty()) {
                        logger.error("dockeyDocvector is empty: " + taskCount);
                        taskCount--;
                        continue;
                    }
                    QueryDocVectors qdv;
                    for (String dockey : dockeyDocvector.keySet()) {
                        qdv = new QueryDocVectors(dockey, Constants.VECTORIZE_METHOD, dockeyDocvector.get(dockey));
                        // the tasks are categerized by query id
                        if (qid < 0) {
                            qid = qdv.qid;
                            String outdir = localDocvecDir + Constants.SEP + qdv.getOutdirSuffix() + Constants.SEP;
                            if (!new File(outdir).exists()) {
                                new File(outdir).mkdirs();
                            }
                            ps = new PrintStream(outdir + qid);
                        }
                        ps.println(qdv.toString());
                    }
                    ps.close();
                    taskCount--;
                } catch (Exception ex) {
                    logger.error("", ex);
                }
            }
        }
    }

    /**
     *
     * @param args
     * @return
     * @throws org.apache.commons.cli.ParseException
     */
    @Override
    public int run(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("l", "log4jconf", true, "log4jconf");
        options.addOption("m", "methodname", true, "name of method");
        options.addOption("h", "help", false, "print this message");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String methodname = cmd.getOptionValue("m");
        // parse the methods to be computed
        try {
            String[] methods;
            if (methodname.contains(",")) {
                methods = methodname.split(",");
            } else {
                methods = new String[]{methodname};
            }
            // setup the logger 
            String log4jconf = cmd.getOptionValue("l");
            org.apache.log4j.PropertyConfigurator.configure(log4jconf);
            LogManager.getRootLogger().setLevel(Level.INFO);
            // for each combination method, run convert2
            FileSystem fs = FileSystem.get(getConf());
            ExecutorService executor = Executors.newFixedThreadPool(24);
            for (String method : methods) {
                concatenateVectors(method, fs, executor);
            }
        } catch (IOException | InterruptedException | ExecutionException ex) {
            logger.error("", ex);
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ComputeWordV2DocV(), args);
        System.exit(exitCode);
    }

}
