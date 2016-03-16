package de.mpii.docsimilarity.tasks.doc2vec.bagofword.wvec2dvec;

import gnu.trove.list.TFloatList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.util.StringUtils;

/**
 * Given a selecting methods, a query and a document, a querydocterms instance
 * will be generated
 *
 * @author khui
 */
public class UniqQDRecord {

    private static final Logger logger = Logger.getLogger(UniqQDRecord.class);

    private static class Constants {

        static String SEP = "/";

    }

    // QDrecord from doc terms
    public static class QueryDocTerms extends UniqQDRecord {

        private String tokens;
        private final TObjectDoubleMap<String> termWeights = new TObjectDoubleHashMap<>();

        public QueryDocTerms(String methodName, String vectorizeMethod, String cwid, int qid, List<String> selectedTerms) {
            super(methodName, vectorizeMethod, cwid, qid);
            addSelectedTerms(selectedTerms);
            super.dockey = generateKey();
        }

        // generate querydocterms from dockey-docvectors
        public String getTokenStr() {
            return tokens;
        }

        private void addSelectedTerms(List<String> terms) {
            int i = 0;
            for (String term : terms) {
                double weight;
                if (!term.contains(":")) {
                    weight = terms.size() - i;
                    termWeights.put(term, weight);
                } else {
                    String[] termweight = term.split(":");
                    weight = Double.parseDouble(termweight[1]);
                    if (weight == 0) {
                        if (termweight[1].startsWith("-")) {
                            weight = -(i + 1);
                        }
                    }
                    termWeights.put(termweight[0], weight);
                }
                i++;
            }
            // put into tokens for tokenizer of word2vec
            tokens = sortDescendingly(termWeights);
        }

        private String sortDescendingly(final TObjectDoubleMap<String> termweight) {
            String[] terms = termweight.keys(new String[0]);
            // sort the term according to the weight score
            // in descending order
            Arrays.sort(terms, new Comparator<String>() {
                @Override
                public int compare(String term1, String term2) {
                    double prox1 = termweight.get(term1);
                    double prox2 = termweight.get(term2);
                    if (prox1 > prox2) {
                        return -1;
                    } else if (prox1 < prox2) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });
            return StringUtils.arrayToDelimitedString(terms, " ");
        }

    }

    public static class QueryDocVectors extends UniqQDRecord {

        private final String vectorStr;

        private final float[] vector;

        private final DecimalFormat df = new DecimalFormat("#.000000");

        public QueryDocVectors(String dockey, String vectorizeMethod, float[] vector) {
            super(dockey, vectorizeMethod);
            this.vector = vector;
            this.vectorStr = getVecStr(vector);
        }

        public QueryDocVectors(String methodName, String vectorizeMethod, String cwid, int qid, float[] vector) {
            super(methodName, vectorizeMethod, cwid, qid);
            this.vector = vector;
            this.vectorStr = getVecStr(vector);
        }

        public QueryDocVectors(String vectorizeMethod, String cwid, int qid, List<QueryDocVectors> qdvs) {
            super(vectorizeMethod, cwid, qid, qdvs);
            TFloatList concatVector = new TFloatArrayList();
            for (QueryDocVectors qdv : qdvs) {
                for (float element : qdv.getVector()) {
                    concatVector.add(element);
                }
            }
            this.vector = concatVector.toArray();
            StringBuilder sb = new StringBuilder();
            int idx = 1;
            for (float element : vector) {
                sb.append(idx).append(":").append(df.format(element)).append(" ");
                idx++;
            }
            this.vectorStr = sb.toString();
        }

        private String getVecStr(float[] vector) {
            StringBuilder sb = new StringBuilder();
            int termid = 1;
            for (double d : vector) {
                sb.append(termid).append(":").append(df.format(d)).append(" ");
                termid++;
            }
            return sb.toString();
        }

        public float[] getVector() {
            return this.vector;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(cwid).append(" ").append(this.vectorStr);
            return sb.toString();
        }

    }

    /**
     * correspond to one selectterms method
     */
    public static class MethodUnit {

        private final String windowlen;
        private final String termnum;
        private final String methodParams;
        private final String inputdirSuffix;
        private final String outdirSuffix;

        MethodUnit(String methodstr, String vectorizedMethod) {
            String[] nameparams = methodstr.split("-");
            this.methodParams = methodstr;
            if (nameparams.length == 3) {
                this.windowlen = nameparams[1];
                this.termnum = nameparams[2];
                this.inputdirSuffix = nameparams[0] + Constants.SEP + this.windowlen + "-100";
                this.outdirSuffix = nameparams[0] + Constants.SEP + vectorizedMethod + Constants.SEP + this.windowlen + "-" + termnum;
            } else {
                logger.error("Illegal number of parameters: " + methodstr);
                this.windowlen = null;
                this.termnum = null;
                this.inputdirSuffix = null;
                this.outdirSuffix = null;
            }
        }

        int getTopN() {
            return Integer.parseInt(termnum);
        }

        String getMethodParam() {
            return methodParams;
        }

        String getOutdirSuffix() {
            return outdirSuffix;
        }

        String getIndirSuffix() {
            return inputdirSuffix;
        }

        boolean readInDocterms(FileSystem fs, String hdfsDoctermsDir, String localDoctermDir) throws IOException {
            String hdfsindir = hdfsDoctermsDir + Constants.SEP + inputdirSuffix;
            String localindir = localDoctermDir + Constants.SEP + inputdirSuffix;
            // copy the docterms file from hdfs
            if (new File(localindir).exists()) {
                logger.warn(localindir + " is already existed");
                return false;
            }
            fs.copyToLocalFile(new Path(hdfsindir), new Path(localindir));
            logger.info("Finished copy2Local into: " + localindir);
            return true;
        }

    }

    public final String cwid;
    // method name is name-wlen-topk
    private final String methodParam;
    private String methodName;
    private String outdirSuffix;
    private String dockey;
    public final int qid;

    // generate querydocterms from document terms
    public UniqQDRecord(String methodParam, String vectorizeMethod, String cwid, int qid) {
        this.cwid = cwid;
        this.qid = qid;
        this.methodParam = methodParam;
        methodNameOutsuffix(methodParam, vectorizeMethod);
    }

    public UniqQDRecord(String dockey, String vectorizeMethod) {
        String[] components = dockey.split("\\.");
        this.dockey = dockey;
        this.methodParam = components[0];
        this.cwid = components[2];
        this.qid = Integer.parseInt(components[1]);
        methodNameOutsuffix(methodParam, vectorizeMethod);
    }

    public UniqQDRecord(String vectorizeMethod, String cwid, int qid, List<? extends UniqQDRecord> qdrs) {
        List<String> mParams = new ArrayList<>();
        Collections.sort(qdrs, new Comparator<UniqQDRecord>() {
            @Override
            public int compare(UniqQDRecord o1, UniqQDRecord o2) {
                return o1.methodParam.compareTo(o2.methodParam);
            }
        });
        for (UniqQDRecord qdr : qdrs) {
            mParams.add(qdr.methodParam);
        }
        this.methodParam = combMethodParams(mParams);
        methodNameOutsuffix(methodParam, vectorizeMethod);
        this.qid = qid;
        this.cwid = cwid;
    }

    public String getMethodParam() {
        return this.methodParam;
    }

    public String getMethodName() {
        return this.methodName;
    }

    public String getKey() {
        return dockey;
    }

    public String getOutdirSuffix() {
        return outdirSuffix;
    }

    protected String generateKey() {
        return methodParam + "." + String.valueOf(qid) + "." + cwid;
    }

    private void methodNameOutsuffix(String methodParam, String vectorizeMethod) {
        String[] params = methodParam.split("-");
        if (params.length != 3) {
            logger.error("methodName is in wrong format: " + methodParam);
        }
        this.methodName = params[0];
        outdirSuffix = params[0] + Constants.SEP + vectorizeMethod + Constants.SEP + params[1] + "-" + params[2];
    }

    // methodname1_methodname2-para1_para1-para2_para2
    private String combMethodParams(List<String> submparams) {
        StringBuilder methodname = new StringBuilder();
        StringBuilder param1 = new StringBuilder();
        StringBuilder param2 = new StringBuilder();
        for (int i = 0; i < submparams.size(); i++) {
            if (i != 0) {
                methodname.append("_");
                param1.append("_");
                param2.append("_");
            }
            String[] params = submparams.get(i).split("-");
            if (params.length != 3) {
                logger.error("Illegal format of params: " + submparams.get(i));
                continue;
            }
            methodname.append(params[0]);
            param1.append(params[1]);
            param2.append(params[2]);
        }
        methodname.append("-").append(param1).append("-").append(param2);
        return methodname.toString();
    }

}
