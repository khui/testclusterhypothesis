package de.mpii.evaltool;

import java.util.*;

/**
 * Created by khui on 10/01/16.
 */
public class EvalErrIA implements EvalMetrics {

    private DivQrels qrels;

    private final int topkcut;

    private final double alpha = 0.5;

    private Map<Integer, String[]> qidDocRank = new HashMap<>();

    public EvalErrIA(String[] qrelLines, String qrelid) {
        this(qrelLines, 20, qrelid);
    }

    public EvalErrIA(String[] qrelLines, int topk, String qrelid) {
        this.qrels = new DivQrels(qrelLines,  qrelid);
        this.topkcut = topk;
    }


    @Override
    public Map.Entry<String, Double> getAvgScore(String[] runLines, boolean condensedList) {
        String runname = readRun(runLines, condensedList);
        double avgErr = 0;
        for(int qid : qidDocRank.keySet()) {
            if (!qrels.queryExistInQrels(qid)) {
                continue;
            }
            if (qrels.getActualSubtopicNum(qid) == 0){
                continue;
            }
            double[] errAll = errIAPerQuery(qid, qidDocRank.get(qid), runname);
            avgErr += errAll[topkcut - 1];

        }
        avgErr /= qidDocRank.size();
        return new AbstractMap.SimpleEntry(runname, avgErr);
    }

    @Override
    public Map.Entry<String, Double> getScore(String[] runLines, int qid, boolean condensedList) {
        String runname = readRun(runLines, condensedList);
        if (!qrels.queryExistInQrels(qid)) {
            System.out.println("Query not exist in qrel: " + qid);
            new AbstractMap.SimpleEntry(runname, -1);
        }
        if (qrels.getActualSubtopicNum(qid) == 0){
            System.out.println("Query has zero subtopics: " + qid);
            new AbstractMap.SimpleEntry(runname, -1);
        }
        double[] errAll = errIAPerQuery(qid, qidDocRank.get(qid), runname);
        return new AbstractMap.SimpleEntry(runname, errAll[topkcut - 1]);
    }

    private double[] errIAPerQuery(int qid, String[] rList, String runname){
        Map<Integer, Double> subtopicGain = new HashMap<>();
        double[] idealIdeal = new double[this.topkcut];
        double[] err = new double[this.topkcut];
        double idealIdealGain = qrels.getActualSubtopicNum(qid);
        Set<Integer> actualSubtopics = qrels.getActualSubtopics(qid);
        for (int s : actualSubtopics){
            subtopicGain.put(s, 1.0);
        }

        for(int r = 0; r < Math.min(rList.length, this.topkcut); r++) {
            String cwid = rList[r];
            double score = 0;
            if (!qrels.docExistInQrels(qid, cwid)){
                continue;
            }
            if (!qrels.isRelevant(qid, cwid)){
                continue;
            }
            for (int s : actualSubtopics){
                if(qrels.isRelevant(qid, cwid, s)){
                    score += subtopicGain.get(s);
                    subtopicGain.put(s, subtopicGain.get(s) * (1 - alpha));
                }
            }
            err[r] = score / (double) (r + 1);
        }

        for(int r = 0; r < this.topkcut; r++) {
            idealIdeal[r] = idealIdealGain / (r + 1);
            idealIdealGain *= (1.0 - alpha);
        }


        for (int r = 1; r < this.topkcut; r++) {
            err[r] += err[r-1];
            idealIdeal[r] += idealIdeal[r-1];
        }

        for (int r = 1; r < this.topkcut; r++) {
            err[r] /= idealIdeal[r];
        }
        return err;
    }



    // The condense list option here is tricky, be cautious to use
    // it for diversification evaluation.
    private String readRun(String[] lines, Boolean condensedList){
        qidDocRank.clear();
        String runname = null;
        int qid = -1;
        List<String> cwids = new ArrayList<>();
        for(String line : lines) {
            String[] cols = line.split(" ");
            int cqid = Integer.parseInt(cols[0]);
            String cwid = cols[2];
            if (qid == -1){
                runname = cols[5];
            }
            if (cqid != qid){
                if (qid > 0) {
                    qidDocRank.put(qid, cwids.toArray(new String[cwids.size()]));
                }
                cwids.clear();
                qid = cqid;
            }
            if (condensedList) {
                if (!qrels.docExistInQrels(qid, cwid)){
                    continue;
                }
            }
            cwids.add(cwid);
        }
        if(cwids.size() > 0){
            qidDocRank.put(qid, cwids.toArray(new String[cwids.size()]));
        }
        return runname;
    }
}
