package de.mpii.evaltool;

import java.util.*;

/**
 * Created by khui on 04/01/16.
 */
public class EvalMap implements EvalMetrics{

    private AdhocQrels qrels;

    private Map<Integer, String[]> qidDocRank = new HashMap<>();

    private Map<Integer, Double> qidAP = new HashMap<>();

    public EvalMap(String[] qrelLines){
        this(qrelLines, "notgiven");
    }

    public EvalMap(String[] qrelLines, String qrelid){
        this.qrels = new AdhocQrels(qrelLines, qrelid);
    }

    @Override
    public Map.Entry<String, Double> getAvgScore(String[] runLines, boolean condensedList) {
        String runname = readRun(runLines, condensedList);
        double map = 0;
        for(int qid : qidDocRank.keySet()) {
            if (!qrels.queryExistInQrels(qid)){
                continue;
            }
            double ap = averagePrecition(qid);
            if (ap >= 0) {
                map += ap;
            }
        }
        map /= qrels.getQueryNum();
        return new AbstractMap.SimpleEntry(runname, map);
    }

    @Override
    public Map.Entry<String, Double> getScore(String[] runLines, int qid, boolean condensedList) {
        String runname = readRun(runLines, condensedList);
        if (!qrels.queryExistInQrels(qid)){
            System.out.println("Query not exist in qrel: " + qid);
            new AbstractMap.SimpleEntry(runname, -1);
        }
        double ap = averagePrecition(qid);
        return new AbstractMap.SimpleEntry(runname, ap);
    }

    public String getMAPPerQuery(String[] runLines,  Boolean condensedList) {
        StringBuilder sb = new StringBuilder();
        String runname = readRun(runLines, condensedList);
        double map = 0;
        for(int qid : qidDocRank.keySet()) {
            double ap = averagePrecition(qid);
            if (ap >= 0) {
                map += ap;
            }
            sb.append(runname + " " + qid + " " + ap).append("\n");
        }
        map /= qidAP.size();
        sb.append(runname + " map " + map).append("\n");
        return sb.toString();
    }

    private String readRun(String[] lines, Boolean condensedList){
        qidDocRank.clear();
        qidAP.clear();
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

    private double averagePrecition(int qid){
        String[] cwidrank = qidDocRank.get(qid);
        if (cwidrank.length == 0) {
            qidAP.put(qid,0.0);
            return -1;
        }
        double ap = 0;
        int relnum = 0;
        for (int i = 0; i < cwidrank.length; i++) {
            int rank = i + 1;
            if (qrels.isRelevant(qid, cwidrank[i])){
                relnum ++;
                ap += relnum / (double) rank;
            }
        }
        if (relnum == 0){
            qidAP.put(qid,0.0);
            return 0.0;
        }
        ap /= qrels.getNumberOfRelevant(qid);
        qidAP.put(qid, ap);
        return ap;
    }


}
