package de.mpii.evaltool;

import org.apache.commons.lang3.ArrayUtils;

import java.io.*;
import java.util.*;

/**
 * Created by khui on 04/01/16.
 */
public class EvalNdcg implements EvalMetrics{

    private AdhocQrels qrels;

    private Map<Integer, String[]> qidDocRank = new HashMap<>();

    private Map<Integer, Double> qidNdcg = new HashMap<>();

    private Map<Integer, Double> qidIdealDcg = new HashMap<>();

    private Map<Double, Double> level2Grade = new HashMap<>();

    private final int topK;

    private final double LOGBASEDIV = Math.log(2);

    public EvalNdcg(String[] qrelLines){
        this(qrelLines, 30, "notgiven");
    }

    public EvalNdcg(String[] qrelLines, int topk, String qrelid){
        this.qrels = new AdhocQrels(qrelLines, qrelid);
        this.topK = topk;
        for (double r = -2; r < 5; r++){
            if (r <= 0){
                level2Grade.put(r, 0.0);
            } else {
                level2Grade.put(r, r);
            }
        }
        for(int qid : qrels.getQids()) {
            Double[] rellevels = qrels.getRelLevel(qid);
            Double[] gradedgain = new Double[rellevels.length];
            for (int i =0;i < gradedgain.length;i++){
                gradedgain[i] = rellevel2gradegain(rellevels[i]);
            }
            Arrays.sort(gradedgain, Collections.reverseOrder());
            double idealdcg = computeDCG(ArrayUtils.toPrimitive(gradedgain), gradedgain.length);
            qidIdealDcg.put(qid, idealdcg);
        }

    }

    @Override
    public Map.Entry<String, Double> getAvgScore(String[] runLines, boolean condensedList) {
        String runname = readRun(runLines, condensedList);
        double mNdcg = 0;
        for(int qid : qidDocRank.keySet()) {
            if (!qrels.queryExistInQrels(qid)){
                continue;
            }
            double ndcg = computeNdcg(qid);
            if (ndcg >= 0) {
                mNdcg += ndcg;
            }
        }
        mNdcg /= qrels.getQueryNum();
        return new AbstractMap.SimpleEntry(runname, mNdcg);
    }

    @Override
    public Map.Entry<String, Double> getScore(String[] runLines, int qid, boolean condensedList) {
        String runname = readRun(runLines, condensedList);
        if (!qrels.queryExistInQrels(qid)){
            System.out.println("Query not exist in qrel: " + qid);
            new AbstractMap.SimpleEntry(runname, -1);
        }
        double ndcg = computeNdcg(qid);
        return new AbstractMap.SimpleEntry(runname, ndcg);
    }

    private String readRun(String[] lines, Boolean condensedList){
        qidDocRank.clear();
        qidNdcg.clear();
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

    private double rellevel2gradegain(double rellevel){
        double gain;
        if (level2Grade.containsKey(rellevel)){
            gain = level2Grade.get(rellevel);
        } else {
            gain = rellevel;
        }
        return gain;
    }

    private double computeNdcg(int qid){
        String[] cwidrank = qidDocRank.get(qid);
        if (cwidrank.length == 0) {
            qidNdcg.put(qid,0.0);
            return -1;
        }
        double[] gains = new double[cwidrank.length];
        for (int i = 0; i < cwidrank.length; i++) {
            double rellevel = qrels.getRellevel(qid, cwidrank[i]);
            gains[i] = rellevel2gradegain(rellevel);
        }
        double dcg = computeDCG(gains, gains.length);
        if (dcg == 0){
            return 0;
        }
        double idealdcg = qidIdealDcg.get(qid);
        return dcg / idealdcg;
    }

    /**
     * dcg = \frac{gain}{log(i + 2)}
     */
    private double computeDCG(double[] gains, int k) {
        double score = 0;
        for (int i = 0; i < Math.min(k, gains.length); i++) {
            if (gains[i] == 0){
                continue;
            }
            score += gains[i] / (Math.log(i + 2) / LOGBASEDIV);
        }
        return score;
    }

    public static void main(String[] args) throws IOException {
        String qrelfile = "/home/khui/workspace/result/data/qrel/qrels.adhoc.wt11";
        String runfile = "/home/khui/workspace/result/data/qrel/11/input.2011SiftR1.qcleaned";
        int qid = 101;
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(qrelfile))));
        List<String> lines = new ArrayList<>();
        while(br.ready()){
            lines.add(br.readLine());
        }
        br.close();
        String[] qrellines = lines.toArray(new String[lines.size()]);
        br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(runfile))));
        lines.clear();
        while(br.ready()){
            lines.add(br.readLine());
        }
        br.close();
        String[] runlines = lines.toArray(new String[lines.size()]);
        EvalNdcg eval = new EvalNdcg(qrellines);
        for (int q = 101; q < 151; q++) {
            Map.Entry<String, Double> runnameScore = eval.getScore(runlines, q, false);
            System.out.println(runnameScore.getKey() + " " + q + " " + runnameScore.getValue());
        }
        Map.Entry<String, Double> avgScore = eval.getAvgScore(runlines, false);
        System.out.println(avgScore.getKey() + " avg" + " " + avgScore.getValue());
    }

}
