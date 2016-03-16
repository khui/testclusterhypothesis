/*
 * Terrier - Terabyte Retriever 
 * Webpage: http://terrier.org 
 * Contact: terrier{a.}dcs.gla.ac.uk
 * University of Glasgow - School of Computing Science
 * http://www.gla.ac.uk/
 * 
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is TRECQrelsInMemory.java.
 *
 * The Original Code is Copyright (C) 2004-2015 the University of Glasgow.
 * All Rights Reserved.
 *
 * Contributor(s):
 * Ben He <ben{a.}dcs.gla.ac.uk> 
 * Vassilis Plachouras <vassilis{a.}dcs.gla.ac.uk>
 */
package de.mpii.evaltool;

import org.apache.commons.lang.ArrayUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class AdhocQrels implements java.io.Serializable {

    private Map<Integer, HashMap<String, Double>> qidDocnoLabel = new HashMap<>();

    public Map<Integer, Integer> qidRelNum = new HashMap<>();

    private String qrelid;

    public AdhocQrels(String[] qrelLines, String qrelid){
        loadAdhocQrelsFile(qrelLines);
        this.qrelid = qrelid;
    }

    public AdhocQrels(String[] qrelLines){
        this(qrelLines, "notgiven");
    }

    public boolean queryExistInQrels(int qidint) {
        return qidDocnoLabel.containsKey(qidint);
    }

    public int getQueryNum(){
        return qidDocnoLabel.size();
    }

    public String getQrelid(){
        return this.qrelid;
    }


    public boolean docExistInQrels(int qidint, String docid) {
        if (!queryExistInQrels(qidint)) {
            return false;
        }
        return qidDocnoLabel.get(qidint).containsKey(docid);
    }

    public int getNumberOfRelevant(int qidint){
        return qidRelNum.get(qidint);
    }

    public boolean isRelevant(int qidint, String docid){
        boolean isrel = false;
        if (qidDocnoLabel.get(qidint).containsKey(docid)){
            if (qidDocnoLabel.get(qidint).get(docid) > 0) {
                isrel = true;
            }
        }
        return isrel;
    }

    public double getRellevel(int qidint, String docid){
        if (qidDocnoLabel.get(qidint).containsKey(docid)){
            return qidDocnoLabel.get(qidint).get(docid);
        } else {
            return 0;
        }
    }

    public Double[] getRelLevel(int queryid){
        Double[] objLevels = qidDocnoLabel.get(queryid).values().toArray(new Double[qidDocnoLabel.get(queryid).size()]);
        return objLevels;
    }

    public int[] getQids(){
        Integer[] objqids = qidDocnoLabel.keySet().toArray(new Integer[qidDocnoLabel.size()]);
        return ArrayUtils.toPrimitive(objqids);
    }

    protected void loadDiversityQrelsFile(String[] qrelLines) {
        try {
            Map<Integer, Map<String, Double>> qidAll = new HashMap<>();
            for (String line : qrelLines) {
                String[] cols = line.split(" ");
                if (cols.length == 4) {
                    int queryid = Integer.parseInt(cols[0]);
                    if (!qidAll.containsKey(queryid)) {
                        qidAll.put(queryid, new HashMap<>());
                    }
                    String docno = cols[2];
                    double relGrade = Double.parseDouble(cols[3]);
                    qidAll.get(queryid).put(docno, relGrade);
                }
            }
            for (Integer queryid : qidAll.keySet()) {
                if (!qidDocnoLabel.containsKey(queryid)) {
                    qidDocnoLabel.put(queryid, new HashMap<>());
                }
                if (!qidRelNum.containsKey(queryid)) {
                    qidRelNum.put(queryid, 0);
                }
                for (String docno : qidAll.get(queryid).keySet()) {
                    double relgrade = qidAll.get(queryid).get(docno);
                    if (relgrade > 0) {
                        qidRelNum.put(queryid, qidRelNum.get(queryid) + 1);
                    }
                    qidDocnoLabel.get(queryid).put(docno, relgrade);

                }
            }
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }


    protected void loadAdhocQrelsFile(String[] qrelLines) {
        try {
            for(String line : qrelLines) {
                String[] cols = line.split(" ");
                if(cols.length == 4) {
                    int queryid = Integer.parseInt(cols[0]);
                    String docno = cols[2];
                    double relGrade = Double.parseDouble(cols[3]);
                    if (relGrade > 0) {
                        if (!qidRelNum.containsKey(queryid)){
                            qidRelNum.put(queryid, 0);
                        }
                        qidRelNum.put(queryid, qidRelNum.get(queryid) + 1);
                    }
                    if (!qidDocnoLabel.containsKey(queryid)) {
                        qidDocnoLabel.put(queryid, new HashMap<>());
                    }
                    qidDocnoLabel.get(queryid).put(docno, relGrade);
                }
            }
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }


    }


}