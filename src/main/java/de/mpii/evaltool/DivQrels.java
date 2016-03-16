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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class DivQrels implements java.io.Serializable {

    private Map<Integer, HashMap<String, Set<Integer>>> qidDidRelSubtopics = new HashMap<>();

    public Map<Integer, Set<String>> qidJudgedDoc = new HashMap<>();

    // subtopic that actually appears in the qrel
    public Map<Integer, Set<Integer>> qidActualSubtopicNum = new HashMap<>();

    private String qrelid;

    public DivQrels(String[] qrelLines, String qrelid){
        loadDiversityQrelsFile(qrelLines);
        this.qrelid = qrelid;
    }

    public DivQrels(String[] qrelLines){
        this(qrelLines, "notgiven");
    }

    public boolean queryExistInQrels(int queryid) {
        return qidDidRelSubtopics.containsKey(queryid);
    }

    public int getQueryNum(){
        return qidJudgedDoc.size();
    }

    public String getQrelid(){
        return this.qrelid;
    }


    public Set<Integer> getActualSubtopics(int qid){
        return qidActualSubtopicNum.get(qid);
    }

    public boolean docExistInQrels(int queryid, String docid) {
        if (!queryExistInQrels(queryid)) {
            return false;
        }
        return qidJudgedDoc.get(queryid).contains(docid);
    }

    public boolean isRelevant(int queryid, String docid){
        boolean isrel = false;
        if (qidDidRelSubtopics.get(queryid).containsKey(docid)){
            isrel = true;
        }
        return isrel;
    }

    public boolean isRelevant(int queryid, String docid, int subtopic){
        boolean isrel = false;
        if (qidDidRelSubtopics.get(queryid).containsKey(docid)){
            if (qidDidRelSubtopics.get(queryid).get(docid).contains(subtopic)){
                isrel = true;
            }
        }
        return isrel;
    }

    public int getActualSubtopicNum(int qid){
        return qidActualSubtopicNum.get(qid).size();

    }

    protected void loadDiversityQrelsFile(String[] qrelLines) {
        try {

            for (String line : qrelLines) {
                String[] cols = line.split(" ");
                if (cols.length == 4) {
                    int queryid = Integer.parseInt(cols[0]);
                    if (!qidJudgedDoc.containsKey(queryid)) {
                        qidDidRelSubtopics.put(queryid, new HashMap<>());
                        qidJudgedDoc.put(queryid, new HashSet<>());
                        qidActualSubtopicNum.put(queryid, new HashSet<>());
                    }
                    String docno = cols[2];
                    int subtopic = Integer.parseInt(cols[1]);
                    int relGrade = Integer.parseInt(cols[3]);
                    if (relGrade > 0) {
                        qidActualSubtopicNum.get(queryid).add(subtopic);
                        if (!qidDidRelSubtopics.get(queryid).containsKey(docno)){
                            qidDidRelSubtopics.get(queryid).put(docno, new HashSet<>());
                        }
                        qidDidRelSubtopics.get(queryid).get(docno).add(subtopic);
                    }
                    qidJudgedDoc.get(queryid).add(docno);
                }
            }
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

}