package de.mpii.evaltool;

import java.util.Map;

/**
 * Created by khui on 10/01/16.
 */
public interface EvalMetrics extends java.io.Serializable {
    Map.Entry<String, Double> getAvgScore(String[] runLines, boolean condenseList);

    Map.Entry<String, Double> getScore(String[] runLines,int queryid, boolean condenseList);
}
