package de.mpii.evaltool;

import java.util.*;

/**
 * Created by khui on 30/01/16.
 * Implementation of the Kendall's Tau AP described in
 * (Yilmaz, 2008)
 * A New Rank Correlation Coefficient for Information Retrieval
 *
 * The formula is as follows.
 *
 * tau_ap = \frac{2}{N-1} \sum_{C(r) / (r - 1)} - 1
 * where C(r) is the number of items above rank r and correctly
 * ranked with respect to the item at rank r in the ground truth rank.
 */
public class KendalltauAP {

    private final Map<String, Set<String>> runidAboverunids;

    // 2/(N-1)
    private final double normfactor;

    public KendalltauAP(final Map<String, Double> groundTruthRidVal){
        List<String> sortedRunids = sortRunid(groundTruthRidVal);
        runidAboverunids = new HashMap<>();
        runidAboverunids.put(sortedRunids.get(0), new HashSet<>());
        for (int idx = 1; idx < sortedRunids.size(); idx ++){
            String currentid = sortedRunids.get(idx);
            runidAboverunids.put(currentid, new HashSet<>());
            for (int above = 0; above < idx; above ++){
                runidAboverunids.get(currentid).add(sortedRunids.get(above));
            }
        }
        int N = groundTruthRidVal.size();
        normfactor = 2d / (N - 1);
    }

    public double correlation(final Map<String, Double> runidVal){
        List<String> sortedRunids = sortRunid(runidVal);
        double kendalltauap = 0;
        for (int idx = 1; idx < sortedRunids.size(); idx ++){
            String currentRunid = sortedRunids.get(idx);
            // 1/(r - 1)
            double weight = 1d / idx;
            // C(r)
            int concordantNum = 0;
            for (int above = 0; above < idx; above ++){
                String aboveRunid = sortedRunids.get(above);
                if (runidAboverunids.get(currentRunid).contains(aboveRunid)){
                    concordantNum++;
                }
            }
            if (concordantNum > 0) {
                // \sum_{C(r) / (r - 1)}
                kendalltauap += concordantNum * weight;
            }
        }
        kendalltauap = kendalltauap * normfactor - 1;
        return kendalltauap;
    }

    private List<String> sortRunid(final Map<String, Double> runidVal){
        List<String> runidlist = new ArrayList<>();
        runidVal
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(v -> -v.getValue()))
                .forEachOrdered(v -> runidlist.add(v.getKey()));
        return runidlist;
    }
}
