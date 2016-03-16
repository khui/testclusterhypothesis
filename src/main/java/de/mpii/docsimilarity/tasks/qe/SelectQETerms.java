package de.mpii.docsimilarity.tasks.qe;

import de.mpii.docsimilarity.tasks.doc2vec.SelectTerms;
import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.FileNotFoundException;
import java.util.Map;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;

/**
 *
 * @author khui
 */
public abstract class SelectQETerms extends SelectTerms {

    private static final Logger logger = Logger.getLogger(SelectQETerms.class);

    protected final int docnumInCol;
    protected final long termnumInCol;

    // read in string line and generate query term-term weight might
    // used in communication between mapper and reducer
    public static TObjectDoubleMap<String> parseString(String line) {
        TObjectDoubleMap<String> termweightmap = new TObjectDoubleHashMap<>();
        String[] termweights = line.split(" ");
        for (String termweight : termweights) {
            String[] tw = termweight.split(":");
            if (tw.length == 2) {
                String term = tw[0];
                double weight = Double.parseDouble(tw[1]);
                termweightmap.put(term, weight);
            } else {
                logger.error("Wrong format for " + termweight + " in line: " + line);
            }
        }
        return termweightmap;
    }

    public static TObjectDoubleMap<String> parseStringNorm(String line) {
        TObjectDoubleMap<String> termweightmap = parseString(line);
        minmaxNorm(termweightmap);
        return termweightmap;
    }

    public SelectQETerms(String termdfpath, String queryPath, int qid, int docnumInCol, long termnumInCol, CleanContentTxt inclean) throws FileNotFoundException {
        super(termdfpath, queryPath, qid, inclean, false);
        this.docnumInCol = docnumInCol;
        this.termnumInCol = termnumInCol;
    }

    public abstract TIntDoubleMap getQueryRelatedTerms(int qid, Map<String, Document> webpages, int numOfTerms2Select);

    /**
     * to generate string for passing to reducer correspond to the static
     * method: parseString
     *
     * @param termweight
     * @param num2select
     * @return
     */
    public String toString(TIntDoubleMap termweight, int num2select) {
        Integer[] termids = sortDescendingly(termweight);
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Integer termid : termids) {
            sb.append(termidTerm.get(termid)).append(":").append(dfFormater.format(termweight.get(termid))).append(" ");
            count++;
            if (count >= num2select) {
                break;
            }
        }
        return sb.toString();
    }

}
