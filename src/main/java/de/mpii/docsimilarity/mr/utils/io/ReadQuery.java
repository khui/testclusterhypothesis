package de.mpii.docsimilarity.mr.utils.io;

import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import de.mpii.docsimilarity.mr.utils.StopWordsFilter;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Read query in full format: trec wt xml format with multiple fields
 *
 * @author khui
 */
public class ReadQuery {

    private static final Logger logger = Logger.getLogger(ReadQuery.class);

    /**
     *
     * @param queryfile path in hdfs
     * @return qid-list of query terms
     */
    public static TIntObjectMap<List<String>> readQueries(String queryfile) {
        TIntObjectMap<List<String>> qidQueryTerms = new TIntObjectHashMap<>();
        try {
            DocumentBuilderFactory factory
                    = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            org.w3c.dom.Document doc = builder.parse(queryfile);
            NodeList nList = doc.getElementsByTagName("topic");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                Element eElement = (Element) nNode;
                int queryId = Integer.parseInt(eElement.getAttribute("number"));
                String query = eElement.getElementsByTagName("query").item(0).getTextContent();
                qidQueryTerms.put(queryId, CleanContentTxt.queryTxt(query));
            }
            logger.info("Finished read in query files: " + qidQueryTerms.size());
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return qidQueryTerms;
    }

    public static List<String> readQuery(int qid, String queryfile) {
        List<String> queryTerms = null;
        try {
            DocumentBuilderFactory factory
                    = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            org.w3c.dom.Document doc = builder.parse(queryfile);
            NodeList nList = doc.getElementsByTagName("topic");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                Element eElement = (Element) nNode;
                int queryId = Integer.parseInt(eElement.getAttribute("number"));
                if (queryId != qid) {
                    continue;
                }
                String query = eElement.getElementsByTagName("query").item(0).getTextContent();
                queryTerms = CleanContentTxt.queryTxt(query);
            }
            logger.info("Finished read in query for qid: " + qid);
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return queryTerms;
    }

    public static TIntObjectMap<TIntList> readQueries(String queryfile, TObjectIntMap<String> termTermid, boolean toStem) {
        TIntObjectMap<TIntList> qidQuerytids = new TIntObjectHashMap<>();
        TIntObjectMap<List<String>> qidQueryTerms = readQueries(queryfile);
        for (int qid : qidQueryTerms.keys()) {
            qidQuerytids.put(qid, new TIntArrayList());
            for (String term : qidQueryTerms.get(qid)) {
                if (StopWordsFilter.isStopWordOrNumber(term)) {
                    continue;
                }
                if (toStem) {
                    term = CleanContentTxt.porterStemming(term);
                }
                if (termTermid.containsKey(term)) {
                    int qtermid = termTermid.get(term);
                    qidQuerytids.get(qid).add(qtermid);
                } else {
                    logger.error("Query term " + term + " from " + qid + " is not included in supplied termTermid map");
                }
            }
        }
        logger.info("Finished read in query file:" + queryfile);
        return qidQuerytids;
    }

    public static TIntList readQuery(String queryfile, int qid, TObjectIntMap<String> termTermid, boolean toStem) {
        TIntList queryTermids = new TIntArrayList();
        List<String> queryTerms = readQuery(qid, queryfile);
        for (String term : queryTerms) {
            if (StopWordsFilter.isStopWordOrNumber(term)) {
                continue;
            }
            if (toStem) {
                term = CleanContentTxt.porterStemming(term);
            }
            if (termTermid.containsKey(term)) {
                int qtermid = termTermid.get(term);
                queryTermids.add(qtermid);
            } else {
                logger.error("Query term " + term + " from " + qid + " is not included in supplied termTermid map");
            }
        }
        logger.info("Finished read in query file: " + queryfile + " for query " + qid);
        return queryTermids;
    }

    public static TIntObjectMap<TIntList> readQueryTermids(Path queryfile, TObjectIntMap<String> termTermid, boolean toStem) {
        return ReadQuery.readQueries(queryfile.toString(), termTermid, toStem);
    }

}
