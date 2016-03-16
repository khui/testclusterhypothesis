package de.mpii.docsimilarity.mr.utils.io;

import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * cwid" "{term:tfidfweight}
 *
 * @author khui
 */
public class ReadSparseVector {
    
    private static final Logger logger = Logger.getLogger(ReadSparseVector.class);

    /**
     * cwid-{term:tfidf}
     *
     * @param conf
     * @param path
     * @return
     * @throws IOException
     */
    public static Map<String, TObjectDoubleMap<String>> termTfidfMap(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        Map<String, TObjectDoubleMap<String>> cwidTermTfidf = new HashMap<>();
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            if (cols != null) {
                if (cols.length > 1) {
                    String cwid = cols[0];
                    if (!cwid.startsWith("clueweb")) {
                        logger.error("wrong format of cwid: " + cwid);
                        continue;
                    }
                    cwidTermTfidf.put(cwid, new TObjectDoubleHashMap<String>());
                    int i = 1;
                    while (i < cols.length) {
                        String termTfidf = cols[i++];
                        String[] pair = termTfidf.split(":");
                        if (pair != null) {
                            if (pair.length == 2) {
                                String term = pair[0];
                                if (term.length() > 2) {
                                    double tfidf = Double.parseDouble(pair[1]);
                                    cwidTermTfidf.get(cwid).put(term, tfidf);
                                }
                            }
                        }
                    }
                } else {
                    logger.error(line + " has insufficient cols");
                }
            }
        }
        br.close();
        inputStream.close();
        logger.info("succefully read in " + path.getName());
        return cwidTermTfidf;
    }

    /**
     * cwid-{tid:tfidf}
     *
     * @param conf
     * @param path
     * @return
     * @throws IOException
     */
    public static Map<String, TIntDoubleMap> tidTfidfMap(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        Map<String, TIntDoubleMap> cwidTermTfidf = new HashMap<>();
        while (br.ready()) {
            String line = br.readLine();
            Map<String, TIntDoubleMap> singleline = tidTfidfMapLine(line);
            if (singleline == null) {
                continue;
            }
            cwidTermTfidf.putAll(singleline);
        }
        br.close();
        inputStream.close();
        logger.info("succefully read in " + path.getName());
        return cwidTermTfidf;
    }
    
    public static Map<String, TIntDoubleMap> tidTfidfMapLine(String line) {
        Map<String, TIntDoubleMap> cwidTermTfidf = new HashMap<>();
        String[] cols = line.split(" ");
        if (cols != null) {
            if (cols.length > 1) {
                String cwid = cols[0];
                if (!cwid.startsWith("clueweb")) {
                    logger.error("wrong format of cwid: " + cwid);
                    return null;
                }
                cwidTermTfidf.put(cwid, new TIntDoubleHashMap());
                int i = 1;
                while (i < cols.length) {
                    String termTfidf = cols[i++];
                    String[] pair = termTfidf.split(":");
                    if (pair != null) {
                        if (pair.length == 2) {
                            int tid = Integer.parseInt(pair[0]);
                            if (tid < 0) {
                                logger.error("error termid: " + termTfidf + " in line " + line);
                            }
                            double tfidf = Double.parseDouble(pair[1]);
                            cwidTermTfidf.get(cwid).put(tid, tfidf);
                            
                        }
                    }
                }
            } else {
                logger.error(line + " has insufficient cols");
            }
        }
        return cwidTermTfidf;
    }
    
}
