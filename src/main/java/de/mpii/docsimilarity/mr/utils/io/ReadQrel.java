package de.mpii.docsimilarity.mr.utils.io;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class ReadQrel {

    private static final Logger logger = Logger.getLogger(ReadQrel.class);

    public static Map<String, TIntList> readInDiversityQrel(Path path, int requiredQid) throws FileNotFoundException {
        Map<String, TIntList> cwidRellabels = new HashMap<>();
        Scanner in = new Scanner(new BufferedReader(new FileReader(path.toString())));
        try {
            while (in.hasNext()) {
                String line = in.nextLine();
                String[] cols = line.split(" ");
                if (cols.length == 4) {
                    int qid = Integer.parseInt(cols[0]);
                    if (qid != requiredQid) {
                        continue;
                    }
                    String cwid = cols[2];
                    int subtopic = Integer.parseInt(cols[1]);
                    int label = Integer.parseInt(cols[3]);
                    if (!cwidRellabels.containsKey(cwid)) {
                        cwidRellabels.put(cwid, new TIntArrayList());
                    }
                    if (label > 0) {
                        cwidRellabels.get(cwid).add(subtopic);
                    }

                } else {
                    logger.error("incorrect number of columns for qrel file: " + line);
                }
            }
            logger.info("read in " + cwidRellabels.size() + " from " + path.getName());
        } catch (Exception ex) {
            logger.error("", ex);
        }
        in.close();
        return cwidRellabels;
    }

    /**
     *
     * @param path
     * @param requiredQid
     * @return cwid-labels, if being judged relevant, the labels would contain
     * one 1, otherwise empty
     * @throws FileNotFoundException
     */
    public static Map<String, TIntList> readInAdhocQrel(Path path, int requiredQid) throws FileNotFoundException {
        Map<String, TIntList> cwidRellabels = new HashMap<>();
        Scanner in = new Scanner(new BufferedReader(new FileReader(path.toString())));
        try {
            while (in.hasNext()) {
                String line = in.nextLine();
                String[] cols = line.split(" ");
                if (cols.length == 4) {
                    int qid = Integer.parseInt(cols[0]);
                    if (qid != requiredQid) {
                        continue;
                    }
                    String cwid = cols[2];
                    int label = Integer.parseInt(cols[3]);
                    if (!cwidRellabels.containsKey(cwid)) {
                        cwidRellabels.put(cwid, new TIntArrayList());
                    }
                    if (label > 0) {
                        cwidRellabels.get(cwid).add(1);
                    }

                } else {
                    logger.error("incorrect number of columns for qrel file: " + line);
                }
            }
            logger.info("read in " + cwidRellabels.size() + " from " + path.getName());
        } catch (Exception ex) {
            logger.error("", ex);
        }
        in.close();
        return cwidRellabels;
    }

}
