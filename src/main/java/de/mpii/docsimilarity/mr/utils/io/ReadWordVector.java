package de.mpii.docsimilarity.mr.utils.io;

import de.mpii.docsimilarity.mr.utils.GlobalConstants;
import gnu.trove.list.TFloatList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 *
 * @author khui
 */
public class ReadWordVector {
    
    private static final Logger logger = Logger.getLogger(ReadWordVector.class);
    
    public static void readTermVector(String termvecfile, TObjectIntMap<String> termTermid, TIntObjectMap<float[]> termidVector) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(termvecfile))));
        TFloatList vector = new TFloatArrayList();
        String term;
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            term = cols[0];
            if (!termTermid.containsKey(term)) {
                logger.error("Term is not included in the termTermid map: " + term);
                continue;
            }
            int tid = termTermid.get(term);
            vector.clear();
            for (int i = 1; i < cols.length; i++) {
                String[] idxVal = cols[i].split(":");
                if (idxVal.length != 2) {
                    logger.error("Illegal format of the idxval: " + cols[i] + " for " + term);
                }
                float val = Float.parseFloat(idxVal[1]);
                vector.add(val);
            }
            termidVector.put(tid, vector.toArray());
        }
        br.close();
    }
    
    public static void readTermVector(String termvecfile, TObjectIntMap<String> termTermid, TIntObjectMap<NamedVector> termidVector, int cardinality) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(termvecfile))));
        Vector docVector;
        if (cardinality > Integer.MAX_VALUE) {
            logger.error("Input cardinality is illegal: " + cardinality);
            cardinality = Integer.MAX_VALUE;
        }
        String term;
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            term = cols[0];
            if (!termTermid.containsKey(term)) {
                logger.error(term + " is not included in the given term-termid map.");
                continue;
            }
            int tid = termTermid.get(term);
            docVector = convert2VecSingleline(cols, cardinality);
            termidVector.put(tid, new NamedVector(docVector.clone(), term));
        }
        br.close();
    }
    
    public static TObjectIntMap<String> readTermVector(String termvecfile, TIntObjectMap<NamedVector> linenumVector, int cardinality) throws FileNotFoundException, IOException {
        TObjectIntMap<String> termLinenum = new TObjectIntHashMap<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(termvecfile))));
        Vector docVector;
        if (cardinality > Integer.MAX_VALUE) {
            cardinality = Integer.MAX_VALUE;
        }
        String term;
        int linenum = 1;
        while (br.ready()) {
            String line = br.readLine();
            String[] cols = line.split(" ");
            term = cols[0];
            docVector = convert2VecSingleline(cols, cardinality);
            linenumVector.put(linenum, new NamedVector(docVector.clone(), term));
            termLinenum.put(term, linenum);
            linenum++;
        }
        br.close();
        return termLinenum;
    }
    
    public static Vector convert2VecSingleline(String[] cols, int cardinality) {
        Vector docVector;
        String term = cols[0];
        if (cardinality <= GlobalConstants.CARDINALITY_THRES_DENSEVEC) {
            docVector = new DenseVector((int) cardinality);
        } else {
            docVector = new RandomAccessSparseVector((int) cardinality);
        }
        for (int i = 1; i < cols.length; i++) {
            String[] idxVal = cols[i].split(":");
            if (idxVal.length != 2) {
                logger.error("Illegal format of the idxval: " + cols[i] + " for " + term);
            }
            int idx = Integer.parseInt(idxVal[0]) - 1;
            float val = Float.parseFloat(idxVal[1]);
            docVector.set(idx, val);
        }
        return docVector;
    }
    
}
