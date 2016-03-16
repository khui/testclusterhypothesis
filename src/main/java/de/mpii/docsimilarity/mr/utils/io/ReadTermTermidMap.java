package de.mpii.docsimilarity.mr.utils.io;

import de.mpii.docsimilarity.mr.utils.CleanContentTxt;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Based on the term-df output from the complete dataset, we construct the
 * unique termid-term map for each term in favor of further processing
 *
 * @author khui
 */
public class ReadTermTermidMap {

    private static final Logger logger = Logger.getLogger(ReadTermTermidMap.class);

    /**
     * Input the term df file, where in each line, there is a term and the
     * corresponding document frequency. Based on this file, we construct
     * forward, backward map between termid and term. With stemming used, the
     * third map stores the map between term stem and the terms. If the
     * porterstemming is set true, the third map should be initialized
     * beforehand.
     *
     * @param termdfPath
     * @param termTermid
     * @param termidTerm
     * @param termidDf
     * @param termidCf
     * @param termidRawterms
     * @param porterstemming: whether to stem the term
     * @throws FileNotFoundException
     */
    public static void readTermDfCf(String termdfPath, TObjectIntMap<String> termTermid,
            TIntObjectMap<String> termidTerm, TIntIntMap termidDf, TIntLongMap termidCf, TIntObjectMap<Set<String>> termidRawterms, boolean porterstemming) throws FileNotFoundException {
        Scanner in = new Scanner(new BufferedReader(new FileReader(termdfPath)));
        if (termTermid == null) {
            termTermid = new TObjectIntHashMap<>();
        }
        if (termidTerm == null) {
            termidTerm = new TIntObjectHashMap<>();
        }
        // the termid start from 0
        int linenum = 0;
        String term;
        try {
            while (in.hasNext()) {
                String line = in.nextLine();
                String[] cols = line.split("\t");
                if (cols.length >= 2) {
                    if (porterstemming) {
                        term = CleanContentTxt.porterStemming(cols[0]);
                    } else {
                        term = cols[0];
                    }
                    int df = Integer.parseInt(cols[1]);

                    if (!termTermid.containsKey(term)) {
                        termTermid.put(term, linenum);
                        termidTerm.put(linenum, term);
                        if (termidDf != null) {
                            termidDf.put(linenum, df);
                        }
                        if (cols.length > 2 && termidCf != null) {
                            long cf = Long.parseLong(cols[2]);
                            termidCf.put(linenum, cf);
                        }
                        if (porterstemming) {
                            termidRawterms.put(linenum, new HashSet<String>());
                            termidRawterms.get(linenum).add(cols[0]);
                        }
                    } else if (porterstemming) {
                        int termid = termTermid.get(term);
                        termidRawterms.get(termid).add(cols[0]);
                    }
                    linenum++;
                }
            }
            logger.info("Finished read in " + termTermid.size() + " term statistics");
            in.close();
        } catch (Exception ex) {
            logger.error("", ex);
        }

    }

    public static void readTermDfCf(String termdfPath, TObjectIntMap<String> termTermid, TIntObjectMap<String> termidTerm, boolean isStem) throws FileNotFoundException {
        readTermDfCf(termdfPath, termTermid, termidTerm, null, null, null, isStem);
    }

    public static void readTermDf(String termdfPath, TObjectIntMap<String> termTermid,
            TIntObjectMap<String> termidTerm, TIntIntMap termidDf) throws FileNotFoundException {
        readTermDfCf(termdfPath, termTermid, termidTerm, termidDf, null, null, false);
    }

    public static void readTermDf(Path termdfPath, TObjectIntMap<String> termTermid,
            TIntObjectMap<String> termidTerm, TIntIntMap termidDf, TIntObjectMap<Set<String>> termidRawterms, boolean porterstemming) throws FileNotFoundException {
        readTermDfCf(termdfPath.toString(), termTermid, termidTerm, termidDf, null, termidRawterms, porterstemming);
    }

    public static void readTermDf(String termdfPath, TObjectIntMap<String> termTermid,
            TIntObjectMap<String> termidTerm, TIntIntMap termidDf, TIntObjectMap<Set<String>> termidRawterms, boolean porterstemming) throws FileNotFoundException {
        readTermDfCf(termdfPath, termTermid, termidTerm, termidDf, null, termidRawterms, porterstemming);
    }

    public static void readTermDfCf(String termdfPath, TObjectIntMap<String> termTermid,
            TIntObjectMap<String> termidTerm, TIntIntMap termidDf, TIntLongMap termidCf, boolean toStem) throws FileNotFoundException {
        readTermDfCf(termdfPath, termTermid, termidTerm, termidDf, termidCf, null, toStem);
    }

    public static void readTermDfCf(Path termdfPath, TObjectIntMap<String> termTermid,
            TIntObjectMap<String> termidTerm, TIntIntMap termidDf, TIntLongMap termidCf, TIntObjectMap<Set<String>> termidRawterms, boolean porterstemming) throws FileNotFoundException {
        readTermDfCf(termdfPath.toString(), termTermid, termidTerm, termidDf, termidCf, termidRawterms, porterstemming);
    }

}
