package de.mpii.docsimilarity.mr.utils;

/**
 *
 * @author khui
 */
public class GlobalConstants {

    // collection length
    public final static long COLLECTIONLEN_CW09 = 344904496888L;
    // collection length
    public final static long COLLECTIONLEN_CW12 = 521902712343L;
    // doc number
    public final static int DOCFREQUENCY_CW09 = 5180307;

    public final static int DOCFREQUENCY_CW12 = 7496964;
    // total term numbers
    public final static int CARDINALITY_CW09 = 9394917;
    // total term numbers
    public final static int CARDINALITY_CW12 = 12760369;

    public final static int CARDINALITY_THRES_DENSEVEC = 300;

    // the variable name for cwids to be filtered
    public final static String CWIDS_FILE_NAME_PREFIX = "cwids_file_name";

    public final static int TOPN_TERM2REMOVE_HIGHDF = 0;

    public final static String CARDINALITY_SPARSEVEC = "cardinality4vec";

    public final static int MAX_TOKEN_LENGTH = 64;

    public final static int MAX_DOC_LENGTH = 512 * 1024;
    
    public final static int DF_MIN2FILTER = 50;

}
