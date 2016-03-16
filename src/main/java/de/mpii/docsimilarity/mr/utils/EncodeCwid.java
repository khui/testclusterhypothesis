package de.mpii.docsimilarity.mr.utils;

import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class EncodeCwid {
    
    private static final Logger logger = Logger.getLogger(EncodeCwid.class);
    
    public static long cwid2did(String cwid) {
        if (cwid.startsWith("clueweb09")) {
            return cwid092did(cwid);
        } else if (cwid.startsWith("clueweb12")) {
            return cwid122did(cwid);
        } else {
            logger.error("failed to encode the cwid: " + cwid);
            return -1;
        }
    }
    
    public static long cwid092did(String cwid) {
        //clueweb09-<directory>-<file>-<record>
        //"clueweb09-en0000-11-11182";
        //"clueweb09-enwp00-00-05201";
        String[] parts = cwid.split("-");
        String dir = parts[1].substring(2);
        String digits;
        long iswp = 0; // 0 or 1, 1 digit
        // en<4 digits> or enwp<2 digits>
        if (dir.startsWith("wp")) {
            digits = dir.substring(2);
            iswp = 1;
        } else {
            digits = dir;
        }
        long digitsOfdir = Integer.parseInt(digits);// 0000 -> 0133, 8 digits
        long file = Integer.parseInt(parts[2]);//00 ->99, 7 digits
        long record = Integer.parseInt(parts[3]);//00000->50000, 16 digits
        long did = (iswp << 31) | (digitsOfdir << 23) | (file << 16) | (record);
        return did;
    }
    
    public static long cwid122did(String cwid) {
        //clueweb12-<segment #><directory #><type>-<file #>-<record #>
        //"clueweb12-0001wb-04-27064"
        String[] parts = cwid.split("-");
        String dir = parts[1];
        long type = 0; // 1, 2 or 3, 2 digit
        // wb tw wt
        if (dir.endsWith("wb")) {
            type = 1;
        } else if (dir.endsWith("tw")) {
            type = 2;
        } else if (dir.endsWith("wt")) {
            type = 3;
        }
        long segment = Long.parseLong(dir.substring(0, 2));//00 ->99, 7 digits 
        long directory = Long.parseLong(dir.substring(2, 4));//00 ->99, 7 digits
        long file = Long.parseLong(parts[2]);//00 ->99, 7 digits
        long record = Long.parseLong(parts[3]);//00000->99999, 17 digits

        long did = (segment << 33) | (directory << 26) | (type << 24) | (file << 17) | (record);
        return did;
    }
    
}
