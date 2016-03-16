package de.mpii.docsimilarity.mr.input.writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Klaus Bererich on 26/11/15.
 */
public class IntTuple2Writable implements WritableComparable {

    private int fst;

    private int snd;

    public IntTuple2Writable() {
    }

    public IntTuple2Writable(IntTuple2Writable ipw) {
        this.fst = ipw.fst;
        this.snd = ipw.snd;
    }

    public IntTuple2Writable(int fst, int snd) {
        this.fst = fst;
        this.snd = snd;
    }

    public int getFirst() {
        return fst;
    }

    public int getSecond() {
        return snd;
    }

    public void setFirst(int fst) {
        this.fst = fst;
    }

    public void setSecond(int snd) {
        this.snd = snd;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, fst);
        WritableUtils.writeVInt(out, snd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fst = WritableUtils.readVInt(in);
        snd = WritableUtils.readVInt(in);
    }

    @Override
    public int compareTo(Object t) {
        IntTuple2Writable other = (IntTuple2Writable) t;
        if (fst < other.fst) {
            return -1;
        } else if (fst > other.fst) {
            return +1;
        } else if (snd < other.snd) {
            return -1;
        } else if (snd > other.snd) {
            return +1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object input){
        IntTuple2Writable aThat = (IntTuple2Writable)input;
        return (aThat.fst == this.fst) && (aThat.snd == this.snd);
    }


}