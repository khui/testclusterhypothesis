package de.mpii.docsimilarity.mr.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 *
 * @author khui
 */
public class QidCwidsRelSimilarity implements WritableComparable {

    private static final Logger logger = Logger.getLogger(QidCwidsRelSimilarity.class);

    private Text cwidleft;

    private Text cwidright;

    private IntWritable qid;
    // both relevant, one relevant one irrelevant, both irrelevant
    private IntWritable labeltype;

    private DoubleWritable similarity;

    public QidCwidsRelSimilarity() {
        setCwids(new Text(), new Text());
        setQid(new IntWritable());
        setLabelType(new IntWritable());
        setSimilarity(new DoubleWritable());
    }

    public QidCwidsRelSimilarity(String cwidleft, String cwidright, int qid, int labeltype) {
        setCwids(new Text(cwidleft), new Text(cwidright));
        setQid(new IntWritable(qid));
        setLabelType(new IntWritable(labeltype));
        setSimilarity(new DoubleWritable(-1));
    }

    public QidCwidsRelSimilarity(QidCwidsRelSimilarity qrs) {
        setCwids(new Text(qrs.getCwidLeft()), new Text(qrs.getCwidRight()));
        setQid(new IntWritable(qrs.getQid()));
        setLabelType(new IntWritable(qrs.getLabelType()));
        setSimilarity(new DoubleWritable(qrs.getSimilarity()));
    }

    public final void setCwids(Text cwidleft, Text cwidright) {
        this.cwidleft = cwidleft;
        this.cwidright = cwidright;
    }

    public final void setQid(IntWritable qid) {
        this.qid = qid;
    }

    public final void setLabelType(IntWritable type) {
        this.labeltype = type;
    }

    public final void setSimilarity(DoubleWritable similarity) {
        this.similarity = similarity;
    }

    public void setSimilarity(double similarity) {
        setSimilarity(new DoubleWritable(similarity));
    }

    public String getCwidLeft() {
        return this.cwidleft.toString();
    }

    public String getCwidRight() {
        return this.cwidright.toString();
    }

    public int getQid() {
        return this.qid.get();
    }

    public int getLabelType() {
        return this.labeltype.get();
    }

    public double getSimilarity() {
        return this.similarity.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.qid.write(out);
        this.cwidleft.write(out);
        this.cwidright.write(out);
        this.labeltype.write(out);
        this.similarity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.qid.readFields(in);
        this.cwidleft.readFields(in);
        this.cwidright.readFields(in);
        this.labeltype.readFields(in);
        this.similarity.readFields(in);
    }

    @Override
    public boolean equals(Object obj) {
        try {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final QidCwidsRelSimilarity other = (QidCwidsRelSimilarity) obj;
            long[] thisdidminmax = cwid2dids(this.cwidleft, this.cwidright);
            long[] otherdidminmax = cwid2dids(other.cwidleft, other.cwidright);

            if (thisdidminmax[0] != otherdidminmax[0]) {
                return false;
            }
            if (thisdidminmax[1] != otherdidminmax[1]) {
                return false;
            }
            return this.qid.get() == other.qid.get();
        } catch (Exception ex) {
            logger.error("", ex);
            return false;
        }
    }

    @Override
    public int compareTo(Object o) {
        try {
            final QidCwidsRelSimilarity other = (QidCwidsRelSimilarity) o;
            long[] thisdidminmax = cwid2dids(this.cwidleft, this.cwidright);
            long[] otherdidminmax = cwid2dids(other.cwidleft, other.cwidright);
            long thissum = thisdidminmax[0] + thisdidminmax[1];
            long thisdiff = thisdidminmax[1] - thisdidminmax[0];
            long othersum = otherdidminmax[1] + otherdidminmax[0];
            long otherdiff = otherdidminmax[1] - otherdidminmax[0];
            if (this.qid.get() > other.qid.get()) {
                return 1;
            } else if (this.qid.get() < other.qid.get()) {
                return -1;
            } else if (thissum > othersum) {
                return 1;
            } else if (thissum < othersum) {
                return -1;
            } else if (thisdiff > otherdiff) {
                return 1;
            } else if (thisdiff < otherdiff) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception ex) {
            logger.error("", ex);
            return 1;
        }
    }

    @Override
    public String toString() {
        DecimalFormat decimalformat = new DecimalFormat("#.00000");
        StringBuilder sb = new StringBuilder();
        sb.append(this.qid.get()).append(" ");
        sb.append(this.cwidleft.toString()).append(" ");
        sb.append(this.cwidright.toString()).append(" ");
        sb.append(this.labeltype.get()).append(" ");
        sb.append(decimalformat.format(this.similarity.get()));
        return sb.toString();
    }

    private long[] cwid2dids(Text cwidleft, Text cwidright) {
        try {
            long did_left = EncodeCwid.cwid2did(cwidleft.toString());
            long did_right = EncodeCwid.cwid2did(cwidright.toString());
            long didmin = Math.min(did_left, did_right);
            long didmax = Math.max(did_left, did_right);
            return new long[]{didmin, didmax};
        } catch (Exception ex) {
            logger.error("", ex);
            return new long[]{0, 1};
        }
    }

    @Override
    public int hashCode() {
        return this.cwidleft.hashCode() * 163 + this.cwidright.hashCode() + this.qid.hashCode() * 31;
    }

}
