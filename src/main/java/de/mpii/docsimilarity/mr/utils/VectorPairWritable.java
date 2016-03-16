package de.mpii.docsimilarity.mr.utils;

import de.mpii.docsimilarity.mr.input.docpair.SparseDocVector;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TObjectDoubleMap;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * writable wrapper for pair of doc-vector to compare
 *
 * @author khui
 */
public class VectorPairWritable implements Writable {

    private static final Logger logger = Logger.getLogger(VectorPairWritable.class);

    private Vector doc1;

    private Vector doc2;

    public VectorPairWritable() {
    }

    public VectorPairWritable(TObjectDoubleMap<String> doc1, TObjectDoubleMap<String> doc2) {
        this.set(doc1, doc2);
    }

    public VectorPairWritable(TIntDoubleMap doc1, TIntDoubleMap doc2, long cardinality) {
        this.set(doc1, doc2, cardinality);
    }

    public void set(Vector doc1, Vector doc2) {
        this.doc1 = doc1;
        this.doc2 = doc2;
    }

    public double cosineSimilarity() {
        double similarity = -1;
        try {
            CosineDistanceMeasure cs = new CosineDistanceMeasure();
            similarity = 1 - cs.distance(doc1, doc2);
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return similarity;
    }

    public final void set(TObjectDoubleMap<String> doc1, TObjectDoubleMap<String> doc2) {
        SparseDocVector.Convert2Vector converter = new SparseDocVector.Convert2Vector(doc1, doc2);
        set(converter.convert2vector(doc1), converter.convert2vector(doc2));
    }

    public final void set(TIntDoubleMap doc1, TIntDoubleMap doc2, long cardinality) {
        set(SparseDocVector.convert2vector(doc1, cardinality), SparseDocVector.convert2vector(doc2, cardinality));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new VectorWritable(doc1).write(out);
        new VectorWritable(doc2).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        VectorWritable vw1 = new VectorWritable();
        VectorWritable vw2 = new VectorWritable();
        vw1.readFields(in);
        vw2.readFields(in);
        doc1 = vw1.get();
        doc2 = vw2.get();
    }

}
