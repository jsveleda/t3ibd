/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop.sort;

import ibd.query.Operation;
import ibd.query.Tuple;
import java.util.Comparator;

/**
 * This operation sorts tuples by the pks of a determined data source.
 * @author Sergio
 */
public class PKSort extends Sort {

    public PKSort(Operation op, String sourceName) throws Exception {
        super(op, sourceName);
    }

    @Override
    public Comparator<Tuple> createComparator() {
        return new TupleComparator();
    }

     /**
     * A comparator class that defines how tuples are sorted.
     * The tuples are compared by the pk of a determined data source (identified by sourceTupleIndex).
     */
    public class TupleComparator implements Comparator<Tuple> {

        @Override
        public int compare(Tuple tt1, Tuple tt2) {
            return Long.compare(tt1.sourceTuples[sourceTupleIndex].record.getPrimaryKey(), tt2.sourceTuples[sourceTupleIndex].record.getPrimaryKey());
        }
    }

    @Override
    public String toString() {
        return "PK Sort";
    }

}
