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
 * This operation sorts tuples by the content of a determined data source.
 * @author Sergio
 */
public class ContentSort extends Sort {


    public ContentSort(Operation op, String sourceName) throws Exception {
        super(op, sourceName);
    }

    @Override
    public Comparator<Tuple> createComparator() {
        return new ContentTupleComparator();
    }

    /**
     * a comparator class that defines how tuples are sorted.
     * The tuples are compared by the content of a determined data source (identified by sourceTupleIndex).
     */
    public class ContentTupleComparator implements Comparator<Tuple> {

        @Override
        public int compare(Tuple tuple1, Tuple tuple2) {
            String content1 = tuple1.sourceTuples[sourceTupleIndex].record.getContent();
            String content2 = tuple2.sourceTuples[sourceTupleIndex].record.getContent();
            return content1.compareTo(content2);
        }
    }

    @Override
    public String toString() {
        return "Content Sort";
    }


}
