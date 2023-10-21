/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.NoLookupFilter;
import ibd.query.unaryop.sort.ContentSort;
import java.util.Iterator;

/**
 *
 * @author Sergio
 */
public class DuplicateRemoval extends UnaryOperation {

    boolean isOrdered;

    public DuplicateRemoval(Operation op, String sourceName, boolean isOrdered) throws Exception {
        super(op, sourceName);
        this.isOrdered = isOrdered;
    }

    @Override
    public void open() throws Exception {
        if (!isOrdered) {
            ContentSort cs = new ContentSort(childOperation, dataSourceAlias);
            childOperation = cs;
        }
        super.open();

    }

    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new DuplicateRemovalIterator(lookup);
    }

    private class DuplicateRemovalIterator extends OperationIterator {

        //the iterator over the child operation
        Iterator<Tuple> tuples;
        //the lookup required from the parent operation
        LookupFilter lookup;
        String prevContent = null;

        public DuplicateRemovalIterator(LookupFilter lookup) {
            this.lookup = lookup;
            tuples = childOperation.run();//push filter down 
        }

        @Override
        protected Tuple findNextTuple() {
            while (tuples.hasNext()) {
                Tuple tp = tuples.next();
                //a tuple must satisfy the lookup filter 
                if (!lookup.match(tp)) {
                    continue;
                }
                if (prevContent == null || !(tp.sourceTuples[sourceTupleIndex].record.getContent().equals(prevContent))) {
                    prevContent = tp.sourceTuples[sourceTupleIndex].record.getContent();
                    return tp;
                }

            }
            return null;
        }

    }
}
