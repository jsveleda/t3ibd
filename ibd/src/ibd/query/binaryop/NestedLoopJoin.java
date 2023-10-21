/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.PKLookupFilter;
import ibd.table.ComparisonTypes;
import java.util.Iterator;

/**
 * Performs a nested loop join between the left and the right operations.
 * This operation performs an equi-join between a left pk and a right pk.
 * The pks used as the join condition comes from the source tuples defined by the specified aliases.
 * @author Sergio
 */
public class NestedLoopJoin extends BinaryOperation {

    /**
     * 
     * @param leftOperation the left side operation
     * @param rightOperation the right side operation
     * @throws Exception
     */
    public NestedLoopJoin(Operation leftOperation, Operation rightOperation) throws Exception {
        super(leftOperation, rightOperation);
    }

    /**
     *
     * @param leftOperation the left side operation
     * @param leftDataSourceAlias the alias of the data source from the left side used to
     * perform the join condition of this nested lopp join
     * @param rightOperation the right side operation
     * @param rigthtDataSourceAlias the alias of the data source from the right side used to
     * perform oin condition of this nested lopp join
     * @throws Exception
     */
    public NestedLoopJoin(Operation leftOperation, String leftDataSourceAlias, Operation rightOperation, String rigthtDataSourceAlias) throws Exception {
        super(leftOperation, leftDataSourceAlias, rightOperation, rigthtDataSourceAlias);
    }

    /**
     *
     * @return the name of the operation
     */
    @Override
    public String toString() {
        return "Nested Loop Join";
    }

    /**
     * {@inheritDoc }
     * @return an iterator that performs a simple nested loop join over the tuples from the left and right sides
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new NestedLoopJoinIterator(lookup);
    }

    /**
     * the class that produces resulting tuples from the join between the two underlying operations.
     */
    private class NestedLoopJoinIterator extends OperationIterator {

        //keeps the current state of the left side of the join: The current tuple read
        Tuple leftTuple;
        //the iterator over the operation on the left side
        Iterator<Tuple> leftTuples;
        //the iterator over the operation on the left side
        Iterator<Tuple> rightTuples;
        //the lookup required from the parent operation
        LookupFilter lookup;

        /**
         *
         * @param lookup the condition from the parent operation that needs to be satisfied
         */
        public NestedLoopJoinIterator(LookupFilter lookup) {
            leftTuple = null;
            this.lookup = lookup;
            leftTuples = leftOperation.run(); //iterate over all tuples from the left side
        }

        /**
         * 
         * @return the next satisfying tuple, if any
         */
        @Override
        protected Tuple findNextTuple() {

            //the left side cursor only advances if the current left side tuple is done.
            //it means all corresponding tuples from the right side were processed
            while (leftTuple != null || leftTuples.hasNext()) {
                if (leftTuple == null) {
                    leftTuple = leftTuples.next();
                    //lookup the target tuples from the right side
                    rightTuples = rightOperation.lookUp(new PKLookupFilter(leftTuple.sourceTuples[leftSourceTupleIndex].record.getPrimaryKey(), ComparisonTypes.EQUAL, rightSourceTupleIndex));
                }

                //iterate through the right side tuples that satisfy the lookup
                while (rightTuples.hasNext()) {
                    Tuple curTuple2 = (Tuple) rightTuples.next();
                    //create returning tuple and add the joined tuples
                    Tuple tuple = new Tuple();
                    tuple.setSourceTuples(leftTuple, curTuple2);
                    //a tuple must satisfy the lookup filter that comes from the parent operation
                    if (lookup.match(tuple)) {
                        return tuple;
                    }

                }
                //All corresponding tuples from the right side processed. 
                //set null to allow left side cursor to advance
                leftTuple = null;
            }
            
            //no more tuples to be joined
            return null;
        }
    }

}
