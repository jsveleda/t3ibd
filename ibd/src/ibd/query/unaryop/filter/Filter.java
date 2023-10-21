/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop.filter;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.unaryop.UnaryOperation;
import java.util.Iterator;

/**
 * A filter operation filters tuples that come from its child operation.
 * This operation actually asks the child operation to resolve the filter condition.
 * The source tuple used as the filter condition comes from the data source defined by the specify alias.
 * @author Sergio
 */
public abstract class Filter extends UnaryOperation {

    int comparisonType;

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @param comparisonType the comparison type that needs to be satisfied
     * @throws Exception
     */
    public Filter(Operation childOperation, String dataSourceAlias, int comparisonType) throws Exception {
        super(childOperation, dataSourceAlias);
        this.comparisonType = comparisonType;
    }

    /**
     *
     * @return the comparidon type
     */
    public int getComparisonType() {
        return comparisonType;
    }

    /**
     *
     * @param lookup
     * @return an iterator that performs a filter over the child operation
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new FilterScanIterator(lookup);
    }

    /**
     * Defines the filter condition to be applied over a source tuple.
     * Subclasses must implement this function to define a concrete filter condition 
     * @param tupleIndex the identification of the source tuple
     * @return the filter condition.
     */
    public abstract LookupFilter createLookupFilter(int tupleIndex);
    
    /**
     * the class that produces resulting tuples from a filter over the child operation
     */
    public class FilterScanIterator extends OperationIterator {

        //the iterator over the child operation
        Iterator<Tuple> tuples;
        //the lookup required from the parent operation
        LookupFilter lookup;

        /**
         *
         * @param lookup
         */
        public FilterScanIterator(LookupFilter lookup) {
            this.lookup = lookup;
            tuples = childOperation.lookUp(createLookupFilter(sourceTupleIndex));//pushes filter down to the child operation
        }

        /**
         * {@inheritDoc }
         */
        @Override
        protected Tuple findNextTuple() {
            while (tuples.hasNext()) {
                Tuple tp = tuples.next();
                //a tuple must satisfy the lookup filter that comes from the parent operation
                if (lookup.match(tp)) 
                return tp;

            }
            return null;
        }

    }
}
