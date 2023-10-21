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
import ibd.query.lookup.PKLookupFilter;
import ibd.table.ComparisonTypes;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

/**
 * This unary operation is materialized. It creates a hash containing all the tuples that come from the child operation.
 * Useful for lookups based on equivalence over a primary key, and the child operation is not a source operation. 
 * @author Sergio
 */
public class PKHashIndex extends UnaryOperation {

    /*
    materialized hash of objects, using the pk as the key.
    It can answer pk equivalence filters efficiently.
    This collection is shared among all MaterializedIndexIterator objects, becuase we need all queries issued over this operation to use the same collection.
     */
    Hashtable<Long, Tuple> tuples = new Hashtable();

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @throws Exception
     */
    public PKHashIndex(Operation childOperation, String dataSourceAlias) throws Exception {
        super(childOperation, dataSourceAlias);
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        super.open();
        //erases the previosulsy built hash.
        //a new one is created when the first query is executed. 
        tuples = null;
    }

    /**
     *
     * @param lookup
     * @return an iterator that uses a hash to materialize tuples from the child operation.
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new MaterializedIndexIterator(lookup);
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "PK Sort";
    }

    /**
     * the class that materializes a collection of tuples that come from the child operation using a hash. 
     * The query is answered using the hash. 
     */
    public class MaterializedIndexIterator extends OperationIterator {

        //the iterator over the child operation
        Iterator<Tuple> it = null;
        //the lookup required from the parent operation
        LookupFilter lookup;

        /**
         *
         * @param lookup
         */
        public MaterializedIndexIterator(LookupFilter lookup) {

            this.lookup = lookup;
            //build hash, if one does not exist yet
            if (tuples == null) {
                tuples = new Hashtable();
                try {
                    //accesses and indexes all tuples that come from the child operation
                    it = childOperation.run();
                    while (it.hasNext()) {
                        Tuple tuple = (Tuple) it.next();
                        tuples.put(tuple.sourceTuples[0].record.getPrimaryKey(), tuple);
                    }

                } catch (Exception ex) {
                }
            }

            //here is where we build an iterator o traverse the query results
            //if the query has a equivalence over the pk filter, the index can be used
            if (lookup instanceof PKLookupFilter) {

                PKLookupFilter pklf = (PKLookupFilter) lookup;
                if (pklf.getComparisonType() == ComparisonTypes.EQUAL) {
                    //use the hash to find results based on the lookup key
                    ArrayList<Tuple> result = new ArrayList<>();
                    Tuple t = tuples.get(pklf.getPk());
                    if (t != null) {
                        result.add(t);
                    }
                    //the iterator now accesses only the tuples that satisfy the pk filter
                    it = result.iterator();
                    return;
                }
            }

            //if the filter is not equivalence over the pk field, we need to access all tuples from the hash
            it = tuples.values().iterator();

        }

        /**
         *
         * @return
         */
        @Override
        protected Tuple findNextTuple() {
            while (it.hasNext()) {
                Tuple tp = it.next();
                //a tuple must satisfy the lookup filter that comes from the parent operation
                //if the filter is equivalence over the pk, all found tuples will match
                if (lookup.match(tp)) {
                    return tp;
                }

            }
            return null;
        }

    }

}
