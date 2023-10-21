/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.ContentLookupFilter;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.NoLookupFilter;
import ibd.table.ComparisonTypes;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

/**
 * This unary operation creates a materialized hash containing all the tuples that come from the child operation.
 * Useful for lookups based on equivalence over a primary key, and the child operation is not a source operation. 
 * @author Sergio
 */
public class ContentHashIndex extends UnaryOperation {

    /*
    materialized hash of objects, using the a prefix of the content as the key.
    It can answer content equivalence filters efficiently.
    This collection is shared among all MaterializedIndexIterator objects, becuase we need all calls to lookup to use the same collection.
     */
    Hashtable<String, List<Tuple>> tuples = new Hashtable();
    
     /*
    * the prefix of the content to be indexed
    */
    int prefix = 0;

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @param prefix the prefix of the content to be indexed
     * @throws Exception
     */
    public ContentHashIndex(Operation childOperation, String dataSourceAlias, int prefix) throws Exception {
        super(childOperation, dataSourceAlias);
        this.prefix = prefix;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        super.open();
        //a call to open erases the materialized hash.
        //the previosulsy built hash is detroyed when this method is called. 
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
     * Custom iterator class for iterating over all tuples in a Hashtable of lists of tuples 
     */
    private class InnerIterator implements Iterator<Tuple>{

        
        Iterator<List<Tuple>> outerIterator; // Iterator for the lists of tuples
        Iterator<Tuple> innerIterator; // Iterator for the tuples in the current list
        
        // Constructor: Initialize the iterators
        public InnerIterator(Iterator<List<Tuple>> outerIterator){
            this.outerIterator = outerIterator; 
            innerIterator = getNextTupleIterator(); 
            
        }
        
        // Helper method to get the next non-empty tuple iterator
        private Iterator<Tuple> getNextTupleIterator() {
        while (outerIterator.hasNext()) {
            List<Tuple> nextList = outerIterator.next();
            if (nextList != null && !nextList.isEmpty()) {
                return nextList.iterator();
            }
        }
        return null;
    }
        
        // Check if there is another tuple to iterate over
        @Override
        public boolean hasNext() {
            
            return innerIterator != null && innerIterator.hasNext();
        }

        // Get the next tuple and update the tuple iterator when necessary
        @Override
        public Tuple next() {
            if (innerIterator == null) {
            return null; // / Iterator exhausted, return null or throw an exception
        }
        Tuple nextTuple = innerIterator.next();
        if (!innerIterator.hasNext()) {
            innerIterator = getNextTupleIterator(); // Move to the next list of tuples
        }
        return nextTuple;
        }
    }
    
    
    /**
     * the class that materializes a collection of tuples that come from the child operation using a hash. 
     * The lookup is answered using the hash. 
     */
    private class MaterializedIndexIterator extends OperationIterator {

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
                    //access all tuples that come from the child operation
                    it = childOperation.run();
                    while (it.hasNext()) {
                        Tuple tuple = (Tuple) it.next();
                        String content = tuple.sourceTuples[sourceTupleIndex].record.getContent();
                        String prefixContent = content.substring(0, prefix);
                        List<Tuple> list = tuples.get(prefixContent);
                        if (list==null){
                            list = new ArrayList();
                            tuples.put(prefixContent, list);
                        }
                        list.add(tuple);
                    }

                } catch (Exception ex) {
                }
            }

            
            if (lookup instanceof ContentLookupFilter) {

                ContentLookupFilter pklf = (ContentLookupFilter) lookup;
                if (pklf.getComparisonType() == ComparisonTypes.EQUAL) {
                    //find results based on the lookup key
                    List<Tuple> result = tuples.get(pklf.getContent().substring(0, prefix));
                    //the iterator now accesses only the tuples that satisfy the content filter
                    it = result.iterator();
                    return;
                }
            }

            //if the filter is not equivalence over the content field, we need to access all tuples from the hash
            it = new InnerIterator(tuples.values().iterator());

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
                //if the filter is equivalence over the pk, all found tuples will match, at least for the prefix lenght of the content
                if (lookup.match(tp)) {
                    return tp;
                }

            }
            return null;
        }

    }

}
