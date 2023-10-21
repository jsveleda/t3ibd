/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop.sort;

import ibd.query.Operation;
import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.unaryop.UnaryOperation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A sort operation sorts tuples that come from its child operation.
 * The source tuple used as the sort condition comes from the data source defined by the specify alias.
 * This operation is materialized.
 * @author Sergio
 */
public abstract class Sort extends UnaryOperation {

    /*
    * materialized shared collection of objects.
    * all queries over this operation share the same collection.
    */
    
    ArrayList<Tuple> tuples;

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @throws Exception
     */
    public Sort(Operation childOperation, String dataSourceAlias) throws Exception {
        super(childOperation, dataSourceAlias);
    }

    /**
     * {@inheritDoc }
     * @return an iterator that performs a sort over the tuples from the child operation
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new SortIterator(lookup);
    }

    /**
     *
     * @return the comparator class used to sort tuples.
     * Subclasses must implement this function to define a concrete comparator
     */
    public abstract Comparator<Tuple> createComparator();
    
    /**
     * the class that produces resulting tuples from the sorting over the tuples that come from the child operation.
     * The incoming tuples are materialized in memory, and then sorted.
     */
    private class SortIterator extends OperationIterator {

        //the iterator over the child operation
        Iterator<Tuple> it;
        //the lookup required from the parent operation
        LookupFilter lookup;

        public SortIterator(LookupFilter lookup) {

            this.lookup = lookup;
            //build materialized collection, if one does not exist yet.
            if (tuples == null) {
                tuples = new ArrayList<>();
                try {
                    //accesses and stores all tuples that come from the child operation
                    it = childOperation.run(); 
                    while (it.hasNext()) {
                        Tuple tuple = (Tuple) it.next();
                        tuples.add(tuple);

                    }

                    //sort collection
                    Collections.sort(tuples, createComparator());
                } catch (Exception ex) {
                    Logger.getLogger(PKSort.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            //build iterator
            it = tuples.iterator();
        }

        
        @Override
        protected Tuple findNextTuple(){
        //iterates over all tuples from the materialized collection.
            while (it.hasNext()) {
                Tuple tp = it.next();
                //a tuple must satisfy the lookup filter that comes from the parent operation
                if (lookup.match(tp)) 
                    return tp;
            }
            return null;
        }

    }

}
