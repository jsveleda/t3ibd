/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.sourceop;

import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.table.Table;
import ibd.table.record.Record;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * scans all records stored in a table
 * @author Sergio
 */
public class FullTableScan extends SourceOperation {

    /**
     * the table reached from this operation
     */
    public Table table;

    /**
     *
     * @param tableAlias the alias of the table reached by this operation
     * @param table the table reached from this operation
     */
    public FullTableScan(String tableAlias, Table table) {
        super(tableAlias);
        this.table = table;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {

    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "[" + dataSourceAlias + "] Table Scan";
    }

    /**
     * {@inheritDoc }
     * @return an iterator that performs a full table scan
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new FullTableScanIterator(lookup);
    }

    /**
     * the class that produces resulting tuples from a full table scan
     */
    public class FullTableScanIterator extends OperationIterator {

        //the iterator over the child operation
        Iterator<Record> iterator;
        //the lookup required from the parent operation
        LookupFilter lookup;

        /**
         *
         * @param lookup
         */
        public FullTableScanIterator(LookupFilter lookup) {
            this.lookup = lookup;
            try {
                //this itrator provides access to all stored records
                iterator = table.iterator();
            } catch (Exception ex) {
                Logger.getLogger(FullTableScan.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        /**
         * finds all records stored in the table
         * @return
         */
        @Override
        protected Tuple findNextTuple() {

            while (iterator.hasNext()) {
                    Record record = iterator.next();
                    Tuple tuple = new Tuple();
                    //the resulting tuple contains a single source tuple that stores the records taken from the table
                    tuple.setSingleSourceTuple(dataSourcesAliases[0], record);
                    //a tuple must satisfy the lookup filter that comes from the parent operation
                    if (lookup.match(tuple)) {
                        return tuple;
                    } 
            }

            return null;
        }

    }
}
