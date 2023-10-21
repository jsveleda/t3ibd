/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.sourceop;

import ibd.query.OperationIterator;
import ibd.query.Tuple;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.PKLookupFilter;
import ibd.table.ComparisonTypes;
import ibd.table.Table;
import ibd.table.record.Record;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Performs an indx scan over a table
 * @author Sergio
 */
public class IndexScan extends SourceOperation {

    /**
     * the table reached from this operation
     */
    public Table table;

    /**
     *
     * @param tableAlias the alias of the table reached by this operation
     * @param table the table reached from this operation
     */
    public IndexScan(String tableAlias, Table table) {
        super(tableAlias);
        this.table = table;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "[" + dataSourceAlias + "] Index Scan";
    }

    /**
     * {@inheritDoc }
     * @param lookup
     * @return an iterator that performs an index scan over a table
     */
    @Override
    public Iterator<Tuple> lookUp(LookupFilter lookup) {
        return new IndexScanIterator(lookup);
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
    }

    /**
     * the class that produces resulting tuples from an index scan
     */
    public class IndexScanIterator extends OperationIterator {

        //the iterator over the child operation
        Iterator<Record> iterator;
        //the lookup required from the parent operation
        LookupFilter lookup;

        /**
         *
         * @param lookup
         */
        public IndexScanIterator(LookupFilter lookup) {
            this.lookup = lookup;
            try {
                //if filter is equivalence over the pk, we can use the getRecord() function to locate the record, if any
                if (lookup instanceof PKLookupFilter) {
                    PKLookupFilter pklookup = (PKLookupFilter) lookup;
                    if (pklookup.getComparisonType() == ComparisonTypes.EQUAL) {
                        Record record = table.getRecord(pklookup.getPk());
                        List<Record> records = new ArrayList();
                        if (record != null) {
                            records.add(record);
                        }
                        iterator = records.iterator();
                        return;
                    }

                }
                //if filter is not equivalence over the pk, the scan has to traverse the entire table
                iterator = table.iterator();

            } catch (Exception ex) {
                Logger.getLogger(IndexScan.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        /**
         * {@inheritDoc }
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
                //if the filter is equivalence over the pk, all found tuples will match
                if (lookup.match(tuple)) {
                    return tuple;
                } 
            }

            return null;
        }

    }
}
