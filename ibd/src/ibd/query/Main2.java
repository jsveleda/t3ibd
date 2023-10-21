/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query;

import ibd.query.sourceop.FullTableScan;
import ibd.query.unaryop.PKHashIndex;
import ibd.query.unaryop.filter.PKFilter;
import ibd.query.binaryop.NestedLoopJoin;
import ibd.query.sourceop.IndexScan;
import ibd.query.unaryop.filter.ContentFilter;
import ibd.query.unaryop.sort.ContentSort;
import ibd.table.Params;
import static ibd.table.ComparisonTypes.EQUAL;
import static ibd.table.ComparisonTypes.GREATER_THAN;
import static ibd.table.ComparisonTypes.LOWER_EQUAL_THAN;
import ibd.table.Directory;
import ibd.table.Table;
import static ibd.table.Utils.createTable;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sergio
 */
public class Main2 {

    //returns all records from a table
    public Operation testSimpleQuery() throws Exception {
        //Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, true, 1, 50);
        Table table1 = Directory.getTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE,  false);
        Operation scan = new IndexScan("t1", table1);
        return scan;

    }

    //returns the record that contains a specific pk
    public Operation testFilterQuery(long value) throws Exception {
        Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, true, 1, 50);
        //Table table1 = Directory.getTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE,  false);
        Operation scan1 = new FullTableScan("t1", table1);
        PKFilter scan = new PKFilter(scan1, "t1", EQUAL, value);
        return scan;
    }

    //returns all records whose pk and content satisfy a greater than and equality filters, respectively.
    public Operation testFilterQuery(long pk, String value) throws Exception {
        //Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, true, 1);
        Table table1 = Directory.getTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, false);
        Operation scan1 = new FullTableScan("t1", table1);
        ContentFilter scan = new ContentFilter(scan1, "t1", EQUAL, value);
        PKFilter scan2 = new PKFilter(scan, "t1", GREATER_THAN, pk);
        return scan2;
    }

    //returns all records from a table sorted by content
    public Operation testSortQuery() throws Exception {
        Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, true, 1, 50);
        //Operation scan = new PKSort(new FullTableScan("t1", table1), "t1");
        Operation scan = new ContentSort(new FullTableScan("t1", table1), "t1");
        return scan;

    }

    //returns all records produced after a join between two tables using nested loop join.
    public Operation testNestedLoopJoinQuery() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, true, 1, 50);
        Table table2 = createTable("c:\\teste\\ibd", "tab2", Table.DEFULT_PAGE_SIZE, 1000, true, 20, 50);

        //Table table1 = Directory.getTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE,  false);
        //Table table2 = Directory.getTable("c:\\teste\\ibd", "tab2", Table.DEFULT_PAGE_SIZE,  false);
        
        Operation scan1 = new FullTableScan("t1", table1);
        Operation scan2 = new FullTableScan("t2", table2);

        Operation join1 = new NestedLoopJoin(scan1, scan2);
        return join1;

    }

    //returns all records produced after a join between two tables using indexed nested loop join.
    public Operation testIndexedNestedLoopJoinQuery() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);
        Table table2 = createTable("c:\\teste\\ibd", "tab2", Table.DEFULT_PAGE_SIZE, 1000, false, 25, 50);

        Operation scan1 = new FullTableScan("t1", table1);
        Operation scan2 = new FullTableScan("t2", table2);
        //use this to rely on the existing physical btree index over the table
        Operation indexed = new IndexScan("t2", table2);
        //use this to built an in-memory index over the table specifically to answer this query
        Operation indexed2 = new PKHashIndex(scan2, "t2");

        //use indexed instead of indexed2 if you want the in-memory index instead of the physical index
        Operation join1 = new NestedLoopJoin(scan1, indexed2);
        return join1;

    }

    //returns all records produced after a join between two tables and a filter.
    public Operation testNestedLoopJoinQueryWithFilter(long pk) throws Exception {

        Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);
        Table table2 = createTable("c:\\teste\\ibd", "tab2", Table.DEFULT_PAGE_SIZE, 1000, false, 50, 50);

        Operation scan1 = new FullTableScan("t1", table1);
        Operation scan2 = new FullTableScan("t2", table2);
        PKFilter filter = new PKFilter(scan2, "t2", EQUAL, pk);

        Operation join1 = new NestedLoopJoin(scan1, filter);
        return join1;

    }

    //returns all records produced after a join among three tables.
    public Operation testNestedLoopJoinQuery1() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd", "tab1", Table.DEFULT_PAGE_SIZE, 1000, false, 5, 50);
        Table table2 = createTable("c:\\teste\\ibd", "tab2", Table.DEFULT_PAGE_SIZE, 1000, false, 2, 50);
        Table table3 = createTable("c:\\teste\\ibd", "tab3", Table.DEFULT_PAGE_SIZE, 1000, false, 1, 50);

        Operation scan1 = new FullTableScan("t1", table1);
        Operation scan2 = new IndexScan("t2", table2);
        Operation scan3 = new IndexScan("t3", table3);

        Operation join1 = new NestedLoopJoin(scan1, scan2);
        Operation join2 = new NestedLoopJoin(join1, scan3);

        return join2;

    }

    public void run(Operation op) throws Exception{
    
    Params.BLOCKS_LOADED = 0;
            Params.BLOCKS_SAVED = 0;

            int count = 0;
            op.open();
            Iterator<Tuple> it = op.run();
            //Iterator<Tuple> it = op.lookUp(new PKLookupFilter(20, EQUAL, 0));
            while (it.hasNext()) {
                Tuple r = it.next();
                System.out.println(r);
                count++;
            }
            op.close();

            System.out.println("number of records: " + count);

            System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
            System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);
    }

    public static void main(String[] args) {
        try {
            Main2 m = new Main2();

            //Operation op = m.testUpdateQuery(990, "xxxxxxxxxx");
            
            Operation op = m.testNestedLoopJoinQuery();
            //Operation op = m.testIndexedNestedLoopJoinQuery();
            //Operation op = m.testNestedLoopJoinQuery1();

            //Operation op = m.testNestedLoopJoinQueryWithFilter();
            //Operation op = m.testFilterQuery("Ana");         
            //Operation op = m.testSimpleQuery();
            //Operation op = m.testSortQuery();
            
            
            //op = new ContentSort(op, "t1");
            //op = new PKFilter(op, "t1", GREATER_THAN, 20);
            

            
            //op = new ibd.query.unaryop.DuplicateRemoval(op, "t1", false);
            m.run(op);
            

        } catch (Exception ex) {
            Logger.getLogger(Main2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
