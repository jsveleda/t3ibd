/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.table;

import ibd.table.record.Record;
import java.util.List;

public abstract class Table implements Iterable {

    public static final int DEFULT_PAGE_SIZE = 4096;

    public String tableKey;


    public abstract Record getRecord(Long primaryKey) throws Exception;

    public abstract List<? extends Record> getRecords(Long primaryKey, int comparisonType) throws Exception;

    public abstract Record addRecord(Long primaryKey, String content) throws Exception;

    public abstract Record updateRecord(Long primaryKey, String content) throws Exception;
        
    public abstract Record removeRecord(Long primaryKey) throws Exception;

    public abstract void flushDB() throws Exception;

    public abstract int getRecordsAmount() throws Exception;
    
    public abstract void printStats() throws Exception;

    
}
