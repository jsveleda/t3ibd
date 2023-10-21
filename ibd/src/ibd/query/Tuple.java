/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query;

import ibd.table.record.Record;

/**
 * A tuple is a concatenation of source tuples.
 * It is produced as source tuples are combined, for instance, by join operations.
 * @author Sergio
 */
public class Tuple 
{
    
    /**
     * the source tuples combined.
     */
    public SourceTuple sourceTuples[];
    

    /**
     * Defines a single source of information for this tuple.
     * Useful when the tuples are produced by a source operation that has direct access to the records.
     * @param dataSourceAlias the alias of the data source
     * @param record the record accessed through the data source
     */
    public void setSingleSourceTuple(String dataSourceAlias, Record record){
    SourceTuple sourceTuple = new SourceTuple();
    sourceTuple.source = dataSourceAlias;
    sourceTuple.record = record;
    //sourceTuple.primaryKey = primaryKey;    
    //sourceTuple.content = content;
    sourceTuples = new SourceTuple[1];
    sourceTuples[0] = sourceTuple;
    }
    
    /**
     * sets the source tuples directly.
     * @param sourceTuples
     */
    public void setSources(SourceTuple sourceTuples[]){
    this.sourceTuples = sourceTuples;
    }
    
    /**
     * Copies the tuple sources from one tuple to this tuple. 
     * Useful when the tuple is produced by taking data from a single tuple, as in unary operations. 
     * @param t
     */
    public void setSourceTuples(Tuple t){
    sourceTuples = new SourceTuple[t.sourceTuples.length];
    int count = 0;
        for (SourceTuple sourceTuple : t.sourceTuples) {
            sourceTuples[count] = sourceTuple;
            count++;
        }
    
    }
    
    

    /**
     * Copies the tuple sources from two tuples to this tuple. 
     * Useful when the tuple is produced as the result of a join operation. 
     * @param t1
     * @param t2
     */
    public void setSourceTuples(Tuple t1, Tuple t2){
    sourceTuples = new SourceTuple[t1.sourceTuples.length+t2.sourceTuples.length];
    int count = 0;
        for (SourceTuple sourceTuple : t1.sourceTuples) {
            sourceTuples[count] = sourceTuple;
            count++;
        }
        for (SourceTuple sourceTuple : t2.sourceTuples) {
            sourceTuples[count] = sourceTuple;
            count++;
        }
    
    }
    
    /**
     *
     * @return
     */
    public int size(){
        return sourceTuples.length * Record.RECORD_SIZE;
    }
    
    /**
     * Finds the index of a source tuple by the alias of the data source
     * @param dataSourceTuple
     * @return the index of a source tuple 
     */
    public int findSourceIndexByName(String dataSourceTuple){
        for (int i = 0; i < sourceTuples.length; i++) {
            SourceTuple st = sourceTuples[i];
            if (st.source.equals(dataSourceTuple))
                return i;
            
        }
        return -1;
    }
    
    /**
     *
     * @return
     */
    @Override
    public String toString(){
        String str = new String();
        for (SourceTuple st : sourceTuples) {
            str+=st.record.getPrimaryKey()+":"+st.record.getContent().trim()+", ";
        }
        return str;
    }
    
   
}
