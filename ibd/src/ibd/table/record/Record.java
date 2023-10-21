/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd.table.record;


/**
 *
 * @author pccli
 */
public class Record implements Comparable<Record>{
    
    public final static Integer RECORD_SIZE = 350;
    
    protected final Long primaryKey;
    protected String content;
    
    public Record(Long pk) {
        primaryKey = pk;
    }
    
    /**
     * @return the data_id
     */
    public Long getPrimaryKey() {
        return primaryKey;
    }
    
    /**
     * @return the content
     */
    public String getContent() {
        return content;
    }

    /**
     * @param content the content to set
     */
    public void setContent(String content) {

        if (content.length() > RECORD_SIZE) {
            this.content = content.substring(0, RECORD_SIZE);
        } else {
            this.content = content;
        }


    }
    

    @Override
    public int compareTo(Record o) {
        return getPrimaryKey().compareTo(o.getPrimaryKey());
    }
    
    
}
