/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.lookup;

import ibd.query.Tuple;

/**
 * this filter finds tuples based on the tuple content comparison. 
 * @author Sergio
 */
public class ContentLookupFilter extends RealLookupFilter {

    /*
    * the first operand of this filter
    */
    String content;

    /**
     *
     * @param content
     * @param comparisonType
     * @param tupleIndex
     */
    public ContentLookupFilter(String content, int comparisonType, int tupleIndex) {
        super(comparisonType, tupleIndex);
        this.content = content;
    }

    /**
     * @return return the value of the first operand
     */
    public String getContent() {
        return content;
    }

    /**
     *
     * @param tuple
     * @return
     */
    @Override
    public boolean match(Tuple tuple) {
        //compares the content against the content that comes from a source tuple
        return LookupFilter.match(tuple.sourceTuples[tupleIndex].record.getContent(),content,  comparisonType);
    }

}
