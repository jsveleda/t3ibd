/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.lookup;

import ibd.query.Tuple;


/**
 * this filter finds tuples based on pk comparison. 
 * @author Sergio
 */
public class PKLookupFilter extends RealLookupFilter{

    /*
    * the first operand of this filter
    */
    long pk;
    
    /**
     *
     * @param pk the first operand of this filter
     * @param comparisonType the comparison type
     * @param tupleIndex the index of the source tuple that corresponds to the second operand
     */
    public PKLookupFilter(long pk,int comparisonType, int tupleIndex){
        super(comparisonType, tupleIndex);
        this.pk = pk;
    }
    
    /**
     *
     * @return the value of the first operand
     */
    public Long getPk(){
        return pk;
    }
    
    /**
     * {@inheritDoc }
     */
    @Override
    public boolean match(Tuple tuple) {
        //compares the pk against the pk that comes from a source tuple
        return LookupFilter.match(tuple.sourceTuples[tupleIndex].record.getPrimaryKey(), pk, comparisonType);
        //return tuple.sourceTuples[tupleIndex].record.getPrimaryKey().equals(pk);
    }
    
}
