/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package ibd.query.lookup;

/**
 * defines a filter that actually needs to be satisfied, as opposed to NoLookup filters; 
 * onyl real lookup filter need a comparison type and the identification of a source tuple to be used as an operand.
 * the first operand needs to be defined by sub-classes that extend this class.
 * @author Sergio
 */
public abstract class RealLookupFilter implements LookupFilter{

    
    
    /*
    * the comparison type
    */
    protected int comparisonType;
    
    /*
    * the identification of the source tuple whose information is to be used as the second operand.
    */
    protected int tupleIndex;

    /**
     * @return the comparisonType
     */
    public int getComparisonType() {
        return comparisonType;
    }

    public RealLookupFilter(int comparisonType, int tupleIndex){
        this.comparisonType = comparisonType;
        this.tupleIndex = tupleIndex;
    }
    
    
    /**
     * @return the tupleIndex
     */
    public int getTupleIndex() {
        return tupleIndex;
    }
    
}
