/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.lookup;

import ibd.query.Tuple;
import ibd.table.ComparisonTypes;

/**
 * A lookup filter is responsible for finding tuples that satisfy a filter condition. 
 * The condition is defined by sub-classes that implement this interface.
 * Operation can use the match() function or access the conditions directly if the filter needs to be performed in a different  way.
 * For instance, to perform the lookup efficiently, one may pass the condition to an index.
 * @author Sergio
 */
public interface LookupFilter {
    
    /**
     * 
     * @param tuple the tuple to be compared
     * @return true if the tuple matches this lookupfilter condition
     */
    public boolean match(Tuple tuple);
    
    /**
     * 
     * @param value1 the first operand
     * @param value2 the second operand
     * @param comparisonType the comparison type
     * @return true if the comparable object match according to the comparison type
     */
    public static boolean match(Comparable value1, Comparable value2, int comparisonType) {

        int resp = value1.compareTo(value2);
        if (resp == 0 && (comparisonType == ComparisonTypes.EQUAL
                || comparisonType == ComparisonTypes.LOWER_EQUAL_THAN
                || comparisonType == ComparisonTypes.GREATER_EQUAL_THAN)) {
            return true;
        } else if (resp < 0 && (comparisonType == ComparisonTypes.LOWER_THAN
                || comparisonType == ComparisonTypes.LOWER_EQUAL_THAN)) {
            return true;
        } else if (resp > 0 && (comparisonType == ComparisonTypes.GREATER_THAN
                || comparisonType == ComparisonTypes.GREATER_EQUAL_THAN)) {
            return true;
        } else if (resp != 0 && comparisonType == ComparisonTypes.DIFF) {
            return true;
        } else {
            return false;
        }
    }
}
