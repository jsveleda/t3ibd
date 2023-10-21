/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop.filter;

import ibd.query.Operation;
import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.PKLookupFilter;
import ibd.table.ComparisonTypes;

/**
 * A pk filter operation filters tuples that come from its child operation.
 * The source tuple used as the filter condition comes from the data source defined by the specify alias.
 * The filter compares the pk of the source tuple against a fixed value. 
 * @author Sergio
 */
public class PKFilter extends Filter {

    /*
    * the pk value to be compared against the pk of the chosen source tuple.
    */
    long pkvalue;

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @param comparisonType the type of comparison that needs to be satisfied (e.g. =, < , >)
     * @param value the value to be compared against the pk of the source tuple
     * @throws Exception
     */
    public PKFilter(Operation childOperation, String dataSourceAlias, int comparisonType, long value) throws Exception {
        super(childOperation, dataSourceAlias, comparisonType);
        this.pkvalue = value;
    }

    /**
     *
     * @return the pk value to be compared against the pk of the source tuple
     */
    public Long getValue() {
        return pkvalue;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        if (dataSourceAlias == null) {
            return "PK Filter " + ComparisonTypes.getComparisonType(comparisonType) + " " + pkvalue;
        } else {
            return "PK Filter [" + dataSourceAlias + "]" + ComparisonTypes.getComparisonType(comparisonType) + " " + pkvalue;
        }
    }

    /**
     * {@inheritDoc }
     * The filter condition defined compares the pk of a source tuple against a fixed value.
     */
    @Override
    public LookupFilter createLookupFilter(int tupleIndex) {
        return new PKLookupFilter(pkvalue, comparisonType, tupleIndex);
    }
}
