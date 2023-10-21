/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop.filter;

import ibd.query.Operation;
import ibd.query.lookup.ContentLookupFilter;
import ibd.query.lookup.LookupFilter;
import ibd.table.ComparisonTypes;

/**
 * A content filter operation filters tuples that come from its child operation.
 * The source tuple used as the filter condition comes from the data source defined by the specify alias.
 * The filter compares the content of the source tuple against a fixed value. 
 * @author Sergio
 */
public class ContentFilter extends Filter {

    /*
    * the value to be compared against the content of the chosen source tuple.
    */
    String value;

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @param comparisonType the type of comparison that needs to be satisfied (e.g. =, < , >)
     * @param value the value to be compared against the content of the source tuple
     * @throws Exception
     */
    public ContentFilter(Operation childOperation, String dataSourceAlias, int comparisonType, String value) throws Exception {
        super(childOperation, dataSourceAlias, comparisonType);
        this.value = value;
    }

    /**
     *
     * @return the value to be compared against the content of the source tuple
     */
    public String getValue() {
        return value;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        if (dataSourceAlias == null) {
            return "Content Filter " + ComparisonTypes.getComparisonType(comparisonType) + " " + value;
        } else {
            return "Content Filter [" + dataSourceAlias + "]" + ComparisonTypes.getComparisonType(comparisonType) + " " + value;
        }
    }

    /**
     * {@inheritDoc }
     * The filter condition defined compares the content of a source tuple against a fixed value.
     */
    @Override
    public LookupFilter createLookupFilter(int tupleIndex) {
        return new ContentLookupFilter(value, comparisonType, tupleIndex);
    }
}
