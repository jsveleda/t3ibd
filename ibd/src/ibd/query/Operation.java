/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query;

import ibd.query.lookup.LookupFilter;
import ibd.query.lookup.NoLookupFilter;
import java.util.Iterator;

/**
 * An operation performs transformation over data. 
 * Operations can be connected in a tree. 
 * The tree is a query execution plan, where each node(operation) has its own responsability in the data access/transformation process. 
 * The operations can be source, unary or binary operations.
 * Source operations access a data source directly.
 * Unary Operations access another operation to perform data transformation.
 * Binary OPerations access two other operations to perform data transformation.
 * The leaf nodes are source operations. They provide ascess to the data sources. 
 * The root node expresses the whole query.
 * Queries are posed over any node, which redirects the query to the operations below it, in a pipeline fashion. 
 * @author Sergio
 */
public abstract class Operation {

    /**
     * the aliases of the data sources accessed by this operation and the operations below it
     * this information is useful to identify which part of a returning tuple comes from a specific data source. 
     * Given the alias, the source tuple can be located and used by the transformation process. 
     */
    protected String[] dataSourcesAliases;

    /**
     * the parent oepration, if one exists
     */
    Operation parentOperation;

    /**
     * @return the data sources accessed by this operation and the operations below it
     * @throws Exception
     */
    public String[] getDataSourcesAliases() throws Exception {
        return dataSourcesAliases;
    }

    /**
     * Prepares this operation for query answering performing one-time setup commands 
     * The preparation involves:
     *   - setting up static variables
     *   - setting the aliases of the data sources that this operation is reaching
     *   - setting indexes to data sources
     *   - opening data sources, if any
     *   - opening the tree below this operations, if any
     * @throws Exception
     */
    public void open() throws Exception {
        setDataSourcesAliases();
    }

    /**
     * Sets the parent operation
     * @param op
     */
    public void setParentOperation(Operation op) {
        parentOperation = op;
    }

    /**
     * @return the parent operation
     */
    public Operation getParentOperation() {
        return parentOperation;
    }

    /**
     * sets the aliases of the data sources that are accessed by this operation and the operations below it. 
     * @throws Exception
     */
    protected abstract void setDataSourcesAliases() throws Exception;

    /**
     * Runs a query, using the tree below this operation to transform data and produce resulting tuples.
     * @param filter the lookup filter that needs to be satisfied.
     * @return an iterator containing the tuples that answers the query.
     */
    public abstract Iterator<Tuple> lookUp(LookupFilter filter);

    /**
     * Runs a query, using the tree below this operation to transform data and produce resulting tuples.
     * This function uses the lookUp functions with an empty filter parameter.
     * @return an iterator containing the tuples that answers teh query
     */
    public Iterator<Tuple> run(){
        
        return lookUp(new NoLookupFilter());
    }
    
    /**
     * Cleans up resources, if necessary.
     * The tree below this operation also has to closed. 
     * @throws Exception
     */
    public abstract void close() throws Exception;

}
