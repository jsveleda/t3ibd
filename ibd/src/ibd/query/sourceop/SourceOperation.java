/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.sourceop;

import ibd.query.Operation;


/**
 * A source operation is a leaf node. Its provides direct access to a data source.
 * The data source has as alias. The alias is used by other operations to identify source tuples that come from a specific data source
 * @author Sergio
 */
public abstract class SourceOperation extends Operation{
    
    //the alias of the data source accessed by this source operation
    String dataSourceAlias;
    
    /**
     *
     * @param dataSourceAlias the alias of the data source accessed by this source operation
     */
    public SourceOperation(String dataSourceAlias){
        this.dataSourceAlias = dataSourceAlias;
        
    }
    
    /**
     * 
     * @return the alias of the data source accessed by this source operation
     */
    public String getDataSourceAlias() {
    return dataSourceAlias;
    }

    /**
     * {@inheritDoc }
     *  Source operations are leaf nodes and reach a single data source. 
     * Therefore, a single alias is needed, which is the alias of the data source.
     * @throws Exception
     */
    @Override
    protected void setDataSourcesAliases() throws Exception {
        dataSourcesAliases = new String[1];
        dataSourcesAliases[0] = dataSourceAlias;
    }
    
    /**
     *
     * @return
     */
    @Override
     public String toString(){
         return "["+dataSourceAlias+"]";
     }
    
}
