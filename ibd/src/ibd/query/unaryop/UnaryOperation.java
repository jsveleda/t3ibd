/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.unaryop;

import ibd.query.Operation;


/**
 * An unary operation accesses data that comes from a child operation to perform data transformation.
 * A tuple index identifies the source tuple that drives the execution. 
 * The tuple index is set based on the alias of a data source. 
 * @author Sergio
 */
public abstract class UnaryOperation extends Operation {

    /**
     * the child operation.
     */
    protected Operation childOperation;
    
    /**
     * the alias of the data source
     */
    protected String dataSourceAlias;

    /**
     * the index of the source tuple
     */
    protected int sourceTupleIndex = -1;

    /**
     *
     * @param childOperation the child operation
     * @param dataSourceAlias the alias of the data source
     * @throws Exception
     */
    public UnaryOperation(Operation childOperation, String dataSourceAlias) throws Exception {
        setChildOperation(childOperation);
        this.dataSourceAlias = dataSourceAlias;
    }

    /**
     * sets the child operation
     * @param childOperation the child operation
     */
    public final void setChildOperation(Operation childOperation) {
        this.childOperation = childOperation;
        childOperation.setParentOperation(this);
    }
    
    /**
     *
     * @return the child operation
     */
    public Operation getChildOperation() {
        return childOperation;
    }

    /**
     *
     * @return the alias of the data source used by this unary operation to produce tuples
     */
    public String getDataSourceAlias() {
        return dataSourceAlias;
    }

    /**
     * {@inheritDoc }
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        //the opening must occur from the bottom-up :the subtree must be prepared before this operation node
        childOperation.open();
        super.open();
        //sets the tuple index
        setSourceTupleIndex();

    }
    
    /**
     * sets the source tuple index.
     * if no alias is provided, the first tuple is selected (index 0) 
     */
    private void setSourceTupleIndex() throws Exception {
        sourceTupleIndex = -1;
        if (dataSourceAlias == null) {
            if (dataSourcesAliases.length > 0) {
                dataSourceAlias = dataSourcesAliases[0];
                sourceTupleIndex = 0;
            } else {
                throw new Exception("source not found");
            }
            return;
        }

        String[] sources = getDataSourcesAliases();
        for (int i = 0; i < sources.length; i++) {

            if (sources[i].equals(dataSourceAlias)) {
                sourceTupleIndex = i;
                break;
            }

        }
        if (sourceTupleIndex == -1) {
            throw new Exception("source not found");
        }
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception{
        childOperation.close();
    }

    /**
     * {@inheritDoc }
     * copies the aliases from the child operation to this unary operation
     * @throws Exception
     */
    @Override
    protected void setDataSourcesAliases() throws Exception {
        String s[] = childOperation.getDataSourcesAliases();
        dataSourcesAliases = new String[s.length];
        System.arraycopy(s, 0, dataSourcesAliases, 0, s.length);

    }

    
}
