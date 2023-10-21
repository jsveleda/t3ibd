/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd.query.binaryop;

import ibd.query.Operation;

/**
 * Defines the template of a binary operation.
 * A binary operation accesses data that comes from two operations (from the left and from the right) to perform data transformation.
 * The data transformation is based on two source tuples: one that comes from the left operation and one that comes from the right operation.
 * The left and right tuple indexes identify the source tuples that drive the execution. 
 * The indexes are computed based on the alias of the corresponding data sources. 
 * @author Sergio
 */
public abstract class BinaryOperation extends Operation {

    /**
     * the left side operation
     */
    protected Operation leftOperation;

    /**
     * the right side operation
     */
    protected Operation rightOperation;

    /**
     * the alias of the data source from the left side
     */
    protected String leftDataSourceAlias;

    /**
     * the alias of the data source from the right side
     */
    protected String rightDataSourceAlias;

    /**
     * the index of the source tuple from left side
     */
    protected int leftSourceTupleIndex = -1;

    /**
     * the index of the source tuple from the right side
     */
    protected int rightSourceTupleIndex = -1;

    /**
     *
     * @param leftOperation the left side operation
     * @param rightOperation the right side operation
     * @throws Exception
     */
    public BinaryOperation(Operation leftOperation, Operation rightOperation) throws Exception {
        setLeftOperation(leftOperation);
        setRightOperation(rightOperation);
    }

    /**
     *
     * @param leftOperation the left side operation
     * @param leftDataSourceAlias the alias of the left side data source
     * @param rightOperation the right side operation
     * @param rightDataSourceAlias the alias of the right side data source
     * @throws Exception
     */
    public BinaryOperation(Operation leftOperation, String leftDataSourceAlias, Operation rightOperation, String rightDataSourceAlias) throws Exception {
        this(leftOperation, rightOperation);
        this.leftDataSourceAlias = leftDataSourceAlias;
        this.rightDataSourceAlias = rightDataSourceAlias;
    }

    /**
     * {@inheritDoc }
     *
     * @throws Exception
     */
    @Override
    public void open() throws Exception {
        //the opening must occur from the bottom-up :the subtree must be prepared before this operation node
        getLeftOperation().open();
        getRightOperation().open();
        super.open();

        //sets the tuple indexes used to perform this binary operation
        setSourceTupleIndexes();
    }

    /*
    * sets the tuple indexes
    * the indexes are set based on the defined alias of the data sources
     */
    private void setSourceTupleIndexes() throws Exception {
        setLeftSideSourceTupleIndex();
        setRightSideSourceTupleIndex();

    }

    /*
    * sets the source tuple index from the left side used to perform this binary operation
    * the index is set based on the defined alias that identifies the source tuple
    * if an invalid alias is used, the first source tuple is chosen (index 0)
     */
    private void setLeftSideSourceTupleIndex() throws Exception {
        if (leftDataSourceAlias == null) {
            leftSourceTupleIndex = 0;
        } else {
            String[] leftSourcesAliases = leftOperation.getDataSourcesAliases();
            for (int i = 0; i < leftSourcesAliases.length; i++) {
                if (leftSourcesAliases[i].equals(leftDataSourceAlias)) {
                    leftSourceTupleIndex = i;
                    break;
                }

            }
        }
        if (leftSourceTupleIndex == -1) {
            throw new Exception("source not found");
        }

    }

    /*
    * sets the source tuple index from the right side used to perform this binary operation
    * the index is set based on the defined alias that identifies the source tuple
    * if an invalid alias is used, the first source tuple is chosen (index 0)
     */
    private void setRightSideSourceTupleIndex() throws Exception {
        if (rightDataSourceAlias == null) {
            rightSourceTupleIndex = 0;
        } else {

            String[] rightSourcesAliases = rightOperation.getDataSourcesAliases();
            for (int i = 0; i < rightSourcesAliases.length; i++) {
                if (rightSourcesAliases[i].equals(rightDataSourceAlias)) {
                    rightSourceTupleIndex = i;
                    break;
                }

            }
            if (rightSourceTupleIndex == -1) {
                throw new Exception("source not found");
            }
        }
    }

    /**
     * {@inheritDoc }
     * the data sources aliases is a concatenation of the data sources aliases that come from the left and the right subtrees
     * @throws Exception
     */
    @Override
    protected void setDataSourcesAliases() throws Exception {

        String left[] = getLeftOperation().getDataSourcesAliases();
        String right[] = getRightOperation().getDataSourcesAliases();
        dataSourcesAliases = new String[left.length + right.length];
        int count = 0;
        for (int i = 0; i < left.length; i++) {
            dataSourcesAliases[count] = left[i];
            count++;
        }
        for (int i = 0; i < right.length; i++) {
            dataSourcesAliases[count] = right[i];
            count++;
        }

    }

    /**
     * sets the left side operation
     *
     * @param op
     */
    public final void setLeftOperation(Operation op) {
        leftOperation = op;
        op.setParentOperation(this);
    }

    /**
     * sets the right side operation
     *
     * @param op
     */
    public final void setRightOperation(Operation op) {
        rightOperation = op;
        op.setParentOperation(this);
    }

    /**
     * gets the left side operation
     *
     * @return
     */
    public Operation getLeftOperation() {
        return leftOperation;
    }

    /**
     * gets the right side operation
     *
     * @return
     */
    public Operation getRightOperation() {
        return rightOperation;
    }

    /**
     * gets the alias of the left side data source
     * @return
     */
    public String getLeftSourceAlias() {
        return leftDataSourceAlias;
    }

    /**
     * gets the alias of the right side data source
     * @return
     */
    public String getRigthSourceAliases() {
        return rightDataSourceAlias;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        leftOperation.close();
        rightOperation.close();
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "Binary Operation";
    }

}
