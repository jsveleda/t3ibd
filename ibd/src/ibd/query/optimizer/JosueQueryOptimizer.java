package ibd.query.optimizer;

import ibd.query.Operation;
import ibd.query.binaryop.BinaryOperation;
import ibd.query.binaryop.NestedLoopJoin;
import ibd.query.sourceop.IndexScan;
import ibd.query.unaryop.UnaryOperation;
import ibd.query.unaryop.filter.Filter;
import ibd.query.unaryop.filter.PKFilter;
import ibd.table.ComparisonTypes;

public class JosueQueryOptimizer implements QueryOptimizer {
    private Operation rootNode;

    @Override
    public Operation optimize(Operation op) {
        rootNode = op;
        ReorderOperations(op);
        return rootNode;
    }

    private void ReorderOperations(Operation op){
        if(op instanceof BinaryOperation){
            BinaryOperation binaryOperation = (BinaryOperation) op;

            ReorderOperations(binaryOperation.getLeftOperation());
            ReorderOperations(binaryOperation.getRightOperation());
        }
        else if(op instanceof UnaryOperation){
            UnaryOperation unaryOperation = (UnaryOperation) op;
            Operation childOperation = unaryOperation.getChildOperation();

            if(IsEqualPkFilter(unaryOperation)){
                ReorderByEqualPkFilter((PKFilter) unaryOperation);
            }
            else if(op instanceof Filter){
                ReorderByFilter((Filter) op);
            }

            ReorderOperations(childOperation);
        }
    }

    private void ReorderByFilter(Filter filter) {
        if (!IsRightChildFromNestedLoop(filter)
            || !FetchEqualPkFilterByAlias(filter, filter.getDataSourceAlias())) {
            return;
        }

        BinaryOperation filterParent = (BinaryOperation) filter.getParentOperation();

        filterParent.setRightOperation(filter.getChildOperation());
        filter.getChildOperation().setParentOperation(filterParent);

        filter.setParentOperation(filterParent.getParentOperation());
        filter.setChildOperation(filterParent);

        filterParent.setParentOperation(filter);

        if(filter.getParentOperation() == null){
            rootNode = filter;
        }
        else if (filter.getParentOperation() instanceof BinaryOperation) {
            BinaryOperation binaryOperation = (BinaryOperation) filter.getParentOperation();

            if(binaryOperation.getLeftOperation().equals(filterParent)){
                binaryOperation.setLeftOperation(filter);
            }
            else {
                binaryOperation.setRightOperation(filter);
            }
        }
        else if (filter.getParentOperation() instanceof UnaryOperation){
            ((UnaryOperation) filter.getParentOperation()).setChildOperation(filter);
        }
    }

    private boolean FetchEqualPkFilterByAlias(Operation op, String alias) {
        if(IsEqualPkFilter(op) && ((PKFilter) op).getDataSourceAlias().equals(alias)){
            return false;
        }

        if(op instanceof BinaryOperation){
            return FetchEqualPkFilterByAlias(((BinaryOperation) op).getRightOperation(), alias);
        }
        else if(op instanceof UnaryOperation){
            return FetchEqualPkFilterByAlias(((UnaryOperation) op).getChildOperation(), alias);
        }

        return true;
    }

    private static boolean IsRightChildFromNestedLoop(Filter op) {
        return op.getParentOperation() instanceof NestedLoopJoin
                && ((BinaryOperation) op.getParentOperation()).getRightOperation().equals(op);
    }

    private void ReorderByEqualPkFilter(PKFilter pkFilter) {
        IndexScan indexScan = FindIndexScanByAlias(pkFilter, pkFilter.getDataSourceAlias());
        if (indexScan == null) {
            return;
        }

        if(pkFilter.getParentOperation() == null){
            rootNode = pkFilter.getChildOperation();
        }
        else {
            ReorderOperationChild(pkFilter, pkFilter.getChildOperation());
        }
        pkFilter.getChildOperation().setParentOperation(pkFilter.getParentOperation());
        pkFilter.setParentOperation(indexScan.getParentOperation());

        ReorderOperationChild(indexScan, pkFilter);

        pkFilter.setChildOperation(indexScan);
        indexScan.setParentOperation(pkFilter);
    }

    private static void ReorderOperationChild(Operation op, Operation childOperation) {
        if(op.getParentOperation() instanceof BinaryOperation) {
            BinaryOperation binaryOperation = ((BinaryOperation) op.getParentOperation());

            if(binaryOperation.getLeftOperation().equals(op)){
                binaryOperation.setLeftOperation(childOperation);
            }
            else{
                binaryOperation.setRightOperation(childOperation);
            }
        }
        else if(op.getParentOperation() instanceof UnaryOperation) {
            UnaryOperation unaryOperation = ((UnaryOperation) op.getParentOperation());

            unaryOperation.setChildOperation(childOperation);
        }
    }

    private IndexScan FindIndexScanByAlias(Operation op, String alias){
        if(op instanceof IndexScan
            && ((IndexScan) op).getDataSourceAlias().equals(alias)
            && !IsEqualPkFilter(op.getParentOperation())){
            return (IndexScan) op;
        }

        if(op instanceof BinaryOperation){
            BinaryOperation binaryOperation = (BinaryOperation) op;

            IndexScan leftResult = FindIndexScanByAlias(binaryOperation.getLeftOperation(), alias);
            if(leftResult != null){
                return leftResult;
            }

            return FindIndexScanByAlias(binaryOperation.getRightOperation(), alias);
        }
        else if(op instanceof UnaryOperation){
            UnaryOperation unaryOperation = (UnaryOperation) op;

            return FindIndexScanByAlias(unaryOperation.getChildOperation(), alias);
        }

        return null;
    }

    private static boolean IsEqualPkFilter(Operation op) {
        return op instanceof PKFilter
                && ((PKFilter) op).getComparisonType() == ComparisonTypes.EQUAL;
    }
}