package edu.caltech.nanodb.qeval;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.FromClause.ClauseType;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.AggregateReplacementProcessor;
import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.CompareOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.HashedGroupAggregateNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;
import edu.caltech.nanodb.plans.SortNode;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class generates execution plans for performing SQL queries.
 */
public class GeneralPlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a prepared plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
                             List<SelectClause> enclosingSelects) throws IOException {
        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not yet implemented:  enclosing queries!");
        }

        PlanNode res = makeGeneralSelect(selClause);
        res.prepare();
        return res;
    }

    /**
     * Constructs a plan node for the from clause of the given select statement.
     * @param selClause
     * @return A plan node for the clause, or null if there is no such clause.
     * @throws IOException
     */
    private PlanNode planFromClause(SelectClause selClause) 
            throws IOException {
        FromClause fromClause = selClause.getFromClause();
        if (fromClause == null) {
            return null;
        }
        PlanNode planNode = null;
        if (fromClause.isBaseTable()) {
            planNode = makeSimpleSelect(fromClause.getTableName(),
                    null, null);
            planNode = planWhereClause(planNode, selClause);
        }
        else if (fromClause.isJoinExpr()){
            planNode = makeJoinExpression(fromClause);
        }
        else {
            planNode = makeGeneralSelect(fromClause.getSelectClause());
            planNode = new RenameNode(planNode, fromClause.getResultName());
        }
        return planNode;
    }

    /**
     * Returns the root of a plan tree for executing the given query.
     * @param selClause The select statement.
     * @return A plan tree for executing the query.
     * @throws IOException
     */
    private PlanNode makeGeneralSelect(SelectClause selClause) throws IOException {
        PlanNode res = planFromClause(selClause);
        res = planWhereClause(res, selClause);
        res = planGroupingAggregation(res, selClause);
        res = planHavingClause(res, selClause);
        res = planProjectClause(res, selClause);
        res = planOrderByClause(res, selClause);
        return res;
    }

    /**
     * Returns a plan node for the where clause in the select statement.
     *
     * @param child The child of the resultant node.
     * @param selClause
     * @return the resultant node
     * @throws IOException
     */
    private PlanNode planWhereClause(PlanNode child, SelectClause selClause) throws IOException {
        if (selClause.getWhereExpr() == null) {
            return child;
        }
        SelectNode selNode = new SimpleFilterNode(child, selClause.getWhereExpr());
        return selNode;
    }

    /**
     * Returns a plan node for the grouping/aggregation part of the select statement.
     *
     * Aggregation function calls are replaced with column references by AggregateReplacementProcessor. These,
     * in turn, are employed during evaluation.
     *
     * @param child The child of the resultant node.
     * @param selClause
     * @return The resultant node.
     */
    private PlanNode planGroupingAggregation(PlanNode child, SelectClause selClause) {
        
        List<Expression> groupByExprs = selClause.getGroupByExprs();

        // Replace aggregate function calls with column references.
        AggregateReplacementProcessor processor = new AggregateReplacementProcessor();
        
        for (SelectValue sv : selClause.getSelectValues()) {
            if (!sv.isExpression())
            continue;
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        // Update the having expression
        if (selClause.getHavingExpr() != null) {
            Expression e = selClause.getHavingExpr().traverse(processor);
            selClause.setHavingExpr(e);
        }

        // Make sure there are no aggregate functions in the where / from clauses.
        if (selClause.getWhereExpr() != null) {
            processor.setErrorMessage("Aggregate functions in WHERE clauses are not allowed");
            selClause.getWhereExpr().traverse(processor);
        }
        if (selClause.getFromClause() != null && selClause.getFromClause().getClauseType() ==
                FromClause.ClauseType.JOIN_EXPR && selClause.getFromClause().getOnExpression() != null) {
            processor.setErrorMessage("Aggregate functions in ON clauses are not allowed");
            selClause.getFromClause().getOnExpression().traverse(processor);
        }

        if (processor.getGroupAggregates().isEmpty() && groupByExprs.isEmpty()) {
            return child;
        }

        HashedGroupAggregateNode hashNode = new HashedGroupAggregateNode(child, groupByExprs, processor.getGroupAggregates());
        return hashNode;
    }

    /**
     * Returns a node to evaluate the having portion of the select statement.
     * @param child The child of the resultant node.
     * @param selClause
     * @return The having node.
     */
    private PlanNode planHavingClause(PlanNode child, SelectClause selClause) {
        if (selClause.getHavingExpr() == null) {
            return child;
        }
        SelectNode selNode = new SimpleFilterNode(child, selClause.getHavingExpr());
        return selNode;
    }

    /**
     * Returns a projection node for the given select statement.
     * @param child The child of the resultant node.
     * @param selClause
     * @return The projection node.
     */
    private PlanNode planProjectClause(PlanNode child, SelectClause selClause) {
        if (selClause.isTrivialProject()) {
            return child;
        }
        List<SelectValue> columns = selClause.getSelectValues();
        ProjectNode projNode = new ProjectNode(child, columns);
        return projNode;
    }

    /**
     * Returns an order-by node corresponding for the given select statement.
     * @param child The child of the resultant node.
     * @param selClause
     * @return The order-by node.
     */
    private PlanNode planOrderByClause(PlanNode child, SelectClause selClause) {
        List<OrderByExpression> orderExpressions = selClause.getOrderByExprs();
        if (!orderExpressions.isEmpty()) {
            return new SortNode(child, orderExpressions);
        }
        return child;
    }
    
    private PlanNode handleJoinClause(FromClause from) throws IOException {
        PlanNode node = null;
        switch (from.getClauseType()) {
        case BASE_TABLE:
            node = makeSimpleSelect(from.getTableName(),
                    null, null);
            break;
        case JOIN_EXPR:
            node = makeJoinExpression(from);
            break;
        case SELECT_SUBQUERY:
            node = makeGeneralSelect(from.getSelectClause());
            break;
        case TABLE_FUNCTION:
            break;
        default:
            break;
        }
        if (from.getClauseType() != ClauseType.JOIN_EXPR) {
            node = new RenameNode(node, from.getResultName());
        }
        return node;
    }
    
    /*private String getFromResultName(FromClause from) {
        String ret = null;
        if (from.getClauseType() != ClauseType.JOIN_EXPR) {
            ret = from.getResultName();
        }
        return ret;
    } */

    /**
     * Returns a join node for the given from clause.
     * @param fromClause The clause to translate.
     * @return A join node for evaluating the join expression.
     * @throws IOException
     */
    private PlanNode makeJoinExpression(FromClause fromClause)  
            throws IOException {
        FromClause fromLeft = fromClause.getLeftChild();
        FromClause fromRight = fromClause.getRightChild();
        PlanNode leftNode, rightNode;

        // Extract plan nodes from the left and right joins
        leftNode = handleJoinClause(fromLeft);
        rightNode = handleJoinClause(fromRight);
        
        //String resultLeft = getFromResultName(fromLeft);
        //String resultRight = getFromResultName(fromRight);
        
        // Check for different join conditions and handle accordingly
        PlanNode ret;
        Expression onExpr;
        
        if (fromClause.getConditionType() == null) {
            return getNestedLoopsJoinNode(leftNode, rightNode, fromClause.getJoinType(), null);
        }
        
        switch (fromClause.getConditionType()) {
            case JOIN_ON_EXPR:
                // The simplest case. Create NestedLoopsJoinNode with the join
                // parameters.
                ret = getNestedLoopsJoinNode(leftNode, rightNode,
                        fromClause.getJoinType(), 
                        fromClause.getOnExpression());
                break;
            case JOIN_USING:
                List<String> usingCols = fromClause.getUsingNames();
                // Create the Join onExpression to pass into 
                // NestedLoopsJoinNode.
                /*onExpr = getColumnsEqualityExpression(
                        fromLeft.getResultName(), fromRight.getResultName(), 
                        usingCols);*/
                onExpr = getColumnsEqualityExpression(
                        fromLeft.getResultName(), fromRight.getResultName(), 
                        usingCols);
                ret = getNestedLoopsJoinNode(leftNode, rightNode,
                        fromClause.getJoinType(), onExpr);
                // Project the table to the correct schema
                ret = (PlanNode) new ProjectNode(ret, 
                        fromClause.getPreparedSelectValues());
                break;
            case NATURAL_JOIN:
                Schema leftSchema, rightSchema;
                leftSchema = fromLeft.getPreparedSchema();
                rightSchema = fromRight.getPreparedSchema();
                // Get the common columns between the left and right tables, 
                // then construct the Join onExpression by creating an 
                // expression equating these columns.
                Set<String> commonCols = leftSchema.getCommonColumnNames(rightSchema);
                // Get the on expression on which the tables should be joined
                onExpr = getColumnsEqualityExpression(
                        fromLeft.getResultName(), fromRight.getResultName(), 
                        commonCols);
                ret = new NestedLoopsJoinNode(leftNode, rightNode,
                        fromClause.getJoinType(), onExpr);
                // Project the table to the correct schema
                ret = (PlanNode) new ProjectNode(ret, 
                        fromClause.getPreparedSelectValues());
                break;
            default:
                ret = null;
                break;
        }
        return ret;
    }
    
    private PlanNode getNestedLoopsJoinNode(PlanNode leftNode, 
            PlanNode rightNode, JoinType joinType, Expression onExpr) {
        PlanNode ret;
        if (joinType == JoinType.RIGHT_OUTER) {
            // Right outer is not implemented in the NestedLoopsJoinNode, so
            // just do a left outer and call swap() to swap the left and right
            // children.
            ret = new NestedLoopsJoinNode(leftNode, rightNode,
                    JoinType.LEFT_OUTER, onExpr);
            NestedLoopsJoinNode joinNode = (NestedLoopsJoinNode) ret;
            joinNode.swap();
        }
        else {
            ret = new NestedLoopsJoinNode(leftNode, rightNode,
                    joinType, onExpr);
        }
        return ret;
    }
    
    // Construct an expression that equates columns in COL that appear in both
    // LEFTTABLE and RIGHTTABLE.
    private Expression getColumnsEqualityExpression(String leftTable, String rightTable,
            Collection<String> cols) {
        Collection<Expression> compareOperators = new HashSet<Expression>();
        for (String col : cols) {
            ColumnName colNameLeft = new ColumnName(leftTable, col);
            ColumnName colNameRight = new ColumnName(rightTable, col);
            ColumnValue colValLeft = new ColumnValue(colNameLeft);
            ColumnValue colValRight = new ColumnValue(colNameRight);
            compareOperators.add(new CompareOperator(CompareOperator.Type.EQUALS, colValLeft, colValRight));
        }
        return new BooleanOperator(BooleanOperator.Type.AND_EXPR, compareOperators); 
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
                                       List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                    "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.initialize();
        return selectNode;
    }
}
