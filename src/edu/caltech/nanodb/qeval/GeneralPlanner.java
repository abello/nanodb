package edu.caltech.nanodb.qeval;
import java.io.IOException;
import java.util.*;

import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.plans.*;
import org.apache.log4j.Logger;
import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.expressions.Expression;
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
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
                             List<SelectClause> enclosingSelects) throws IOException {
        System.out.println("makePlan called!");
        // TODO:  Implement!

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not yet implemented:  enclosing queries!");
        }

        // TODO: get logger.debug working.
        System.out.println(selClause);
        PlanNode res = makeGeneralSelect(selClause);
        res.prepare();
        return res;
    }

    private PlanNode planFromClause(SelectClause selClause) throws IOException {
        FromClause fromClause = selClause.getFromClause();
        PlanNode planNode = null;
        if (fromClause.isBaseTable()) {
            planNode = makeSimpleSelect(fromClause.getTableName(),
                    null, null);
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

    private PlanNode makeGeneralSelect(SelectClause selClause) throws IOException {
        PlanNode res = planFromClause(selClause);
        res = planWhereClause(res, selClause);
        res = planGroupingAggregation(res, selClause);
        res = planHavingClause(res, selClause);
        res = planProjectClause(res, selClause);
        res = planOrderByClause(res, selClause);
        return res;
    }

    private PlanNode planWhereClause(PlanNode child, SelectClause selClause) throws IOException {
        if (selClause.getWhereExpr() == null) {
            return child;
        }
        SelectNode selNode = new SimpleFilterNode(child, selClause.getWhereExpr());
        return selNode;
    }

    private PlanNode planGroupingAggregation(PlanNode child, SelectClause selClause) {
        /*
        List<Expression> groupByExprs = selClause.getGroupByExprs();
        Map<String, FunctionCall> groupAggregates = new HashMap<String, FunctionCall>();

        System.out.println(groupByExprs);
        for (SelectValue sv: selClause.getSelectValues()) {
            if (!sv.isExpression())
                continue;
            Expression e = sv.getExpression();
//            System.out.println(e);
            if (e instanceof FunctionCall) {
//                System.out.println(e);
                Function f = ((FunctionCall)e).getFunction();
                System.out.println(f);
                if (f instanceof AggregateFunction) {
                    AggregateFunction af = (AggregateFunction)f;
                    groupAggregates.put(e.toString(), (FunctionCall)e);
                    //System.out.println("yodl");
                }
            }
        }

        HashedGroupAggregateNode hashNode = new HashedGroupAggregateNode(planNode, groupByExprs, groupAggregates);
        hashNode.initialize();
        planNode = (PlanNode) hashNode;
        return planNode;*/
        return child;
    }

    private PlanNode planHavingClause(PlanNode child, SelectClause selClause) {
        if (selClause.getHavingExpr() == null) {
            return child;
        }
        SelectNode selNode = new SimpleFilterNode(child, selClause.getHavingExpr());
        return selNode;
    }

    private PlanNode planProjectClause(PlanNode child, SelectClause selClause) {
        List<SelectValue> columns = selClause.getSelectValues();
        // TODO: check to see if we have a trivial projection
        ProjectNode projNode = new ProjectNode(child, columns);
        return projNode;
    }

    private PlanNode planOrderByClause(PlanNode child, SelectClause selClause) {
        List<OrderByExpression> orderExpressions = selClause.getOrderByExprs();
        if (!orderExpressions.isEmpty()) {
            return new SortNode(child, orderExpressions);
        }
        return child;
    }

    private PlanNode makeJoinExpression(FromClause joinClause) throws IOException {
// TODO: Handle for more complicated joins
        FromClause fromLeft = joinClause.getLeftChild();
        FromClause fromRight = joinClause.getRightChild();
        PlanNode leftNode, rightNode;
        if (fromLeft.isBaseTable()) {
            leftNode = makeSimpleSelect(fromLeft.getTableName(),
                    null, null);
        }
        else {
            leftNode = makeGeneralSelect(fromLeft.getSelectClause());
        }
        if (fromRight.isBaseTable()) {
            rightNode = makeSimpleSelect(fromRight.getTableName(),
                    null, null);
        }
        else {
            rightNode = makeGeneralSelect(fromRight.getSelectClause());
        }
        NestedLoopsJoinNode ret = new NestedLoopsJoinNode(leftNode, rightNode,
                joinClause.getJoinType(), joinClause.getOnExpression());
        ret.prepare();
        return ret;
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
        System.out.println("makeSimpleSelect called!");
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
