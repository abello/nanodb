package edu.caltech.nanodb.qeval;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SortNode;
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
        PlanNode res = makeGeneralSelect(selClause, enclosingSelects);
        res.prepare();
        return res;
    }

    private PlanNode makeGeneralSelect(SelectClause selClause,
                                       List<SelectClause> enclosingSelects)
            throws IOException {
        FromClause fromClause = selClause.getFromClause();
        PlanNode planNode = null;
        if (fromClause != null) {
        	if (fromClause.isBaseTable()) {
            	planNode = makeSimpleSelect(fromClause.getTableName(),
            			selClause.getWhereExpr(), null);
        	} 
        	else if (fromClause.isJoinExpr()){
        		planNode = makeJoinExpression(fromClause);
        	}
        	else {
        		planNode = makeGeneralSelect(fromClause.getSelectClause(), null);
            	planNode = new RenameNode(planNode, fromClause.getResultName());
        	}
        }
        
        System.out.println(planNode);

        List<SelectValue> columns = selClause.getSelectValues();
        // TODO: check to see if we have a trivial projection
        ProjectNode projNode = new ProjectNode(planNode, columns);
        planNode = projNode;

        List<OrderByExpression> orderExpressions = selClause.getOrderByExprs();
        if (!orderExpressions.isEmpty()) {
            planNode = (PlanNode) new SortNode((PlanNode)projNode, orderExpressions);
        }

        planNode.prepare();
        return planNode;
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
			leftNode = makeGeneralSelect(fromLeft.getSelectClause(), null);
		}
		
		if (fromRight.isBaseTable()) {
			rightNode = makeSimpleSelect(fromRight.getTableName(), 
					null, null);
		}
		else {
			rightNode = makeGeneralSelect(fromRight.getSelectClause(), null);
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
        selectNode.prepare();
        return selectNode;
    }
}
