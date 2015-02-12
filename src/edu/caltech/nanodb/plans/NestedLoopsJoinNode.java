package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.List;
import java.util.Set;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.qeval.PlanCost;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This plan node implements a nested-loops join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopsJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SortMergeJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    /** Checking whether the outer loop had a matching row. Pretty gross to keep
     * track of state like this, but not sure if there a simple prettier way */
    private boolean matchedRow;

    /** If this flag is set to true, padd the outer column with nulls, indicating that we're inserting it
     * without a matching row from the other table */
    private boolean padNull;

    /** Flag to break inner loop */
    private boolean breakInner;

    /** Flag to indicate whether this row has no matching rows in the other table */
    private boolean unMatchedRow;

    public NestedLoopsJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopsJoinNode) {
            NestedLoopsJoinNode other = (NestedLoopsJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loops plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoops[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopsJoinNode node = (NestedLoopsJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();


        float tupleSize = leftChild.getCost().tupleSize + rightChild.getCost().tupleSize;
        Set<String> commonCols = leftSchema.getCommonColumnNames(rightSchema);
        long denominator = 1;
        for (String col : commonCols) {
            int leftIdx = leftSchema.getColumnIndex(col);
            int rightIdx = rightSchema.getColumnIndex(col);
            int leftDistinct = leftChild.getStats().get(leftIdx).getNumUniqueValues();
            int rightDistinct = leftChild.getStats().get(rightIdx).getNumUniqueValues();
            denominator *= Math.max(leftDistinct, rightDistinct);
        }
        float numTuples = rightChild.getCost().numTuples * leftChild.getCost().numTuples / denominator;

        switch (super.joinType) {
            case INNER:
                break;
            case LEFT_OUTER:
                numTuples += rightChild.getCost().numTuples;
                break;
            case RIGHT_OUTER:
                numTuples += leftChild.getCost().numTuples;
                break;
            case FULL_OUTER:
                numTuples += leftChild.getCost().numTuples + rightChild.getCost().numTuples;
                break;
        }
        float cpuCost = rightChild.getCost().cpuCost * leftChild.getCost().numTuples +
                leftChild.getCost().cpuCost + numTuples;
        // We assume that we can store both tables in memory simultaneously.
        long numBlockIOs = leftChild.getCost().numBlockIOs + rightChild.getCost().numBlockIOs;
        cost = new PlanCost(numTuples, tupleSize, cpuCost, numBlockIOs);
    }


    public void initialize() {
        super.initialize();

        done = false;
        leftTuple = null;
        rightTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;

        while (getTuplesToJoin()) {
            switch (super.joinType) {
                case CROSS:
                    if (canJoinTuples()) {
                        Tuple result = joinTuples(leftTuple, rightTuple);
                        return result;
                    }
                    break;

                case INNER:
                    if (canJoinTuples()) {
                        Tuple result = joinTuples(leftTuple, rightTuple);
                        return result;
                    }
                    break;

                case LEFT_OUTER:
                    if (padNull == true) {
                        logger.debug("Padding null");
                        TupleLiteral rightTupleNulls = new TupleLiteral(rightChild.getSchema().getColumnInfos().size());
                        logger.debug("Created new tuple literal with null columns");

                        Tuple result = joinTuples(leftTuple, rightTupleNulls);
                        return result;
                    }
                    else if (canJoinTuples()) {
                        matchedRow = true;
                        Tuple result = joinTuples(leftTuple, rightTuple);
                        logger.debug(leftTuple);
                        return result;
                    }
                    break;

                case SEMIJOIN:
                    if (canJoinTuples()) {
                        logger.debug(leftTuple);
                        breakInner = true;
                        return leftTuple;
                    }
                    break;
                case ANTIJOIN:
                    if (unMatchedRow) {
                        unMatchedRow = false;
                        return leftTuple;
                    }
                    else if (canJoinTuples()) {
                        matchedRow = true;
                        logger.debug(leftTuple);
                        breakInner = true;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("This type of join is not yet supported!");
            }
        }

        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loops logic.
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {
        PlanNode rightChild = super.rightChild;
        PlanNode leftChild = super.leftChild;
        // logger.debug("getUplesToJoin() called!");

        // If we're done, return here, no need to do more stuff
        if (done) {
            logger.debug("getTuplesToJoin DONE, but still called");
            return false;
        }

        // If both iterators are null and we're not done, then we're in the very first iteration
        if (rightTuple == null && leftTuple == null) {
            logger.debug("Just started loop iterators");
            leftTuple = leftChild.getNextTuple();
            matchedRow = false;
            padNull = false;
            breakInner = false;
            unMatchedRow = false;

            // If the left tuple is null, we're done
            if (leftTuple == null) {
                done = true;
                return false;
            }
        }

        // If we're here, we know that we're in the middle of an iteration

        // Move the next inner loop (unless we're coming from a padNull round, in which case we shouldn't try
        // to advance the right tuple; NOTE: It shouldn't matter if we advance it, we should still get null. However
        // it's probably not a good practice to call getNextTuple() and get null twice in a row)
        Tuple nextRightTuple;
        if (padNull == false) {
            nextRightTuple = rightChild.getNextTuple();
        }
        else {
            nextRightTuple = null;
        }

        // If we're told to break, update rightTuple and return true iff it's non-null
        if (breakInner  == true) {
            rightTuple = nextRightTuple;
            breakInner = false;
            return (nextRightTuple != null);
        }

        // If the inner iterator is exhausted, just reset it back to the beginning and
        // move the outer iterator by 1
        if (nextRightTuple == null) {
            logger.debug("nextRightTuple is null");
            if (joinType == JoinType.LEFT_OUTER && padNull == false) {
                // If we don't have a matched row at this point, we need to add a null-padded row
                if (matchedRow == false) {
                    padNull = true;
                    return true;
                }
            }

            // If we're doing an ANTIJOIN and we've run out of right tuples AND there was no previous match
            // this is an unmatched row
            else if (joinType == JoinType.ANTIJOIN && matchedRow == false) {
                unMatchedRow = true;
                return true;
            }

            // Advance outer loop
            leftTuple = leftChild.getNextTuple();

            // If the outer loop can't advance, we're done.
            if (leftTuple == null) {
                logger.debug("getTuplesToJoin DONE");
                done = true;
                return false;
            }

            // Reset matchedRow for new row in outer loop
            matchedRow = false;

            // At this point we've advanced the outer tuple. We advance the inner one too, to keep looping.

            rightChild.initialize();
            rightTuple = rightChild.getNextTuple();

            // If right relation is empty
            if (rightTuple == null) {

                // For the following joins, if the right relation is empty, we're done
                if (joinType == JoinType.INNER || joinType == JoinType.CROSS || joinType == JoinType.SEMIJOIN) {
                    done = true;
                    return false;
                }
                padNull = true;
            }
            else {
                padNull = false;
            }

            return true;
        }


        // At this point, we know that none of the iterators are exhausted. Just update the outer tuple
        rightTuple = nextRightTuple;
        return true;
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
