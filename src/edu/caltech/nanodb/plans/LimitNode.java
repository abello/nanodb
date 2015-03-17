package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.Tuple;


/**
 * PlanNode representing the <tt>WHERE</tt> clause in a <tt>SELECT</tt>
 * operation.  This is the relational algebra Select operator.
 */
public class LimitNode extends PlanNode {

    /** The number of tuples to output. */
    public int limit;


    /** The current tuple that the node is selecting. */
    protected Tuple currentTuple;


    /** True if we have finished scanning or pulling tuples from children. */
    private boolean done;
    
    /** Number of tuples we've passed. Initialized to zero. */
    private int tuplesTraversed;
    
    private PlanNode child;


    /**
     * Constructs a LimitNode that limits the number of tuples in output.
     *
     * @param predicate the selection criterion
     */
    protected LimitNode(PlanNode child, int limit) {
        super(OperationType.LIMIT, child);
        this.child = child;
        this.limit = limit;
    }


    /**
     * Creates a copy of this select node and its subtree.  This method is used
     * by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        LimitNode node = (LimitNode) super.clone();

        // Copy the limit
        node.limit = limit;
        return node;
    }


    /** Do initialization for the select operation. Resets state variables. */
    @Override
    public void initialize() {
        super.initialize();

        done = false;
        currentTuple = null;
        tuplesTraversed = 0;
    }


    /**
     * Gets the next tuple selected by the predicate.
     *
     * @return the tuple to be passed up to the next node.
     *
     * @throws java.lang.IllegalStateException if this is a scanning node
     *         with no algorithm or a filtering node with no child, or if
     *         the leftChild threw an IllegalStateException.
     *
     * @throws java.io.IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IllegalStateException, IOException {

        // If this node is finished finding tuples, return null until it is
        // re-initialized.
        if (done)
            return null;

        // Continue to advance the current tuple until it is selected by the
        // predicate.
        do {
        	Tuple oldTuple = currentTuple;
            advanceCurrentTuple();
            tuplesTraversed++;
            
            // Previous tuple is no longer needed, unpin it.
            if (oldTuple != null)
            	oldTuple.unpin();

            // If the last tuple in the file (or chain of nodes) did not satisfy the
            // predicate, then the selection process is over, so set the done flag and
            // return null.
            if (currentTuple == null) {
                done = true;
                return null;
            }
        }
        while (!isLimitReached());

        // The current tuple now satisfies the predicate, so return it.
        return currentTuple;
    }
    
    protected boolean isLimitReached() {
    	return tuplesTraversed == limit;
    }


	@Override
	public List<OrderByExpression> resultsOrderedBy() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean supportsMarking() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean requiresLeftMarking() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean requiresRightMarking() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void markCurrentPosition() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void resetToLastMark() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

}
