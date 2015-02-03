package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

import java.util.HashMap;
import java.util.Map;

/**
 * This processor converts aggregate functions into column references. It also signals errors when
 * it encounters nested loops, and whenever it encounters aggregate functions in places there should be none.
 */
public class AggregateReplacementProcessor implements ExpressionProcessor {
    private Map<String, FunctionCall> groupAggregates = new HashMap<String, FunctionCall>();
    private boolean parentIsAggregate = false;
    private String errorMessage = null;

    public Map<String, FunctionCall> getGroupAggregates() {
        return groupAggregates;
    }

    /**
     * This function sets the error message. If errorMessage is non-null, and the processor encounters
     * an aggregate function, then an exception will be raised with the given message.
     * @param errorMessage
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public void enter(Expression e) {
        if (e instanceof FunctionCall) {
            Function f = ((FunctionCall)e).getFunction();
            if (f instanceof AggregateFunction) {
                if (errorMessage != null) {
                    throw new IllegalArgumentException(errorMessage);
                }
                if (parentIsAggregate) {
                    throw new IllegalArgumentException("Nested aggregate functions are not allowed!");
                }
                parentIsAggregate = true;
            }
        }
    }

    @Override
    public Expression leave(Expression e) {
        if (e instanceof FunctionCall) {
            Function f = ((FunctionCall)e).getFunction();
            if (f instanceof AggregateFunction) {
                parentIsAggregate = false;
                AggregateFunction af = (AggregateFunction)f;
                String key = "#" + e.toString();
                if (!groupAggregates.containsKey(key)) {
                    groupAggregates.put(key, (FunctionCall)e);
                }
                ColumnValue res = new ColumnValue(new ColumnName(key));
                return res;
            }
        }
        return e;
    }
}
