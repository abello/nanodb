package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

import java.util.HashMap;
import java.util.Map;

public class AggregateReplacementProcessor implements ExpressionProcessor {
    private Map<String, FunctionCall> groupAggregates = new HashMap<String, FunctionCall>();
    private boolean parentIsAggregate = false;
    private String errorMessage = null;

    public Map<String, FunctionCall> getGroupAggregates() {
        return groupAggregates;
    }

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
