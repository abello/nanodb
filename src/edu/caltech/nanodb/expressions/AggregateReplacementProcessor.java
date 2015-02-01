package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

import java.util.HashMap;
import java.util.Map;

public class AggregateReplacementProcessor implements ExpressionProcessor {
    private Map<String, FunctionCall> groupAggregates = new HashMap<String, FunctionCall>();

    public Map<String, FunctionCall> getGroupAggregates() {
        return groupAggregates;
    }

    @Override
    public void enter(Expression e) { }

    @Override
    public Expression leave(Expression e) {
        if (e instanceof FunctionCall) {
            Function f = ((FunctionCall)e).getFunction();
            if (f instanceof AggregateFunction) {
                AggregateFunction af = (AggregateFunction)f;
                groupAggregates.put("#" + e.toString(), (FunctionCall)e);
                ColumnValue res = new ColumnValue(new ColumnName("#" + e.toString()));
                return res;
            }
        }
        return e;
    }
}
