package edu.caltech.nanodb.qeval;


import java.util.ArrayList;
import java.util.HashSet;

import edu.caltech.nanodb.expressions.*;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;

import org.apache.log4j.Logger;


/**
 * This utility class is used to estimate the selectivity of predicates that
 * appear on Select and Theta-Join plan-nodes.
 */
public class SelectivityEstimator {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(SelectivityEstimator.class);



    /**
     * This collection specifies the data-types that support comparison
     * selectivity estimates (not including equals or not-equals).  It must be
     * possible to use the {@link #computeRatio} on the data-type so that an
     * estimate can be made about where a value fits within the minimum and
     * maximum values for the column.
     * <p>
     * Note that we can compute selectivity for equals and not-equals simply
     * from the number of distinct values in the column.
     */
    private static HashSet<SQLDataType> SUPPORTED_TYPES_COMPARE_ESTIMATES;


    static {
        // Initialize the set of types that support comparison selectivity
        // estimates.  In time, types like dates, times, NUMERIC, etc. could be
        // added as well.

        SUPPORTED_TYPES_COMPARE_ESTIMATES = new HashSet<SQLDataType>();

        SUPPORTED_TYPES_COMPARE_ESTIMATES.add(SQLDataType.INTEGER);
        SUPPORTED_TYPES_COMPARE_ESTIMATES.add(SQLDataType.BIGINT);
        SUPPORTED_TYPES_COMPARE_ESTIMATES.add(SQLDataType.SMALLINT);
        SUPPORTED_TYPES_COMPARE_ESTIMATES.add(SQLDataType.TINYINT);
        SUPPORTED_TYPES_COMPARE_ESTIMATES.add(SQLDataType.FLOAT);
        SUPPORTED_TYPES_COMPARE_ESTIMATES.add(SQLDataType.DOUBLE);
    }


    /**
     * This constant specifies the default selectivity assumed when a select
     * predicate is too complicated to compute more accurate estimates.  We are
     * assuming that generally people are going to do things that limit the
     * results produced.
     */
    public static final float DEFAULT_SELECTIVITY = 0.25f;


    /** This class should not be instantiated. */
    private SelectivityEstimator() {
        throw new IllegalArgumentException("This class should not be instantiated.");
    }


    /**
     * Returns true if the database supports selectivity estimates for
     * comparisons (other than equals and not-equals) on the specified SQL data
     * type.  SQL types that support these selectivity estimates will include
     * minimum and maximum values in their column-statistics.
     *
     * @param type the SQL data type being considered
     *
     * @return true if the database supports selectivity estimates for the type
     */
    public static boolean typeSupportsCompareEstimates(SQLDataType type) {
        return SUPPORTED_TYPES_COMPARE_ESTIMATES.contains(type);
    }


    /**
     * This function computes the selectivity of a selection predicate, using
     * table statistics and other estimates to make an educated guess.  The
     * result is between 0.0 and 1.0, with 1.0 meaning that all rows will be
     * selected by the predicate.
     *
     * @param expr the expression whose selectivity we are estimating
     *
     * @param exprSchema a schema describing the environment that the expression
     *        will be evaluated within
     *
     * @param stats statistics that may be helpful in estimating the selectivity
     *
     * @return the estimated selectivity as a float
     */
    public static float estimateSelectivity(Expression expr, Schema exprSchema,
                                            ArrayList<ColumnStats> stats) {
        float selectivity = DEFAULT_SELECTIVITY;

        if (expr instanceof BooleanOperator) {
            // A Boolean AND, OR, or NOT operation.
            BooleanOperator bool = (BooleanOperator) expr;
            selectivity = estimateBoolOperSelectivity(bool, exprSchema, stats);
        }
        else if (expr instanceof CompareOperator) {
            // This is a simple comparison between expressions.
            CompareOperator comp = (CompareOperator) expr;
            selectivity = estimateCompareSelectivity(comp, exprSchema, stats);
        }

        return selectivity;
    }


    /**
     * This function computes a selectivity estimate for a general Boolean
     * expression that may be comprised of one or more components.  The method
     * treats components as independent, estimating the selectivity of each one
     * separately, and then combines the results based on whether the Boolean
     * operation is an <tt>AND</tt>, an <tt>OR</tt>, or a <tt>NOT</tt>
     * operation.  As one might expect, this method delegates to
     * {@link #estimateSelectivity} to compute the selectivity of individual
     * terms.
     *
     * @param bool the compound Boolean expression
     *
     * @param exprSchema a schema specifying the environment that the expression
     *        will be evaluated within
     *
     * @param stats a collection of column-statistics to use in making
     *        selectivity estimates
     *
     * @return a selectivity estimate in the range [0, 1].
     */
    public static float estimateBoolOperSelectivity(BooleanOperator bool,
        Schema exprSchema, ArrayList<ColumnStats> stats) {

        float selectivity = 1.0f;

        // Construct an arraylist of expressions
        ArrayList<Expression> expressions = new ArrayList<Expression>();
        for (int i = 0; i < bool.getNumTerms(); i++) {
            expressions.add(bool.getTerm(i));
        }

        // This would be perfect for some functional-programming (map/foldr). Maybe some other time

        // The result will be accumulated here
        float result = 0.0f;

        switch (bool.getType()) {
        case AND_EXPR:
            for (Expression e: expressions) {
                result *= estimateSelectivity(e, exprSchema, stats);
            }
            selectivity = result;
            break;

        case OR_EXPR:
            for (Expression e: expressions) {
                result *= 1.0f - estimateSelectivity(e, exprSchema, stats);
            }
            selectivity = 1.0f - result;
            break;

        case NOT_EXPR:
            // Assuming that we only have one expression (otherwise NOT does't
            // make much sense
            selectivity = 1.0f - estimateSelectivity(expressions.get(0), exprSchema, stats);
            break;

        default:
            // Shouldn't have any other Boolean expression types.
            assert false : "Unexpected Boolean operator type:  " + bool.getType();
        }

        logger.debug("Estimated selectivity of Boolean operator \"" + bool +
            "\" as " + selectivity);

        return selectivity;
    }


    /**
     * This function computes a selectivity estimate for a general comparison
     * operation.  The method examines the types of the arguments in the
     * comparison and determines if it will be possible to make a reasonable
     * guess as to the comparison's selectivity; if not then a default
     * selectivity estimate is used.
     *
     * @param comp the comparison expression
     *
     * @param exprSchema a schema specifying the environment that the expression
     *        will be evaluated within
     *
     * @param stats a collection of column-statistics to use in making
     *        selectivity estimates
     *
     * @return a selectivity estimate in the range [0, 1].
     */
    public static float estimateCompareSelectivity(CompareOperator comp,
        Schema exprSchema, ArrayList<ColumnStats> stats) {

        float selectivity = DEFAULT_SELECTIVITY;

        // Move the comparison into a normalized order so that it's easier to
        // write the logic for analysis.  Specifically, this will ensure that
        // if we are comparing a column and a value, the column will always be
        // on the left and the value will always be on the right.
        comp.normalize();

        Expression left = comp.getLeftExpression();
        Expression right = comp.getRightExpression();

        // If the comparison is simple enough then compute its selectivity.
        // Otherwise, just use the default selectivity.
        if (left instanceof ColumnValue && right instanceof LiteralValue) {
            // Comparison:  column op value
            selectivity = estimateCompareColumnValue(comp.getType(),
                (ColumnValue) left, (LiteralValue) right, exprSchema, stats);

            logger.debug("Estimated selectivity of cmp-col-val operator \"" +
                comp + "\" as " + selectivity);
        }
        else if (left instanceof ColumnValue && right instanceof ColumnValue) {
            // Comparison:  column op column
            selectivity = estimateCompareColumnColumn(comp.getType(),
                (ColumnValue) left, (ColumnValue) right, exprSchema, stats);

            logger.debug("Estimated selectivity of cmp-col-col operator \"" +
                comp + "\" as " + selectivity);
        }

        return selectivity;
    }


    /**
     * This helper function computes a selectivity estimate for a comparison
     * between a column and a literal value.  Note that the comparison is always
     * assumed to have the column-name on the <em>left</em>, and the literal
     * value on the <em>right</em>.  Examples would be <tt>T1.A &gt; 5</tt>, or
     * <tt>T2.C = 15</tt>.
     *
     * @param compType the type of the comparison, e.g. equals, not-equals, or
     *        some inequality comparison
     *
     * @param columnValue the column that is used in the comparison
     * @param literalValue the value that the column is being compared to
     *
     * @param exprSchema a schema specifying the environment that the expression
     *        will be evaluated within
     *
     * @param stats a collection of column-statistics to use in making
     *        selectivity estimates
     *
     * @return a selectivity estimate in the range [0, 1].
     */
    private static float estimateCompareColumnValue(CompareOperator.Type compType,
        ColumnValue columnValue, LiteralValue literalValue,
        Schema exprSchema, ArrayList<ColumnStats> stats) {

        // Comparison:  column op value

        float selectivity = DEFAULT_SELECTIVITY;

        // Pull out the critical values for making the estimates.

        int colIndex = exprSchema.getColumnIndex(columnValue.getColumnName());
        ColumnInfo colInfo = exprSchema.getColumnInfo(colIndex);
        SQLDataType sqlType = colInfo.getType().getBaseType();
        ColumnStats colStats = stats.get(colIndex);


        Object value = literalValue.evaluate();

        // TODO: Check to see if we've run analyze
        Object minObj = colStats.getMinValue();
        Object maxObj = colStats.getMaxValue();

        float valueFlt;
        float maxFlt;
        float minFlt;
        try {
             valueFlt = TypeConverter.getFloatValue(value);
             maxFlt = TypeConverter.getFloatValue(maxObj);
             minFlt = TypeConverter.getFloatValue(minObj);
        }
        catch (TypeCastException e) {
            logger.warn("Wasn't able to typecast!!");
            // This could happen if the types weren't able to convert to float. As we aren't handling this case
            // just log a debug note and set to 0
             valueFlt = 0.0f;
             maxFlt = 0.0f;
             minFlt = 0.0f;
        }

        int numUniquevalues = colStats.getNumUniqueValues();

        switch (compType) {
        case EQUALS:
        case NOT_EQUALS:
            // Compute the equality value.  Then, if inequality, invert the
            // result.
            float selEquals;

            // Compute the selectivity if we have an equals statement
            // NOTE: Even if we have no estimate for the number of values, we could probably do a little better than
            // default, by assuming approximate-integrity of other stats. For simplicity, the default_selectivity is
            // being returned for now
            if (numUniquevalues == -1) {
                break;
            }

            // If it's out of the known bounds, set the selectivity to 0
            if (valueFlt < minFlt || valueFlt > maxFlt) {
                selEquals = 0.0f;
            }
            else {
                selEquals = 1.0f / numUniquevalues;
            }

            // Using the selEquals, compute the actual selectivity, by checking if it's an equals or a not-equals
            // statement
            if (compType == CompareOperator.Type.EQUALS) {
                selectivity = selEquals;
            }
            else {
                selectivity = 1.0f - selEquals;
            }
            break;

        case GREATER_OR_EQUAL:
        case LESS_THAN:
            // Compute the greater-or-equal value.  Then, if less-than,
            // invert the result.

            // Only estimate selectivity for this kind of expression if the
            // column's type supports it.

            if (typeSupportsCompareEstimates(sqlType) &&
                colStats.hasDifferentMinMaxValues()) {

                // NOTE: Not caring about number of unique values

                // The selectivity for the greater than or equal case
                float selectivityGOE;

                // Handle out of bounds cases
                if (valueFlt < minFlt) {
                    selectivityGOE = 1.0f;
                }
                else if (valueFlt > maxFlt) {
                    selectivityGOE = 0.0f;
                }
                // if we're in between min and max
                else {
                    // EDGE CASE
                    // If min and max are equal, and if we're "between" these range (i.e. equal), then the selectivity
                    // should theoretically be 1
                    if (minFlt != maxFlt) {
                        selectivityGOE = 1.0f;
                    }
                    else {
                        // selectivityGOE = (maxFlt - valueFlt) / (maxFlt - minFlt);
                        selectivityGOE = computeRatio(value, maxObj, minObj, maxObj);
                    }
                }
                // Now we can compute the final selectivity value
                if (compType == CompareOperator.Type.GREATER_OR_EQUAL) {
                    selectivity = selectivityGOE;
                }
                else {
                    selectivity = 1.0f - selectivityGOE;
                }
            }

            break;

        case LESS_OR_EQUAL:
        case GREATER_THAN:
            // Compute the less-or-equal value.  Then, if greater-than,
            // invert the result.

            // Only estimate selectivity for this kind of expression if the
            // column's type supports it.
            logger.debug("LOE-GT");

            if (typeSupportsCompareEstimates(sqlType) &&
                colStats.hasDifferentMinMaxValues()) {

                // NOTE: Not caring about number of unique values

                // Selectivity for the less than or equal case
                float selectivityLOE;


                // Handle out of bounds cases
                if (valueFlt < minFlt) {
                    logger.debug("value less than min");
                    selectivityLOE = 0.0f;
                }
                else if (valueFlt > maxFlt) {
                    logger.debug("value more than max");
                    selectivityLOE = 1.0f;
                }
                // if we're in between min and max
                else {
                    // EDGE CASE
                    // If min and max are equal, and if we're "between" these range (i.e. equal), then the selectivity
                    // should theoretically be 1
                    if (minFlt == maxFlt) {
                        logger.debug("min and max are equal!");
                        selectivityLOE = 1.0f;
                    }
                    else {
                        logger.debug("Doing ratio");
                        // selectivityLOE = (valueFlt - minFlt) / (maxFlt - minFlt);
                        selectivityLOE = computeRatio(minObj, value, minObj, maxObj);
                    }

                }

                // Now we can compute the final selectivity value
                if (compType == CompareOperator.Type.LESS_OR_EQUAL) {
                    selectivity = selectivityLOE;
                }
                else {
                    selectivity = 1.0f - selectivityLOE;
                }
            }

            break;

        default:
            // Shouldn't be any other comparison types...
            assert false : "Unexpected compare-operator type:  " + compType;
        }

        return selectivity;
    }


    /**
     * This helper function computes a selectivity estimate for a comparison
     * between two columns.  Examples would be <tt>T1.A = T2.A</tt>.
     *
     * @param compType the type of the comparison, e.g. equals, not-equals, or
     *        some inequality comparison
     *
     * @param columnOne the first column that is used in the comparison
     * @param columnTwo the second column that is used in the comparison
     *
     * @param exprSchema a schema specifying the environment that the expression
     *        will be evaluated within
     *
     * @param stats a collection of column-statistics to use in making
     *        selectivity estimates
     *
     * @return a selectivity estimate in the range [0, 1].
     */
    private static float estimateCompareColumnColumn(CompareOperator.Type compType,
        ColumnValue columnOne, ColumnValue columnTwo,
        Schema exprSchema, ArrayList<ColumnStats> stats) {

        // Comparison:  column op column

        float selectivity = DEFAULT_SELECTIVITY;

        // Pull out the critical values for making the estimates.

        int colOneIndex = exprSchema.getColumnIndex(columnOne.getColumnName());
        int colTwoIndex = exprSchema.getColumnIndex(columnTwo.getColumnName());

        ColumnStats colOneStats = stats.get(colOneIndex);
        ColumnStats colTwoStats = stats.get(colTwoIndex);

        int colOneNumRows = colOneStats.getNumUniqueValues();
        int colTwoNumRows = colTwoStats.getNumUniqueValues();

        // If we don't have the number of rows for any column, fallback to default
        if (colOneNumRows == -1 || colTwoNumRows == -1) {
            return DEFAULT_SELECTIVITY;
        }

        switch (compType) {
            case EQUALS:
            case NOT_EQUALS:
                if (Math.max(colOneNumRows, colTwoNumRows) == 0) {
                    // avoid division by 0
                    return 0.0f;
                }
                float selEquals = 1 / Math.max(colOneNumRows, colTwoNumRows);

                if (compType == CompareOperator.Type.EQUALS) {
                    selectivity = selEquals;
                }
                else {
                    selectivity = 1 - selEquals;
                }
                break;
        }

        // TODO: Implement estimates for other types of comparison types


        return selectivity;
    }


    /**
     * This method computes the function
     * (<em>high</em><sub>1</sub> - <em>low</em><sub>1</sub>) /
     * (<em>high</em><sub>2</sub> - <em>low</em><sub>2</sub>), given
     * <tt>Object</tt>-values that can be coerced into types that can
     * be used for arithmetic.  This operation is useful for estimating the
     * selectivity of comparison operations, if we know the minimum and maximum
     * values for a column.
     * <p>
     * The result of this operation is clamped to the range [0, 1].
     *
     * @param low1 the low value for the numerator
     * @param high1 the high value for the numerator
     * @param low2 the low value for the denominator
     * @param high2 the high value for the denominator
     *
     * @return the ratio of (<em>high</em><sub>1</sub> - <em>low</em><sub>1</sub>) /
     *         (<em>high</em><sub>2</sub> - <em>low</em><sub>2</sub>), clamped
     *         to the range [0, 1].
     */
    private static float computeRatio(Object low1, Object high1,
                                      Object low2, Object high2) {

        Object diff1 = ArithmeticOperator.evalObjects(
            ArithmeticOperator.Type.SUBTRACT, high1, low1);

        Object diff2 = ArithmeticOperator.evalObjects(
            ArithmeticOperator.Type.SUBTRACT, high2, low2);

        Object ratio = ArithmeticOperator.evalObjects(
            ArithmeticOperator.Type.DIVIDE, diff1, diff2);

        float fltRatio = TypeConverter.getFloatValue(ratio);

        logger.debug(String.format("Ratio:  (%s - %s) / (%s - %s) = %.2f",
            high1, low1, high2, low2, fltRatio));

        // Clamp the value to the range [0, 1].
        if (fltRatio < 0.0f)
            fltRatio = 0.0f;
        else if (fltRatio > 1.0f)
            fltRatio = 1.0f;

        return fltRatio;
    }
}
