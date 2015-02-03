package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class tests some simple inner and outer joins.
 */
@Test
public class TestNaturalJoins extends SqlTestCase {

    public TestNaturalJoins() {
        super("setup_testNaturalJoins");
    }


    /**
     * ************************************************************
     * ********************* INNER JOIN TESTS *********************
     * ************************************************************
     */

    /**
     * This test performs a simple inner join between two non-empty tables. Not every column
     * should join.
     */
    public void testNaturalSimple() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(1, 10, 100),
            new TupleLiteral(2, 20, 200),
            new TupleLiteral(3, 30, 300),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_natural_1 NATURAL JOIN test_natural_2", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs an inner join between a empty table (right) and a non-empty table.
     * @throws Throwable
     */
    public void testInnerNaturalEmptyRight() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_natural_1 NATURAL JOIN test_natural_empty", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs an inner join between a empty table (left) and a non-empty table.
     * @throws Throwable
     */
    public void testInnerNaturalEmptyLeft() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_natural_empty NATURAL JOIN test_natural_1", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs an inner join between two empty tables;
     * @throws Throwable
     */
    public void testInnerNaturalEmptyBoth() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_natural_empty NATURAL JOIN test_natural_empty_2", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This tests performs an inner join between two nonempty tables where multiple rows of
     * the right table would join.
     * @throws Throwable
     */
    public void testInnerNaturalDupRight() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, 10, 100),
                new TupleLiteral(2, 20, 200),
                new TupleLiteral(2, 20, 222),
                new TupleLiteral(3, 30, 300),
                new TupleLiteral(4, 40, 400),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_natural_dup_1 NATURAL JOIN test_natural_dup_2", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This tests performs an inner join between two nonempty tables where multiple rows of
     * the left table would join.
     * @throws Throwable
     */
    public void testInnerNaturalDupLeft() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, 10, 100),
                new TupleLiteral(2, 20, 200),
                new TupleLiteral(2, 22, 200),
                new TupleLiteral(3, 30, 300),
                new TupleLiteral(4, 40, 400),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_natural_dup_3 NATURAL JOIN test_natural_dup_4", true);
        assert checkUnorderedResults(expected, result);
    }
}
