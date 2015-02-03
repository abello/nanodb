package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class tests some simple inner and outer joins.
 */
@Test
public class TestUsingJoins extends SqlTestCase {

    public TestUsingJoins() {
        super("setup_testUsingJoins");
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
    public void testInnerSimple() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(10, 1, 1, 100),
            new TupleLiteral(20, 2, 2, 200),
            new TupleLiteral(30, 3, 3, 300),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_joins_1 AS t1 INNER JOIN test_joins_2 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs an inner join between a empty table (right) and a non-empty table.
     * @throws Throwable
     */
    public void testInnerEmptyRight() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_joins_1 AS t1 INNER JOIN test_joins_empty AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs an inner join between a empty table (left) and a non-empty table.
     * @throws Throwable
     */
    public void testInnerEmptyLeft() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_joins_empty AS t1 INNER JOIN test_joins_1 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs an inner join between two empty tables;
     * @throws Throwable
     */
    public void testInnerEmptyBoth() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_joins_empty AS t1 INNER JOIN test_joins_empty_2 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This tests performs an inner join between two nonempty tables where multiple rows of
     * the right table would join.
     * @throws Throwable
     */
    public void testInnerDupRight() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(10, 1, 1, 100),
                new TupleLiteral(20, 2, 2, 200),
                new TupleLiteral(20, 2, 2, 222),
                new TupleLiteral(30, 3, 3, 300),
                new TupleLiteral(40, 4, 4, 400),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_joins_dup_1 AS t1 INNER JOIN test_joins_dup_2 AS t2 ON USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This tests performs an inner join between two nonempty tables where multiple rows of
     * the left table would join.
     * @throws Throwable
     */
    public void testInnerDupLeft() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(10, 1, 1, 100),
                new TupleLiteral(20, 2, 2, 200),
                new TupleLiteral(22, 2, 2, 200),
                new TupleLiteral(30, 3, 3, 300),
                new TupleLiteral(40, 4, 4, 400),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_joins_dup_3 AS t1 INNER JOIN test_joins_dup_4 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }
}
