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
    public void testInnerUsingSimple() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(1, 10, 100),
            new TupleLiteral(2, 20, 200),
            new TupleLiteral(3, 30, 300),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_using_1 AS t1 INNER JOIN test_using_2 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs an inner join between a empty table (right) and a non-empty table.
     * @throws Throwable
     */
    public void testInnerUsingEmptyRight() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_using_1 AS t1 INNER JOIN test_using_empty AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }

    /**
     * This test performs an inner join between a empty table (left) and a non-empty table.
     * @throws Throwable
     */
    public void testInnerUsingEmptyLeft() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_using_empty AS t1 INNER JOIN test_using_1 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This test performs an inner join between two empty tables;
     * @throws Throwable
     */
    public void testInnerUsingEmptyBoth() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_using_empty AS t1 INNER JOIN test_using_empty_2 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This tests performs an inner join between two nonempty tables where multiple rows of
     * the right table would join.
     * @throws Throwable
     */
    public void testInnerUsingDupRight() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, 10, 100),
                new TupleLiteral(2, 20, 200),
                new TupleLiteral(2, 20, 222),
                new TupleLiteral(3, 30, 300),
                new TupleLiteral(4, 40, 400),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_using_dup_1 AS t1 INNER JOIN test_using_dup_2 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }


    /**
     * This tests performs an inner join between two nonempty tables where multiple rows of
     * the left table would join.
     * @throws Throwable
     */
    public void testInnerUsingDupLeft() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, 10, 100),
                new TupleLiteral(2, 20, 200),
                new TupleLiteral(2, 22, 200),
                new TupleLiteral(3, 30, 300),
                new TupleLiteral(4, 40, 400),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_using_dup_3 AS t1 INNER JOIN test_using_dup_4 AS t2 USING (b)", true);
        assert checkUnorderedResults(expected, result);
    }
}
