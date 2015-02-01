package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class tests some simple inner and outer joins.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }



    /**
     * This test performs some simple projects that perform arithmetic on the
     * column values, to see if the queries produce the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testProjectMath() throws Throwable {
        // Columns a - 10 as am, c * 3 as cm
        TupleLiteral[] expected = {
            new TupleLiteral(-9,   30),
            new TupleLiteral(-8,   60),
            new TupleLiteral(-7,   90),
            new TupleLiteral(-6, null),
            new TupleLiteral(-5,  120),
            new TupleLiteral(-4,  150)
        };

        CommandResult result;

        result = server.doCommand(
            "SELECT a - 10 AS am, c * 3 AS cm FROM test_select_project", true);
        assert checkUnorderedResults(expected, result);
    }

}
