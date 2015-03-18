package edu.caltech.test.nanodb.sql;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;


/**
 * This class tests the limit node.
 */
@Test
public class TestLimit extends SqlTestCase {

    public TestLimit() {
        super("setup_testLimit");
    }


    /**
     * ************************************************************
     * ********************* Limit Node Test **********************
     * ************************************************************
     */

    /**
     * This test performs a simple limit on a selection.	
     */
    public void testLimitSimple() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(0, 0),
            new TupleLiteral(10, 1),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_limit limit 2", true);
        assert checkUnorderedResults(expected, result);
    }
    
    /**
     * This test tries to select limit the size of the number of tuples.	
     */
    public void testLimitNumRows() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(0, 0),
            new TupleLiteral(10, 1),
            new TupleLiteral(20, 2),
            new TupleLiteral(30, 3),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_limit limit 4", true);
        assert checkUnorderedResults(expected, result);
    }
    
    /**
     * This test tries to select limit a size greater than the number of tuples.	
     */
    public void testLimitMoreThanNumRows() throws Throwable {
        TupleLiteral[] expected = {
            new TupleLiteral(0, 0),
            new TupleLiteral(10, 1),
            new TupleLiteral(20, 2),
            new TupleLiteral(30, 3),
        };

        CommandResult result;

        result = server.doCommand("SELECT * FROM test_limit limit 100", true);
        assert checkUnorderedResults(expected, result);
    }
}
