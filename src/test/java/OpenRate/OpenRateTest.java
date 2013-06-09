/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package OpenRate;

import OpenRate.exception.InitializationException;
import org.junit.*;

/**
 * Tests OpenRate application framework loading.
 *
 * @author TGDSPIA1
 */
public class OpenRateTest {

    public OpenRateTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class OpenRate.
     */
    @Test
    public void testCheckParameters() {
        System.out.println("checkParameters");

        // no parameters passed at all
        String[] argsTestBad1 = new String[0];
        int expectedResult = -3;
        OpenRate app = new OpenRate();
        int result = app.checkParameters(argsTestBad1);
        Assert.assertEquals(expectedResult,result);

        // Only 1 parameter
        String[] argsTestBad2 = new String[1];
        argsTestBad2[0] = "-p";
        expectedResult = -3;
        result = app.checkParameters(argsTestBad2);
        Assert.assertEquals(expectedResult,result);

        // Only the other 1 parameter
        argsTestBad2[0] = "Simple.properties.xml";
        expectedResult = -3;
        result = app.checkParameters(argsTestBad2);
        Assert.assertEquals(expectedResult,result);

        // Both parameters, but non existent file
        String[] argsTestBad4 = new String[2];
        argsTestBad4[0] = "-p";
        argsTestBad4[1] = "Notme.properties.xml";
        expectedResult = -5;
        result = app.checkParameters(argsTestBad4);
        Assert.assertEquals(expectedResult,result);

        // ahhhh, finally! Someone got it right!
        String[] argsTestGood1 = new String[2];
        argsTestGood1[0] = "-p";
        argsTestGood1[1] = "TestDB.properties.xml";
        expectedResult = 0;
        result = app.checkParameters(argsTestGood1);
        Assert.assertEquals(expectedResult,result);
    }
    
    /**
     * Test of main method, of class OpenRate.
     */
    @Test
    public void testGetVersionString() {
        System.out.println("checkVersionString");

        String result = null;
        
        OpenRate appl = new OpenRate();
        try {
            result = appl.getApplicationVersion();
        }
        catch (InitializationException ex) {
            Assert.fail(ex.getMessage());
        }
        
        String expResult = "OpenRate V1.5.2.0, Build 123 (17.05.2013)";
        
        Assert.assertEquals(result, expResult);
    }
}
