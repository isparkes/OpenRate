/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package OpenRate;

import OpenRate.exception.InitializationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.*;

/**
 * Tests OpenRate application framework loading.
 *
 * @author TGDSPIA1
 */
public class OpenRateTest {

    // The revision number has to be changed to match the current revision
    int    revisionNumber = 16;
    String revisionDate   = "20130731";
        
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
     * Test of version string method, of class OpenRate.
     */
    @Test
    public void testGetVersionString() {
        System.out.println("checkVersionString");

        // get the date portion of the version string
        String revision = String.valueOf(revisionNumber);
        String result = null;
        
        OpenRate appl = new OpenRate();
        try {
          result = appl.getApplicationVersion();
        }
        catch (InitializationException ex) {
            Assert.fail(ex.getMessage());
        }
        
        String expResult = "OpenRate V1.5.2.0, Build "+revision+" ("+revisionDate+")";
        Assert.assertEquals(expResult,result);
    }
    
    /**
     * Test of application startup. This test builds a real (but very simple)
     * processing pipeline using the standard framework startup procedure.
     */
    @Test
    public void testApplicationStartup() {
        System.out.println("OpenRate startup");

        // get the date portion of the version string
        int expResult = 0;

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "TestFramework.properties.xml";
        
        // Start up the framework
        OpenRate appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);

        // check the start up of the framework
        Assert.assertEquals(expResult,status);
        
        Thread openRateThread = new Thread(appl);
        openRateThread.start();
        
        // And test the shutdown
        appl.stopAllPipelines();
        
        // wait for it to stop
        while (appl.isFrameworkActive())
        {
          try {
            Thread.sleep(100);
          }
          catch (InterruptedException ex) {
            Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
          }
        }

        // Finish off
        appl.finaliseApplication();
    }
    
    /**
     * Test of application startup. This test builds a real (but very simple)
     * processing pipeline using the standard framework startup procedure.
     */
    @Test
    public void testApplicationStartupFail() {
        System.out.println("OpenRate startup failure: Properties not found");

        // get the date portion of the version string
        int expResult = -5;

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "DoesNotExist.properties.xml";
        
        // Start up the framework
        OpenRate appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);

        // check the start up of the framework
        Assert.assertEquals(expResult,status);
    }
    
    /**
     * Test of application startup. This test builds a real (but very simple)
     * processing pipeline using the standard framework startup procedure.
     */
    @Test
    public void testApplicationConsole() {
        System.out.println("OpenRate console");

        // get the date portion of the version string
        int expResult = 0;

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "TestFramework.properties.xml";
        
        // Start up the framework
        OpenRate appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);

        // check the start up of the framework
        Assert.assertEquals(expResult,status);
        
        // Now test that we can connect via socket
        Socket testSocket = null;
        try {
          testSocket = new Socket("localhost", 8086);
        }
        catch (UnknownHostException ex) {
          Assert.fail("Unable to open socket");
        }
        catch (IOException ex) {
          Assert.fail("Unable to open socket");
        }
        
        if (testSocket == null)
        {
          Assert.fail("Could not get socket");
          return;
        }
        
        BufferedReader inputReader = null;
        try {
          inputReader = new BufferedReader(new InputStreamReader(testSocket.getInputStream()));
        }
        catch (IOException ex) {
          Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        if (inputReader == null)
        {
          Assert.fail("Unable to get reader");
          return;
        }
        
        String[] headerResponse = new String[7];
        headerResponse[0] = "--------------------------------------------------------------";
        headerResponse[1] = "OpenRate Admin Console, " + OpenRate.getApplicationVersionString();
        headerResponse[2] = "Copyright Tiger Shore Management Ltd, 2006-2013";
        headerResponse[3] = headerResponse[0];
        headerResponse[4] = "";
        headerResponse[5] = "Type 'Help' for more information.";
        headerResponse[6] = "";
        
        // Get the welcome message
        String responseLine;
        int index = 0;
        try {
        while ((responseLine = inputReader.readLine()) != null) {
          // Check that we got the right response
          Assert.assertEquals(headerResponse[index++], responseLine);
          System.out.println("Server: " + responseLine);
          if (index == 7) {
            break;
            }
          }
        }
        catch (IOException ex) {
          Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        char responseVal;
        index = 1;
        try {
          // Now see if we got the promt (this is not a full line)
          while ((responseVal = (char) inputReader.read()) != -1)
          {
            System.out.print(responseVal);
            if (index++ == 10)
              break;
          }
        }
        catch (IOException ex) {
          Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Now check the list of modules
        PrintWriter out = null;
        try {
          out = new PrintWriter(testSocket.getOutputStream(), true);
        }
        catch (IOException ex) {
          Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        out.println("m");
        String[] moduleResponse = new String[7];
        moduleResponse[0] = "OpenRate module listing:";
        moduleResponse[1] = "+--------------------+----------------------------------------+----------------------------------------------------+";
        moduleResponse[2] = "| Pipeline Name      | Module Name                            | Class                                              |";
        moduleResponse[3] = moduleResponse[1];
        moduleResponse[4] = "| DBTestPipe         | DBTestPipe                             | OpenRate.Pipeline                                  | ";
        moduleResponse[5] = "| Framework          | Framework                              | OpenRate.OpenRate                                  | ";
        moduleResponse[6] = "| Resource           | LogFactory                             | OpenRate.logging.LogFactory                        | ";

        index = 0;
        try {
        while ((responseLine = inputReader.readLine()) != null) {
          // Check that we got the right response
          Assert.assertEquals(moduleResponse[index++], responseLine);
          System.out.println("Server: " + responseLine);
          if (index == 7) {
            break;
            }
          }
        }
        catch (IOException ex) {
          Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        // And test the shutdown
        appl.stopAllPipelines();
        
        // wait for it to stop
        while (appl.isFrameworkActive())
        {
          try {
            Thread.sleep(100);
          }
          catch (InterruptedException ex) {
            Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
          }
        }

        // Finish off
        appl.finaliseApplication();
    }    
}
