/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * Tiger Shore Management or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */

package OpenRate;

import OpenRate.exception.InitializationException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.*;

/**
 * Tests OpenRate application framework loading, stopping, version handling,
 * console access and sundry functions. These tests are generally higher level
 * than process based tests, and attempt to provide a "roll up" of the 
 * other tests.
 *
 * @author TGDSPIA1
 */
public class OpenRateTest {

    // The revision number has to be changed to match the current revision
    String OpenRateVersion = "V1.5.2.1";
    
    // This has to match the current SVN revision tag
    int    revisionNumber = 26;
    
    // By default we check that the build date is created on each build
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    String revisionDate   = sdf.format(new Date());
    
    // this is the OpenRate application object
    private static OpenRate appl;
        
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
        System.out.println("--> checkParameters");

        // no parameters passed at all
        String[] argsTestBad1 = new String[0];
        int expectedResult = -3;
        
        // get the application instance
        appl = OpenRate.getApplicationInstance();
        
        int result = appl.checkParameters(argsTestBad1);
        Assert.assertEquals(expectedResult,result);

        // Only 1 parameter
        String[] argsTestBad2 = new String[1];
        argsTestBad2[0] = "-p";
        expectedResult = -3;
        result = appl.checkParameters(argsTestBad2);
        Assert.assertEquals(expectedResult,result);

        // Only the other 1 parameter
        argsTestBad2[0] = "Simple.properties.xml";
        expectedResult = -3;
        result = appl.checkParameters(argsTestBad2);
        Assert.assertEquals(expectedResult,result);

        // Both parameters, but non existent file
        String[] argsTestBad4 = new String[2];
        argsTestBad4[0] = "-p";
        argsTestBad4[1] = "Notme.properties.xml";
        expectedResult = -5;
        result = appl.checkParameters(argsTestBad4);
        Assert.assertEquals(expectedResult,result);

        // ahhhh, finally! Someone got it right!
        String[] argsTestGood1 = new String[2];
        argsTestGood1[0] = "-p";
        argsTestGood1[1] = "TestDB.properties.xml";
        expectedResult = 0;
        result = appl.checkParameters(argsTestGood1);
        Assert.assertEquals(expectedResult,result);
        
        appl.cleanup();
    }
    
    /**
     * Test of version string method, of class OpenRate.
     */
    @Test
    public void testGetVersionString() {
        System.out.println("--> checkVersionString");

        // get the date portion of the version string
        String revision = String.valueOf(revisionNumber);
        String result = null;
        
        // get the application instance
        appl = OpenRate.getApplicationInstance();
        
        try {
          result = appl.getApplicationVersion();
        }
        catch (InitializationException ex) {
            Assert.fail(ex.getMessage());
        }
        
        String expResult = "OpenRate "+OpenRateVersion+", Build "+revision+" ("+revisionDate+")";
        Assert.assertEquals(expResult,result);
        
        appl.cleanup();
    }
    
    /**
     * Test of application startup. This test builds a real (but very simple)
     * processing pipeline using the standard framework startup procedure.
     */
    @Test
    public void testApplicationStartup() {
        System.out.println("--> OpenRate startup");

        // get the date portion of the version string
        int expResult = 0;

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "TestFramework.properties.xml";
        
        // Start up the framework
        appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);

        // check the start up of the framework
        Assert.assertEquals(expResult,status);
        
        Thread openRateThread = new Thread(appl);
        openRateThread.start();
        
        // wait for it to start
        while (!appl.isFrameworkActive())
        {
          System.out.println("Waiting 100mS for the system to come up");
          try {
            Thread.sleep(100);
          }
          catch (InterruptedException ex) {
            Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
        
        // And test the shutdown using an injected stop message
        appl.processControlEvent("Shutdown", false, "true");
        
        // wait for it to stop
        while (appl.isFrameworkActive())
        {
          System.out.println("Waiting 1000mS for the system to stop");
          try {
            Thread.sleep(1000);
          }
          catch (InterruptedException ex) {
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
        System.out.println("--> OpenRate startup failure: Properties not found");

        // get the date portion of the version string
        int expResult = -5;

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "DoesNotExist.properties.xml";
        
        // Start up the framework
        appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);

        // check the start up of the framework
        Assert.assertEquals(expResult,status);
        
        // Finish off
        appl.finaliseApplication();
    }
    
    /**
     * Test of application startup. This test builds a real (but very simple)
     * processing pipeline using the standard framework startup procedure.
     */
    @Test
    public void testApplicationCloseViaSemaphore() {
        System.out.println("--> OpenRate shutdown on Semaphore");

        // get the date portion of the version string
        int expResult = 0;

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "TestFramework.properties.xml";
        
        // Start up the framework
        appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);

        // check the start up of the framework
        Assert.assertEquals(expResult,status);
        
        Thread openRateThread = new Thread(appl);
        openRateThread.start();
        
        // And test the shutdown
        try{
          // Create file 
          FileWriter fstream = new FileWriter("Semaphore.txt");
          try (BufferedWriter out = new BufferedWriter(fstream)) {
            out.write("Framework:Shutdown=true");
          }
        }catch (Exception e){//Catch exception if any
          Assert.fail();
        }
        
        // wait for it to stop
        while (appl.isFrameworkActive())
        {
          System.out.println("Waiting 1000mS for the system to stop");
          try {
            Thread.sleep(1000);
          }
          catch (InterruptedException ex) {
          }
        }

        // Finish off
        appl.finaliseApplication();
    }
    
    /**
     * Test of application console. This test builds a real (but very simple)
     * processing pipeline using the standard framework startup procedure.
     */
    @Test
    public void testApplicationConsole() {
        System.out.println("--> OpenRate console");

        // get the date portion of the version string
        int expResult = 0;

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "TestFramework.properties.xml";
        
        // Start up the framework
        appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);
        
        Thread openRateThread = new Thread(appl);
        openRateThread.start();
        
        while (!appl.isFrameworkActive())
        {
          System.out.println("Waiting 100mS for startup to complete");
          try {
            Thread.sleep(100);
          } catch (InterruptedException ex) {
          }
        }

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

        if (out == null)
        {
          Assert.fail("Could not get socket to write to");
        }
        else
        {
          out.println("m");
          String[] moduleResponse = new String[12];
          moduleResponse[0]  = "OpenRate module listing:";
          moduleResponse[1]  = "+--------------------+----------------------------------------+----------------------------------------------------+";
          moduleResponse[2]  = "| Pipeline Name      | Module Name                            | Class                                              |";
          moduleResponse[3]  = moduleResponse[1];
          moduleResponse[4]  = "| DBTestPipe         | DBTestPipe                             | OpenRate.Pipeline                                  | ";
          moduleResponse[5]  = "| Framework          | Framework                              | OpenRate.OpenRate                                  | ";
          moduleResponse[6]  = "| Resource           | LogFactory                             | OpenRate.logging.LogFactory                        | ";
          moduleResponse[7]  = "| DBTestPipe         | SOutAdapter                            | OpenRate.adapter.NullOutputAdapter                 | ";
          moduleResponse[8]  = "| DBTestPipe         | TestInpAdapter                         | OpenRate.adapter.NullInputAdapter                  | ";
          moduleResponse[9]  = "| Resource           | TransactionManager                     | OpenRate.transaction.TransactionManager            | ";
          moduleResponse[10] = moduleResponse[1];

          index = 0;
          try {
          while ((responseLine = inputReader.readLine()) != null) {
            // Check that we got the right response
            Assert.assertEquals(moduleResponse[index++], responseLine);
            System.out.println("Server: " + responseLine);
            if (index == 11) {
              break;
              }
            }
          }
          catch (IOException ex) {
            Logger.getLogger(OpenRateTest.class.getName()).log(Level.SEVERE, null, ex);
          }
        
          // Stop
          out.println("Framework:Shutdown=true");

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
        }

        // Finish off
        appl.finaliseApplication();
    }
}
