

package ExampleApplications.SimpleApplication;

import OpenRate.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.*;

/**
 * Tests OpenRate simple application processing.
 *
 * @author TGDSPIA1
 */
public class SimpleApplicationTest {

    public SimpleApplicationTest() {
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
     * Test of application startup. This test builds a real (but very simple)
     * processing pipeline using the standard framework startup procedure.
     */
    //@Test
    public void testSimpleApplication() {
      System.out.println("Simple Processing Application");

        // Define the property file we are using
        String[] args = new String[2];
        args[0] = "-p";
        args[1] = "Simple.properties.xml";
        
        // Start up the framework
        OpenRate appl = OpenRate.getApplicationInstance();
        int status = appl.createApplication(args);

        // check the start up of the framework
        Assert.assertEquals(0,status);
        
        Thread openRateThread = new Thread(appl);
        openRateThread.start();
        
        // Do the processing
        
        
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
