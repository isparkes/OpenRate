
package OpenRate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.*;

/**
 * Tests OpenRate application framework loading, stopping in RT mode.
 *
 * @author TGDSPIA1
 */
public class OpenRateRTTest {

  // The revision number has to be changed to match the current revision
  String OpenRateVersion = "V1.5.2.3";

  // By default we check that the build date is created on each build
  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
  String revisionDate = sdf.format(new Date());

  // this is the OpenRate application object
  private static OpenRate appl;

  public OpenRateRTTest() {
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
   * Test of application startup and shutdown. This test builds a real 
   * (but very simple) RT processing pipeline using the standard framework 
   * startup procedure, and then shuts it down again.
   */
  @Test(timeout = 10000)
  public void testApplicationCloseViaSemaphore() {
    System.out.println("--> OpenRate RT shutdown on Semaphore");

    // get the date portion of the version string
    int expResult = 0;

    // Define the property file we are using
    String[] args = new String[2];
    args[0] = "-p";
    args[1] = "TestRTFramework.properties.xml";

    // Start up the framework
    appl = OpenRate.getApplicationInstance();
    int status = appl.createApplication(args);

    // check the start up of the framework
    Assert.assertEquals(expResult, status);

    Thread openRateThread = new Thread(appl);
    openRateThread.start();

    System.out.println("Waiting for startup to complete");
    while (!appl.isFrameworkActive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }
    
    // And test the shutdown
    try {
      // Create file 
      FileWriter fstream = new FileWriter("Semaphore.txt");
      try (BufferedWriter out = new BufferedWriter(fstream)) {
        out.write("Framework:Shutdown=true");
      }
    } catch (IOException e) {//Catch exception if any
      Assert.fail();
    }

    // wait for it to stop
    System.out.println("Waiting for the system to stop");
    while (appl.isFrameworkActive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
      }
    }

    // Finish off
    appl.finaliseApplication();
  }
}
