
package OpenRate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.*;

/**
 * Tests OpenRate application framework basic file processing capability.
 *
 * @author TGDSPIA1
 */
public class OpenRateFileProcessingTest {

  // this is the OpenRate application object
  private static OpenRate appl;

  public OpenRateFileProcessingTest() {
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
   * Test application file processing. We start an application, process a single
   * file and then shut down.
   */
  @Test(timeout = 10000)
  public void testFileStartupAndShutdown() {
    System.out.println("--> OpenRate file Pipe startup and shutdown");

    // get the date portion of the version string
    int expResult = 0;

    // Define the property file we are using
    String[] args = new String[2];
    args[0] = "-p";
    args[1] = "TestFileProcessing.properties.xml";

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
    
    // wait for it to start
    System.out.println("Waiting for the system to come up");
    while (!appl.isFrameworkActive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Logger.getLogger(OpenRateFileProcessingTest.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

    // And test the shutdown using an injected stop message
    appl.processControlEvent("Shutdown", false, "true");

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
  
  /**
   * Test application file processing. We start an application, process a single
   * file and then shut down.
   */
  @Test(timeout = 20000)
  public void testFileProcessAndShutdown() {
    System.out.println("--> OpenRate file Pipe startup,process and shutdown");

    // get the date portion of the version string
    int expResult = 0;

    // Define the property file we are using
    String[] args = new String[2];
    args[0] = "-p";
    args[1] = "TestFileProcessing.properties.xml";

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
    
    // wait for it to start
    System.out.println("Waiting for the system to come up");
    while (!appl.isFrameworkActive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }

    // Put a file in for processing
    String randomStreamName = ""+Calendar.getInstance().getTimeInMillis();
    String fileName = "target/CDR_" + randomStreamName + ".in";
    String fileNameWait = fileName + ".wait";
    String fileNameDone = "target/CDR_" + randomStreamName + ".out";
    
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileNameWait), "utf-8"))) {
      // write 10 records
      for (int idx = 0; idx < 10 ; idx++) {
        writer.write("Line " + idx + " in the input file");
      }
    } catch (IOException ex) {
      Assert.fail("Exception writing file test file" + fileNameWait + ": " + ex.getMessage());
    }    

    // Move the file
    File mvFile = new File(fileNameWait);
    File targetFile = new File(fileName);
    mvFile.renameTo(targetFile);
    
    // Now wait for it to process
    File doneFile = new File(fileNameDone);
    System.out.println("Waiting for file processing to complete");
    while (!doneFile.exists()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }        
    
    // And test the shutdown using an injected stop message
    appl.processControlEvent("Shutdown", false, "true");

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
