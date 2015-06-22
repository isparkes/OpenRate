/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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
