/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
