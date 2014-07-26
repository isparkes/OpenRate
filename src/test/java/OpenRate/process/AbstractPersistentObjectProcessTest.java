/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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

package OpenRate.process;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ian
 */
public class AbstractPersistentObjectProcessTest {
  private static URL FQConfigFileName;
  private static AbstractPersistentObjectProcess instance;
  
  public AbstractPersistentObjectProcessTest() {
  }
  
  @BeforeClass
  public static void setUpClass() throws Exception 
  {
    FQConfigFileName = new URL("File:src/test/resources/TestPersistentObject.properties.xml");
    
   // Set up the OpenRate internal logger - this is normally done by app startup
    OpenRate.getApplicationInstance();

    // Load the properties into the OpenRate object
    FrameworkUtils.loadProperties(FQConfigFileName);

    // Get the loggers
    FrameworkUtils.startupLoggers();
    
    // Get the transaction manager
    FrameworkUtils.startupTransactionManager();
    
    // Get Data Sources
    //FrameworkUtils.startupDataSources();
    
    // Get the caches that we are using
    FrameworkUtils.startupCaches();
  }
  
  @AfterClass
  public static void tearDownClass() {
    // Deallocate
    OpenRate.getApplicationInstance().cleanup();
  }
  
  @Before
  public void setUp() {
      getInstance();
  }
  
  @After
  public void tearDown() {
      releaseInstance();
  }

  /**
   * Test of getObject method, of class AbstractPersistentObjectProcess.
   */
  @Test
  public void testGetObject() {
    System.out.println("getObject");
    String ObjectKey = "testkey1";
    String expResult = "1235";
    String result;
    
    // Good retieval
    instance.putObject(ObjectKey, expResult);
    result = (String) instance.getObject(ObjectKey);
    assertEquals(expResult, result);
    
    result = (String) instance.getObject(ObjectKey+"not there");
    assertEquals(null, result);
    
    // Clean up to make tests order independent
    instance.deleteObject(ObjectKey);
  }

  /**
   * Test of deleteObject method, of class AbstractPersistentObjectProcess.
   */
  @Test
  public void testDeleteObject() {
    System.out.println("deleteObject");
    
    String ObjectKey = "testkey2";
    String expResult = "1235";
    String result;
    
    // Good retieval
    instance.putObject(ObjectKey, expResult);
    result = (String) instance.getObject(ObjectKey);
    assertEquals(expResult, result);

    // Do the deletion
    instance.deleteObject(ObjectKey);
    
    // See if it is still there
    result = (String) instance.getObject(ObjectKey+"not there");
    assertEquals(null, result);
  }

  /**
   * Test of containsObjectKey method, of class AbstractPersistentObjectProcess.
   */
  @Test
  public void testContainsObjectKey() {
    System.out.println("containsObjectKey");
    
    String ObjectKey = "testkey3";
    boolean result;
    
    // Good retieval
    instance.putObject(ObjectKey, "test");
    result = instance.containsObjectKey(ObjectKey);
    assertEquals(true, result);

    // Do the deletion
    instance.deleteObject(ObjectKey);
    
    // See if it is still there
    result = instance.containsObjectKey(ObjectKey);
    assertEquals(false, result);
  }

  /**
   * Test of getObjectKeySet method, of class AbstractPersistentObjectProcess.
   */
  @Test
  public void testGetObjectKeySet() {
    System.out.println("getObjectKeySet");
    instance.putObject("1", "test");
    instance.putObject("2", "test");
    instance.putObject("3", "test");
    
    Set result = instance.getObjectKeySet();
    assertEquals(3, result.size());
  }

  public class AbstractPersistentObjectProcessImpl extends AbstractPersistentObjectProcess {

    @Override
    public IRecord procValidRecord(IRecord r) {
      return r;
    }

    @Override
    public IRecord procErrorRecord(IRecord r) {
      return r;
    }
  }
  
 /**
  * Method to get an instance of the implementation. Done this way to allow
  * tests to be executed individually.
  *
  * @throws InitializationException
  */
  private void getInstance()
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractPersistentObjectProcessTest.AbstractPersistentObjectProcessImpl();
      
      try
      {
        // Get the instance
        instance.init("DBTestPipe", "AbstractPersistentObjectProcessTest");
      }
      catch (InitializationException ex)
      {
        org.junit.Assert.fail();
      }

    }
    else
    {
      org.junit.Assert.fail("Instance already allocated");
    }
  }
  
 /**
  * Method to release an instance of the implementation.
  */
  private void releaseInstance()
  {
    instance = null;
  }
}