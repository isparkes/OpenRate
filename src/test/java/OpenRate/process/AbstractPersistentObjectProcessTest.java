

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
  private static OpenRate appl;
  
  public AbstractPersistentObjectProcessTest() {
  }
  
  @BeforeClass
  public static void setUpClass() throws Exception 
  {
    FQConfigFileName = new URL("File:src/test/resources/TestPersistentObject.properties.xml");
    
   // Set up the OpenRate internal logger - this is normally done by app startup
    appl = OpenRate.getApplicationInstance();

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
    OpenRate.getApplicationInstance().finaliseApplication();
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