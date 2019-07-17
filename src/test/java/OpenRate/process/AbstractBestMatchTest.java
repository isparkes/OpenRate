
package OpenRate.process;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import org.junit.*;

/**
 * Unit test for AbstractBestMatch.
 *
 * @author tgdspia1
 */
public class AbstractBestMatchTest
{
  private static URL FQConfigFileName;

  private static AbstractBestMatch instance;

  // Used for logging and exception handling
  private static String message; 
  private static OpenRate appl;

 /**
  * This sets up a run time environment for testing modules. It is fairly
  * elaborate, because of the amount of support that a module needs in order
  * to function properly. It reports exceptions to the framework or pipeline
  * exception handler, logs to one of four logs, all of which are normally
  * found at runtime. For unit testing, we have to set these up manually.
  * 
  * @throws Exception 
  */
  @BeforeClass
  public static void setUpClass() throws Exception
  {
    FQConfigFileName = new URL("File:src/test/resources/TestBestMatchDB.properties.xml");
    
    // Set up the OpenRate internal logger - this is normally done by app startup
    appl = OpenRate.getApplicationInstance();

    // Load the properties into the OpenRate object
    FrameworkUtils.loadProperties(FQConfigFileName);

    // Get the loggers
    FrameworkUtils.startupLoggers();

    // Get the transaction manager
    FrameworkUtils.startupTransactionManager();
    
    // Get Data Sources
    FrameworkUtils.startupDataSources();
    
    // Get a connection
    Connection JDBCChcon = FrameworkUtils.getDBConnection("BestMatchTestCache");

    try
    {
      JDBCChcon.prepareStatement("DROP TABLE TEST_BEST_MATCH;").execute();
    }
    catch (Exception ex)
    {
      if ((ex.getMessage().startsWith("Unknown table")) || // Mysql
          (ex.getMessage().startsWith("user lacks")))      // HSQL
      {
        // It's OK
      }
      else
      {
        // Not OK, fail the case
        message = "Error dropping table TEST_BEST_MATCH in test <AbstractBestMatchTest>.";
        Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_BEST_MATCH (MAP_GROUP varchar(24),INPUT_VAL varchar(64), OUTPUT_VAL1 varchar(64), OUTPUT_VAL2 varchar(64));").execute();

    // Create some records in the table
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','0044','UK','UK Any');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','00','INTL','Rest of the world');").execute();
    
    // Create some records in the table
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale1','0032','WD1','Belgium');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale1','00328165','WD2','Termination to Belgium Geographical Number');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale2','0032','WD3','Belgium');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_BEST_MATCH (MAP_GROUP,INPUT_VAL,OUTPUT_VAL1,OUTPUT_VAL2) values ('WholeSale2','00328165','WD4','Termination to Belgium Geographical Number');").execute();
    
    // Get the caches that we are using
    FrameworkUtils.startupCaches();
  }

  @AfterClass
  public static void tearDownClass()
  {
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
   * Test of getBestMatch method, of class AbstractBestMatch.
   */
  @Test
  public void testGetBestMatch()
  {
    String BNumber;
    String result;
    String expResult;
    String Group;

    System.out.println("getBestMatch");

    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "UK";
    Assert.assertEquals(expResult, result);

    // Simple good case
    Group = "DefaultMap";
    BNumber = "004923434";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "INTL";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBestMatch method, of class AbstractBestMatch - defect case
   */
  @Test
  public void testGetBestMatchErg()
  {
    String BNumber;
    String result;
    String expResult;
    String Group;

    System.out.println("getBestMatchErg");

    // Simple good case
    Group = "WholeSale1";
    BNumber = "003281656264";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "WD2";
    Assert.assertEquals(expResult, result);
    
    // Simple good case
    Group = "WholeSale2";
    BNumber = "003281656264";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "WD4";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBestMatch method, of class AbstractBestMatch - defect case
   */
  @Test
  public void testGetBestMatchNoResults()
  {
    String BNumber;
    String result;
    String expResult;
    String Group;

    System.out.println("testGetBestMatchNoResults");

    // Access a map group (digit tree) we don't have at all
    Group = "WholeSale9";
    BNumber = "003281656264";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);
    
    // Simple good case
    Group = "WholeSale1";
    BNumber = "99999999999";
    result = instance.getBestMatch(Group, BNumber);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBestMatchWithChildData method, of class AbstractBestMatch.
   */
  @Test
  public void testGetBestMatchWithChildData()
  {
    String BNumber;
    ArrayList<String> result;
    ArrayList<String> expResult;
    String Group;
    
    System.out.println("getBestMatchWithChildData");
    
    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.getBestMatchWithChildData(Group, BNumber);
    expResult = new ArrayList();
    expResult.add("UK");
    expResult.add("UK Any");
    Assert.assertEquals(expResult, result);

    // Simple good case
    Group = "DefaultMap";
    BNumber = "004923434";
    result = instance.getBestMatchWithChildData(Group, BNumber);
    expResult = new ArrayList();
    expResult.add("INTL");
    expResult.add("Rest of the world");
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of isValidBestMatchResult method, of class AbstractBestMatch.
   */
  @Test
  public void testIsValidBestMatchResult_ArrayList()
  {
    String BNumber;
    boolean result;
    boolean expResult;
    String Group;

    System.out.println("isValidBestMatchResult");
    
    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.isValidBestMatchResult(instance.getBestMatchWithChildData(Group, BNumber));
    expResult = true;
    Assert.assertEquals(expResult, result);

    // Simple bad case
    Group = "DefaultMap";
    BNumber = "99999";
    result = instance.isValidBestMatchResult(instance.getBestMatchWithChildData(Group, BNumber));
    expResult = false;
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of isValidBestMatchResult method, of class AbstractBestMatch.
   */
  @Test
  public void testIsValidBestMatchResult_String()
  {
    System.out.println("isValidBestMatchResult");
    
    String BNumber;
    boolean result;
    boolean expResult;
    String Group;

    System.out.println("isValidBestMatchResult");
    
    // Simple good case
    Group = "DefaultMap";
    BNumber = "0044123";
    result = instance.isValidBestMatchResult(instance.getBestMatch(Group, BNumber));
    expResult = true;
    Assert.assertEquals(expResult, result);

    // Simple bad case
    Group = "DefaultMap";
    BNumber = "99999";
    result = instance.isValidBestMatchResult(instance.getBestMatch(Group, BNumber));
    expResult = false;
    Assert.assertEquals(expResult, result);
  }

  public class AbstractBestMatchImpl extends AbstractBestMatch
  {
   /**
    * Override the unused event handling routines.
    *
    * @param r input record
    * @return return record
    * @throws ProcessingException
    */
    @Override
    public IRecord procValidRecord(IRecord r) throws ProcessingException
    {
      return r;
    }

   /**
    * Override the unused event handling routines.
    *
    * @param r input record
    * @return return record
    * @throws ProcessingException
    */
    @Override
    public IRecord procErrorRecord(IRecord r) throws ProcessingException
    {
      return r;
    }
  }

 /**
  * Method to get an instance of the implementation. Done this way to allow
  * tests to be executed individually.
  */
  private AbstractBestMatch getInstance()
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractBestMatchTest.AbstractBestMatchImpl();
      
      // Get the instance
      try
      {
        instance.init("DBTestPipe", "AbstractBestMatchTest");
      }
      catch (InitializationException ex)
      {
        Assert.fail();
      }
    }
    else
    {
      Assert.fail("Instance already allocated");
    }
    
    return instance;
  }
  
 /**
  * Method to release an instance of the implementation.
  */
  private void releaseInstance()
  {
    instance = null;
  }
}
