
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
 * Unit test for AbstractRegexMatch.
 *
 * @author tgdspia1
 */
public class AbstractRegexMatchDBTest
{
  private static URL FQConfigFileName;
  private static AbstractRegexMatch instance;

  // Used for logging and exception handling
  private static String message; 
  private static OpenRate appl;

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    FQConfigFileName = new URL("File:src/test/resources/TestRegexDB.properties.xml");
    
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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("RegexMatchTestCache");

    try
    {
      JDBCChcon.prepareStatement("DROP TABLE TEST_REGEX").execute();
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
        // Not OK, Assert.fail the case
        message = "Error dropping table TEST_REGEX in test <AbstractRegexMatchTest>.";
        Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_REGEX (MAP_GROUP varchar(24), INPUT_VAL1 varchar(64), INPUT_VAL2 varchar(64), OUTPUT_VAL1 varchar(64), OUTPUT_VAL2 varchar(64), RANK int);").execute();

    // Create some records in the table
    JDBCChcon.prepareStatement("INSERT INTO TEST_REGEX (MAP_GROUP,INPUT_VAL1,INPUT_VAL2,OUTPUT_VAL1,OUTPUT_VAL2,RANK) values ('DefaultMap','01.*','.*','OK1','OUT2',1);").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_REGEX (MAP_GROUP,INPUT_VAL1,INPUT_VAL2,OUTPUT_VAL1,OUTPUT_VAL2,RANK) values ('DefaultMap','0.*','.*','OK2','OUT2',2);").execute();

    // Create some records in the table for numerical matching
    JDBCChcon.prepareStatement("INSERT INTO TEST_REGEX (MAP_GROUP,INPUT_VAL1,INPUT_VAL2,OUTPUT_VAL1,OUTPUT_VAL2,RANK) values ('NumericalMap','>1','<=7','OK1','OUT2',1);").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_REGEX (MAP_GROUP,INPUT_VAL1,INPUT_VAL2,OUTPUT_VAL1,OUTPUT_VAL2,RANK) values ('NumericalMap','<1','=2','OK2','OUT2',2);").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_REGEX (MAP_GROUP,INPUT_VAL1,INPUT_VAL2,OUTPUT_VAL1,OUTPUT_VAL2,RANK) values ('NumericalMap','=9.2','=9.2','OK3','OUT3',2);").execute();

    // Get the caches that we are using
    FrameworkUtils.startupCaches();
  }

  @AfterClass
  public static void tearDownClass() {
    // Deallocate
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
   * Test of getRegexMatch method, of class AbstractRegexMatch.
   */
  @Test
  public void testGetRegexMatch()
  {
    String result;
    String expResult;
    String Group;

    System.out.println("getRegexMatch DB");

    String[] searchParameters = new String[1];

    // Simple good case
    Group = "DefaultMap";
    searchParameters[0] = "023456";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK2";
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultMap";
    searchParameters[0] = "1";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);

    // Simple good case with lower rank
    Group = "DefaultMap";
    searchParameters[0] = "0123456";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK1";
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getRegexMatch method, of class AbstractRegexMatch.
   */
  @Test
  public void testGetRegexMatchNumericalComparison()
  {
    String result;
    String expResult;
    String Group;

    System.out.println("getRegexMatch (Numerical Comparison) DB");

    String[] searchParameters = new String[2];

    // Simple good case
    Group = "NumericalMap";
    searchParameters[0] = "2";
    searchParameters[1] = "2";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK1";
    Assert.assertEquals(expResult, result);
    
    // Simple good case with a double value
    searchParameters[0] = "1.00001";
    searchParameters[1] = "2";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK1";
    Assert.assertEquals(expResult, result);
    
    // Simple good case with a double value
    searchParameters[0] = "0.8";
    searchParameters[1] = "2";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK2";
    Assert.assertEquals(expResult, result);
    
    // Simple bad case with a double value
    searchParameters[0] = "0.8";
    searchParameters[1] = "3";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);
    
    // Simple good case with a double value - this is there to check if we are rounding anywhere
    searchParameters[0] = "9.2";
    searchParameters[1] = "9.2";
    result = instance.getRegexMatch(Group, searchParameters);
    expResult = "OK3";
    Assert.assertEquals(expResult, result);
    
  }

  /**
   * Test of getRegexMatchWithChildData method, of class AbstractRegexMatch.
   */
  @Test
  public void testGetRegexMatchWithChildData()
  {
    ArrayList<String> result;
    ArrayList<String> expResult = new ArrayList<>();
    String Group;

    System.out.println("getRegexMatchWithChildData DB");

    String[] searchParameters = new String[1];

    // Simple good case
    Group = "DefaultMap";
    searchParameters[0] = "023456";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("OK2");
    expResult.add("OUT2");
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultMap";
    searchParameters[0] = "1";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("NOMATCH");
    Assert.assertEquals(expResult, result);

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("NOMATCH");
    Assert.assertEquals(expResult, result);

    // Simple good case with lower rank
    Group = "DefaultMap";
    searchParameters[0] = "0123456";
    result = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResult.clear();
    expResult.add("OK1");
    expResult.add("OUT2");
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getAllEntries method, of class AbstractRegexMatch.
   */
  @Test
  public void testGetAllEntries()
  {
    System.out.println("getAllEntries DB");

    ArrayList<String> result;
    String expResult;
    String Group;
    int resultCount;

    String[] searchParameters = new String[1];

    // Simple good case
    Group = "DefaultMap";
    searchParameters[0] = "0123456";
    result = instance.getAllEntries(Group, searchParameters);
    resultCount = 2;
    Assert.assertEquals(resultCount, result.size());

    expResult = "OK1";
    Assert.assertEquals(expResult, result.get(0));

    expResult = "OK2";
    Assert.assertEquals(expResult, result.get(1));

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultMap";
    searchParameters[0] = "1";
    result = instance.getAllEntries(Group, searchParameters);
    resultCount = 0;
    Assert.assertEquals(resultCount, result.size());
  }

  /**
   * Test of isValidRegexMatchResult method, of class AbstractRegexMatch.
   */
  @Test
  public void testIsValidRegexMatchResult_ArrayList()
  {
    ArrayList<String> resultToCheck;
    String Group;
    String expResultMatch;
    boolean expResult;
    boolean result;

    System.out.println("isValidRegexMatchResult DB");

    String[] searchParameters = new String[1];

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResultMatch = "NOMATCH";
    Assert.assertEquals(expResultMatch, resultToCheck.get(0));

    // Check that we have the result we need
    expResult = false;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);

    // Simple pass case
    Group = "DefaultMap";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatchWithChildData(Group, searchParameters);
    expResultMatch = "OK2";
    Assert.assertEquals(expResultMatch, resultToCheck.get(0));

    // Check that we have the result we need
    expResult = true;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of isValidRegexMatchResult method, of class AbstractRegexMatch.
   */
  @Test
  public void testIsValidRegexMatchResult_String()
  {
    String resultToCheck;
    String Group;
    String expResultMatch;
    boolean expResult;
    boolean result;

    System.out.println("isValidRegexMatchResult DB");

    String[] searchParameters = new String[1];

    // Simple Assert.fail case because of non matching regex
    Group = "DefaultNOMAP";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatch(Group, searchParameters);
    expResultMatch = "NOMATCH";
    Assert.assertEquals(expResultMatch, resultToCheck);

    // Check that we have the result we need
    expResult = false;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);

    // Simple pass case
    Group = "DefaultMap";
    searchParameters[0] = "0";
    resultToCheck = instance.getRegexMatch(Group, searchParameters);
    expResultMatch = "OK2";
    Assert.assertEquals(expResultMatch, resultToCheck);

    // Check that we have the result we need
    expResult = true;
    result = instance.isValidRegexMatchResult(resultToCheck);
    Assert.assertEquals(expResult, result);
  }

  public class AbstractRegexMatchImpl extends AbstractRegexMatch
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
  *
  * @throws InitializationException
  */
  private void getInstance()
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractRegexMatchDBTest.AbstractRegexMatchImpl();
      
      try
      {
        // Get the instance
        instance.init("DBTestPipe", "AbstractRegexMatchTest");
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
