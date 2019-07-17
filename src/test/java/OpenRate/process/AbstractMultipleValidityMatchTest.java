
package OpenRate.process;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.junit.*;

/**
 *
 * @author tgdspia1
 */
public class AbstractMultipleValidityMatchTest
{
  private static URL FQConfigFileName;

  private static AbstractMultipleValidityMatch instance;

  // Used for logging and exception handling
  private static String message; 
  private static OpenRate appl;

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    FQConfigFileName = new URL("File:src/test/resources/TestMultipleValidityDB.properties.xml");

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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("MultipleValidityMatchTestCache");

    try
    {
    JDBCChcon.prepareStatement("DROP TABLE TEST_MULT_VALIDITY_MATCH;").execute();
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
          message = "Error dropping table TEST_MULT_VALIDITY_MATCH in test <AbstractMultipleValidityMatchTest>.";
          Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_MULT_VALIDITY_MATCH (MAP_GROUP varchar(24), INPUT_VAL varchar(64), START_DATE varchar(14), END_DATE varchar(14), OUTPUT_VAL1 varchar(64), OUTPUT_VAL2 varchar(64));").execute();

    // Create some records in the table
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port1','20120101000000','20120930235959','RESa1_1','RESa1_2');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port1','20120201000000','20121030235959','RESa2_1','RESa2_2');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port1','20120301000000','20121130235959','RESa3_1','RESa3_2');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port1','20120401000000','20121230235959','RESa4_1','RESa4_2');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port2','20120101000000','20120930235959','RESb1_1','RESb1_2');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port2','20120201000000','20121030235959','RESb2_1','RESb2_2');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port2','20120301000000','20121130235959','RESb3_1','RESb3_2');").execute();
    JDBCChcon.prepareStatement("INSERT INTO TEST_MULT_VALIDITY_MATCH (MAP_GROUP,INPUT_VAL,START_DATE,END_DATE,OUTPUT_VAL1,OUTPUT_VAL2) values ('DefaultMap','Port2','20120401000000','20121230235959','RESb4_1','RESb4_2');").execute();

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
   * Test of procHeader method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testProcHeader()
  {
    System.out.println("procHeader");
  }

  /**
   * Test of procTrailer method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testProcTrailer()
  {
    System.out.println("procTrailer");
  }

  /**
   * Test of getFirstValidityMatch method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testGetFirstValidityMatch()
  {
    String result;
    boolean boolExpResult;
    boolean boolResult;
    String expResult;
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("getFirstValidityMatch");

    // Simple good case
    Group = "DefaultMap";
    Resource = "Port1";
    try
    {
      eventDate = sdfEvt.parse("20120101120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getFirstValidityMatch(Group, Resource, eventDate);
    expResult = "RESa1_1";
    Assert.assertEquals(expResult, result);

    // Another Simple good case
    Resource = "Port2";
    result = instance.getFirstValidityMatch(Group, Resource, eventDate);
    expResult = "RESb1_1";
    Assert.assertEquals(expResult, result);

    // Check the validity end date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20121001000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getFirstValidityMatch(Group, Resource, eventDate);
    expResult = "RESa2_1";
    Assert.assertEquals(expResult, result);

    // Bad case, no periods of validity
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20111231235959").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getFirstValidityMatch(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = false;
    Assert.assertEquals(boolExpResult, boolResult);
  }

  /**
   * Test of getFirstValidityMatchWithChildData method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testGetFirstValidityMatchWithChildData()
  {
    boolean boolExpResult;
    boolean boolResult;
    ArrayList<String> result;
    ArrayList<String> expResult = new ArrayList<>();
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("getFirstValidityMatchWithChildData");

    // Simple good case
    Group = "DefaultMap";
    Resource = "Port1";
    try
    {
      eventDate = sdfEvt.parse("20120101120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getFirstValidityMatchWithChildData(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESa1_1");
    expResult.add("RESa1_2");
    Assert.assertEquals(expResult, result);

    // Another Simple good case
    Resource = "Port2";
    result = instance.getFirstValidityMatchWithChildData(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESb1_1");
    expResult.add("RESb1_2");
    Assert.assertEquals(expResult, result);

    // Check the validity end date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20121001000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getFirstValidityMatchWithChildData(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESa2_1");
    expResult.add("RESa2_2");
    Assert.assertEquals(expResult, result);

    // Bad case, no periods of validity
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20111231235959").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getFirstValidityMatchWithChildData(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = false;
    Assert.assertEquals(boolExpResult, boolResult);
  }

  /**
   * Test of getAllValidityMatches method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testGetAllValidityMatches()
  {
    boolean boolExpResult;
    boolean boolResult;
    ArrayList<String> result;
    ArrayList<String> expResult = new ArrayList<>();
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("getAllValidityMatches");

    // Simple good case
    Group = "DefaultMap";
    Resource = "Port1";
    try
    {
      eventDate = sdfEvt.parse("20120601120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getAllValidityMatches(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESa1_1");
    expResult.add("RESa2_1");
    expResult.add("RESa3_1");
    expResult.add("RESa4_1");
    Assert.assertEquals(expResult, result);

    // Another Simple good case
    Resource = "Port2";
    result = instance.getAllValidityMatches(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESb1_1");
    expResult.add("RESb2_1");
    expResult.add("RESb3_1");
    expResult.add("RESb4_1");
    Assert.assertEquals(expResult, result);

    // Check the validity end date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20121001000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getAllValidityMatches(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESa2_1");
    expResult.add("RESa3_1");
    expResult.add("RESa4_1");
    Assert.assertEquals(expResult, result);

    // Check the validity start date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20120201000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getAllValidityMatches(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESa1_1");
    expResult.add("RESa2_1");
    Assert.assertEquals(expResult, result);

    // Check the validity start date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20120301000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getAllValidityMatches(Group, Resource, eventDate);
    expResult.clear();
    expResult.add("RESa1_1");
    expResult.add("RESa2_1");
    expResult.add("RESa3_1");
    Assert.assertEquals(expResult, result);

    // Bad case, no periods of validity
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20111231235959").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getAllValidityMatches(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = false;
    Assert.assertEquals(boolExpResult, boolResult);
  }

  /**
   * Test of getAllValidityMatchesWithChildData method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testGetAllValidityMatchesWithChildData()
  {
    boolean boolExpResult;
    boolean boolResult;
    ArrayList<String> partResult1 = new ArrayList<>();
    ArrayList<String> partResult2 = new ArrayList<>();
    ArrayList<String> partResult3 = new ArrayList<>();
    ArrayList<String> partResult4 = new ArrayList<>();
    ArrayList<ArrayList<String>> result;
    ArrayList<ArrayList<String>> expResult = new ArrayList<>();
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("getAllValidityMatchesWithChildData");

    // Simple good case
    Group = "DefaultMap";
    Resource = "Port1";
    try
    {
      eventDate = sdfEvt.parse("20120601120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    expResult.clear();
    partResult1.clear();
    partResult1.add("RESa1_1");
    partResult1.add("RESa1_2");
    partResult2.clear();
    partResult2.add("RESa2_1");
    partResult2.add("RESa2_2");
    partResult3.clear();
    partResult3.add("RESa3_1");
    partResult3.add("RESa3_2");
    partResult4.clear();
    partResult4.add("RESa4_1");
    partResult4.add("RESa4_2");
    expResult.add(partResult1);
    expResult.add(partResult2);
    expResult.add(partResult3);
    expResult.add(partResult4);
    Assert.assertEquals(expResult, result);

    // Another Simple good case
    Resource = "Port2";
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    expResult.clear();
    partResult1.clear();
    partResult1.add("RESb1_1");
    partResult1.add("RESb1_2");
    partResult2.clear();
    partResult2.add("RESb2_1");
    partResult2.add("RESb2_2");
    partResult3.clear();
    partResult3.add("RESb3_1");
    partResult3.add("RESb3_2");
    partResult4.clear();
    partResult4.add("RESb4_1");
    partResult4.add("RESb4_2");
    expResult.add(partResult1);
    expResult.add(partResult2);
    expResult.add(partResult3);
    expResult.add(partResult4);
    Assert.assertEquals(expResult, result);

    // Check the validity end date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20121001000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    expResult.clear();
    partResult2.clear();
    partResult2.add("RESa2_1");
    partResult2.add("RESa2_2");
    partResult3.clear();
    partResult3.add("RESa3_1");
    partResult3.add("RESa3_2");
    partResult4.clear();
    partResult4.add("RESa4_1");
    partResult4.add("RESa4_2");
    expResult.add(partResult2);
    expResult.add(partResult3);
    expResult.add(partResult4);
    Assert.assertEquals(expResult, result);

    // Check the validity start date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20120201000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    expResult.clear();
    partResult1.clear();
    partResult1.add("RESa1_1");
    partResult1.add("RESa1_2");
    partResult2.clear();
    partResult2.add("RESa2_1");
    partResult2.add("RESa2_2");
    expResult.add(partResult1);
    expResult.add(partResult2);
    Assert.assertEquals(expResult, result);

    // Check the validity start date
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20120301000000").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    Resource = "Port1";
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    expResult.clear();
    partResult1.clear();
    partResult1.add("RESa1_1");
    partResult1.add("RESa1_2");
    partResult2.clear();
    partResult2.add("RESa2_1");
    partResult2.add("RESa2_2");
    partResult3.clear();
    partResult3.add("RESa3_1");
    partResult3.add("RESa3_2");
    expResult.add(partResult1);
    expResult.add(partResult2);
    expResult.add(partResult3);
    Assert.assertEquals(expResult, result);

    // Bad case, no periods of validity
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20111231235959").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = false;
    Assert.assertEquals(boolExpResult, boolResult);
  }

  /**
   * Test of isValidMultipleValidityMatchResult method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testIsValidMultipleValidityMatchResult_List()
  {
    boolean boolExpResult;
    boolean boolResult;
    ArrayList<ArrayList<String>> result;
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("isValidMultipleValidityMatchResult");

    // Simple good case
    Group = "DefaultMap";
    Resource = "Port1";
    try
    {
      eventDate = sdfEvt.parse("20120601120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = true;
    Assert.assertEquals(boolExpResult, boolResult);

    // Bad case, no periods of validity
    Group = "DefaultMap";
    Resource = "Port1";
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20111231235959").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getAllValidityMatchesWithChildData(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = false;
    Assert.assertEquals(boolExpResult, boolResult);
  }

  /**
   * Test of isValidMultipleValidityMatchResult method, of class AbstractMultipleValidityMatch.
   */
  @Test
  public void testIsValidMultipleValidityMatchResult_String()
  {
    String result;
    boolean boolExpResult;
    boolean boolResult;
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("isValidMultipleValidityMatchResult");

    // Simple good case
    Group = "DefaultMap";
    Resource = "Port1";
    try
    {
      eventDate = sdfEvt.parse("20120101120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getFirstValidityMatch(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = true;
    Assert.assertEquals(boolExpResult, boolResult);

    // Bad case, no periods of validity
    try
    {
      // first second after validity ends
      eventDate = sdfEvt.parse("20111231235959").getTime()/1000;
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(message);
    }
    result = instance.getFirstValidityMatch(Group, Resource, eventDate);
    boolResult = instance.isValidMultipleValidityMatchResult(result);
    boolExpResult = false;
    Assert.assertEquals(boolExpResult, boolResult);
  }

  public class AbstractMultipleValidityMatchImpl extends AbstractMultipleValidityMatch
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
      instance = new AbstractMultipleValidityMatchTest.AbstractMultipleValidityMatchImpl();
      
      try
      {
        // Get the instance
        instance.init("DBTestPipe", "AbstractMultipleValidityMatchTest");
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
  }
  
 /**
  * Method to release an instance of the implementation.
  */
  private void releaseInstance()
  {
    instance = null;
  }
}
