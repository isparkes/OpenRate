package OpenRate.process;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the Abstract NP (Number Portability) lookup processing module.
 * 
 * @author ian
 */
public class AbstractValidityFromLookupTest {
  
  private static URL FQConfigFileName;

  private static AbstractValidityFromLookup instance;

  // Used for logging and exception handling
  private static String message; 

  public AbstractValidityFromLookupTest() {
  }
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestNPMatchDB.properties.xml");
    
    // Set up the OpenRate internal logger - this is normally done by app startup
    OpenRate.getApplicationInstance();

    // Load the properties into the OpenRate object
    FrameworkUtils.loadProperties(FQConfigFileName);

    // Get the loggers
    FrameworkUtils.startupLoggers();

    // Get the transaction manager
    FrameworkUtils.startupTransactionManager();
    
    // Get Data Sources
    FrameworkUtils.startupDataSources();
    
    // Get a connection
    Connection JDBCChcon = FrameworkUtils.getDBConnection("NPLookupTestCache");

    try
    {
      JDBCChcon.prepareStatement("DROP TABLE NP_MAP;").execute();
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
        message = "Error dropping table NP_MAP in test <AbstractNPLookupTest>.";
        Assert.fail(message);
      }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE NP_MAP (START_DATE varchar(24),NUMBER varchar(64),DONOR varchar(64),RECIPIENT varchar(64));").execute();

    // Create some records in the table in an ordered way
    JDBCChcon.prepareStatement("INSERT INTO NP_MAP (START_DATE,NUMBER,DONOR,RECIPIENT) VALUES ('01/01/2014 00:00:00','0470000000','BGC','KPNB');").execute();
    JDBCChcon.prepareStatement("INSERT INTO NP_MAP (START_DATE,NUMBER,DONOR,RECIPIENT) VALUES ('01/02/2014 00:00:00','0470000000','BGC','MOBM');").execute();
    JDBCChcon.prepareStatement("INSERT INTO NP_MAP (START_DATE,NUMBER,DONOR,RECIPIENT) VALUES ('01/05/2014 00:00:00','0470000000','BGC','VOXB');").execute();
    
    // Create some records in the table in a disordered way - the cache should order them
    JDBCChcon.prepareStatement("INSERT INTO NP_MAP (START_DATE,NUMBER,DONOR,RECIPIENT) VALUES ('01/02/2014 00:00:00','0470000001','BGC','MOBM');").execute();
    JDBCChcon.prepareStatement("INSERT INTO NP_MAP (START_DATE,NUMBER,DONOR,RECIPIENT) VALUES ('01/05/2014 00:00:00','0470000001','BGC','VOXB');").execute();
    JDBCChcon.prepareStatement("INSERT INTO NP_MAP (START_DATE,NUMBER,DONOR,RECIPIENT) VALUES ('01/01/2014 00:00:00','0470000001','BGC','KPNB');").execute();
    
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
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testSimpleLookupBeforeFirst() {
    System.out.println("testSimpleLookupFirstFound");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000000";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20120101120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);
  }

  /**
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testSimpleLookupFindFirst() {
    System.out.println("testSimpleLookupFindFirst");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000000";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20140101120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "KPNB";
    Assert.assertEquals(expResult, result);
  }

  /**
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testSimpleLookupFindSecond() {
    System.out.println("testSimpleLookupFindSecond");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000000";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20140201120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "MOBM";
    Assert.assertEquals(expResult, result);
  }

  /**
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testSimpleLookupFindLast() {
    System.out.println("testSimpleLookupFindLast");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000000";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20160301120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "VOXB";
    Assert.assertEquals(expResult, result);
  }

  /**
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testUnorderedLookupBeforeFirst() {
    System.out.println("testUnorderedLookupBeforeFirst");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000001";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20120101120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "NOMATCH";
    Assert.assertEquals(expResult, result);
  }

  /**
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testUnorderedLookupFindFirst() {
    System.out.println("testUnorderedLookupFindFirst");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000001";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20140101120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "KPNB";
    Assert.assertEquals(expResult, result);
  }

  /**
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testUnorderedLookupFindSecond() {
    System.out.println("testUnorderedLookupFindSecond");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000001";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20140201120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "MOBM";
    Assert.assertEquals(expResult, result);
  }

  /**
   * In the case where we have multiple entries, check that we get the right one: First.
   */
  @Test
  public void testUnorderedLookupFindLast() {
    System.out.println("testUnorderedLookupFindLast");
    
    String BNumber;
    String result;
    String expResult;
    String Group;
    long eventDate = 0;

    // Simple good case
    Group = "Default";
    BNumber = "0470000001";
    
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    try
    {
      eventDate = sdfEvt.parse("20160301120000").getTime()/1000;
    }
    catch (Exception ex)
    {
      // Not OK, Assert.fail the case
      message = "Error getting event date in test <AbstractNPLookupTest>";
      Assert.fail(message);
    }
    
    result = instance.getValiditySegmentMatch(Group, BNumber, eventDate);
    expResult = "VOXB";
    Assert.assertEquals(expResult, result);
  }

  public class AbstractNPLookupImpl extends AbstractValidityFromLookup
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
  private AbstractValidityFromLookup getInstance()
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractValidityFromLookupTest.AbstractNPLookupImpl();
      
      // Get the instance
      try
      {
        instance.init("DBTestPipe", "AbstractNPLookupTest");
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
