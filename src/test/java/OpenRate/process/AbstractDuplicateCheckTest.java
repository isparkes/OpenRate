
package OpenRate.process;

import OpenRate.OpenRate;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import OpenRate.transaction.ITransactionManager;
import OpenRate.transaction.TransactionManagerFactory;
import OpenRate.utils.ConversionUtils;
import TestUtils.FrameworkUtils;
import TestUtils.TransactionUtils;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import org.junit.*;

/**
 * Unit test for AbstractBestMatch.
 *
 * @author tgdspia1
 */
public class AbstractDuplicateCheckTest
{
  private static URL FQConfigFileName;

  private static AbstractDuplicateCheck instance;
  private static ITransactionManager TM;
  private static int transNumber;

  // Used for logging and exception handling
  private static String message; 
  private static OpenRate appl;

 /**
  * Default constructor
  */
  public AbstractDuplicateCheckTest()
  {
    // Not used
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    FQConfigFileName = new URL("File:src/test/resources/TestDuplicate.properties.xml");
    
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
    Connection JDBCChcon = FrameworkUtils.getDBConnection("DuplicateCheckTestCache");

    try
    {
        JDBCChcon.prepareStatement("DROP TABLE TEST_DUPLICATE_CHECK;").execute();
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
        message = "Error dropping table TEST_DUPLICATE_CHECK in test <AbstractDuplicateCheckTest>.";
        Assert.fail(message);
        }
    }

    // Create the test table
    JDBCChcon.prepareStatement("CREATE TABLE TEST_DUPLICATE_CHECK (CDR_KEY varchar(64),CDR_DATE timestamp)").execute();

    // Create the test table unique index
    //JDBCChcon.prepareStatement("ALTER TABLE TEST_DUPLICATE_CHECK ADD UNIQUE INDEX idx_cdr_key (CDR_KEY)").execute();
    JDBCChcon.prepareStatement("ALTER TABLE TEST_DUPLICATE_CHECK ADD CONSTRAINT idx_cdr_key UNIQUE (CDR_KEY)").execute();
    System.out.println("    Created Test Table and Index...");

    // Get the caches that we are using
    FrameworkUtils.startupCaches();
          
    System.out.println("  Sleeping for 1S to allow things to settle...");
    try {
        Thread.sleep(1000);
    } catch (InterruptedException ex) {
    }

    // Check that we now have the row in the table - we might have to wait
    // a moment because the transaction closing is asynchronous
    TM = TransactionUtils.getTM();
  }

  @AfterClass
  public static void tearDownClass(){
    OpenRate.getApplicationInstance().finaliseApplication();
  }

    @Before
    public void setUp() {
    // get the instance
    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      message = "Error getting cache instance in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }
    }

    @After
    public void tearDown() {
    // release the instance
    try
    {
      releaseInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      message = "Error releasing cache instance in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }
    }

  /**
   * Test of duplicate check cache module, using standard in-transaction
   * checking: two events in the same transaction are duplicates.
   */
  @Test
  public void testCheckDuplicateInTransaction()
  {
    String  CDRKey;
    Boolean result = null;
    Boolean expResult;
    Date    eventDate = null;
    int     oldTransNum;

    System.out.println("testCheckDuplicateInTransaction");

    // *************************** In Transaction ******************************
    // Simple good case - unknown record from cache
    CDRKey = "12334455";

    // Check that we don't have rows in the table
    Assert.assertEquals(0, getTableRowCount(false, CDRKey));

    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    try
    {
      result = instance.CheckDuplicate(eventDate,CDRKey);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = false;
    Assert.assertEquals(expResult, result);

    // Same event again in the same transaction - should be detected as a duplicate
    try
    {
      result = instance.CheckDuplicate(eventDate,CDRKey);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = true;
    Assert.assertEquals(expResult, result);

    // Check that we don't have rows in the table until we commit
    Assert.assertEquals(0, getTableRowCount(true, CDRKey));

    // Close the transaction - this will write the row into the table
    oldTransNum = transNumber;
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNumber);
    System.out.println("testCheckDuplicate: Closed transaction <" + oldTransNum + ">");

    // Check that we find the row
    Assert.assertEquals(1, getTableRowCount(false, CDRKey));
    
    // We have to wait for all flushing to be finished before we can move onto the next test
    while (TransactionUtils.getOpenTransactionCount() > 0)
    {
      System.out.println("  Sleeping for 100mS to allow <"+TransactionUtils.getOpenTransactionCount()+"> transaction operations to close...");
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }    
  }

  /**
   * Test of the performance of the duplicate check cache module, during the 
   * checking and the commit phases. We expect hundreds of thousands per
   * second for the check and tens of thousands for the commit on the test
   * equipment. Of course, this can be improved by tuning.
   */
  @Test
  public void testCheckDuplicatePerformance()
  {
    long    CDRKeyCounter;
    Boolean result = null;
    Boolean expResult;
    Date    eventDate = null;
    int     oldTransNum;

    System.out.println("testCheckDuplicatePerformance");

    // *************************** Performance ******************************
    // Simple good case - unknown record from cache
    CDRKeyCounter = 31123455;
    
    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    long startTimeChk = Calendar.getInstance().getTimeInMillis();
    
    // load some events into the transaction
    for (int idx = 0 ; idx < 10000; idx++)
    {
      try
      {
        result = instance.CheckDuplicate(eventDate,"Perf_" + CDRKeyCounter+idx);
      }
      catch (ProcessingException ex)
      {
        message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
        Assert.fail(message);
      }

      expResult = false;
      Assert.assertEquals(expResult, result);
    }

    long endTimeChk = Calendar.getInstance().getTimeInMillis();
    
    System.out.println("Duplicate Check Performance 10000 events in " +(endTimeChk - startTimeChk) + " mS");
    
    if ((endTimeChk - startTimeChk) > 1000)
    {
      Assert.fail("Performance too low on check");
    }
    
    // Close the transaction - this will write the row into the table
    oldTransNum = transNumber;
    long startTimeCmt = Calendar.getInstance().getTimeInMillis();
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNumber);
    System.out.println("testCheckDuplicate: Closed transaction <" + oldTransNum + ">");

    // We have to wait for all flushing to be finished before we can move onto the next test
    while (TransactionUtils.getOpenTransactionCount() > 0)
    {
      System.out.println("  Sleeping for 100mS to allow <"+TransactionUtils.getOpenTransactionCount()+"> transaction operations to close...");
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }    
    long endTimeCmt = Calendar.getInstance().getTimeInMillis();
    
    System.out.println("Duplicate Commit Performance 10000 events in " +(endTimeCmt - startTimeCmt) + " mS");
    
    if ((endTimeCmt - startTimeCmt) > 5000)
    {
      Assert.fail("Performance too low on commit");
    }
    
  }

  /**
   * Test of duplicate check cache module, using standard cross transaction
   * checking. Two events from different transactions are duplicates, while
   * other events are not. This tests the underlying key merge algorithm.
   */
  @Test
  public void testCheckDuplicateCrossTransaction()
  {
    String CDRKey;
    Boolean result = null;
    Boolean expResult;
    Date    eventDate = null;
    int     oldTransNum;

    System.out.println("testCheckDuplicateCrossTransaction");

    // *************************** Cross Transaction ***************************
    // Simple good case - unknown record from cache
    CDRKey = "12334459";

    // Check that we don't have rows in the table
    Assert.assertEquals(0, getTableRowCount(false, CDRKey));

    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }
    
    // First event from first transaction  - should not be detected as a duplicate
    try
    {
      result = instance.CheckDuplicate(eventDate,CDRKey);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = false;
    Assert.assertEquals(expResult, result);

    // Check that we don't have rows in the table until we commit
    Assert.assertEquals(0, getTableRowCount(true, CDRKey));

    // Close the transaction - this will write the row into the table
    oldTransNum = transNumber;
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNumber);
    System.out.println("testCheckDuplicate: Closed transaction <" + oldTransNum + ">");

    // Check that we find the row
    Assert.assertEquals(1, getTableRowCount(false, CDRKey));
    
    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Same event from a new transaction  - should be detected as a duplicate
    try
    {
      result = instance.CheckDuplicate(eventDate,CDRKey);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = true;
    Assert.assertEquals(expResult, result);

    // new event from new transaction
    CDRKey = "12334456";
    
    try
    {
      result = instance.CheckDuplicate(eventDate,CDRKey);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = false;
    Assert.assertEquals(expResult, result);

    // Close the transaction
    oldTransNum = transNumber;
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNumber);
    System.out.println("testCheckDuplicate: Closed transaction <" + oldTransNum + ">");
    
    // We have to wait for all flushing to be finished before we can move onto the next test
    while (TransactionUtils.getOpenTransactionCount() > 0)
    {
      System.out.println("  Sleeping for 100mS to allow <"+TransactionUtils.getOpenTransactionCount()+"> transaction operations to close...");
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }    
  }

  /**
   * Test of duplicate check commit locking. When two transactions commit at the
   * same time, we must impose locking during the commit to ensure that we
   * don't miss any events, or corrupt the central merge table.
   */
  @Test
  public void testCheckDuplicateCommitLocking()
  {
    long    CDRKeyCounter;
    Boolean result = null;
    Boolean expResult;
    Date    eventDate = null;
    int     transNum1;
    int     transNum2;

    System.out.println("testCheckDuplicateCommitLocking");

    // *************************** First transaction ******************************
    // Simple good case - unknown record from cache
    CDRKeyCounter = 71123455;
    
    // Start a new transaction
    transNum1 = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened Concurrent transaction <" + transNum1 + ">");

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    // load some events into the transaction
    for (int idx = 0 ; idx < 50000; idx++)
    {
      try
      {
        result = instance.CheckDuplicate(eventDate,"Perf_" + CDRKeyCounter+idx);
      }
      catch (ProcessingException ex)
      {
        message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
        Assert.fail(message);
      }

      expResult = false;
      Assert.assertEquals(expResult, result);
    }

    // Simple good case - unknown record from cache
    CDRKeyCounter = 72123455;
    
    // *************************** Second transaction ******************************
    // Start a new transaction
    transNum2 = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened Concurrent transaction <" + transNum2 + ">");

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    // load some events into the transaction
    for (int idx = 0 ; idx < 50000; idx++)
    {
      try
      {
        result = instance.CheckDuplicate(eventDate,"Perf_" + CDRKeyCounter+idx);
      }
      catch (ProcessingException ex)
      {
        message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
        Assert.fail(message);
      }

      expResult = false;
      Assert.assertEquals(expResult, result);
    }
    
    // *************************** close transactions ******************************
    // get the count before
    int countBefore = getTableRowCount(true);
    
    // Close the first transaction - this will write the row into the table
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNum1);
    System.out.println("testCheckDuplicate: Closed Concurrent transaction <" + transNum1 + ">");
    
    // Close the second transaction - this will write the row into the table
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNum2);
    System.out.println("testCheckDuplicate: Closed Concurrent transaction <" + transNum2 + ">");

    // get the count after the transactions have closed - waits for the transaction to close
    int countAfter = getTableRowCount(false);
    
    // Check that all were inserted properly
    Assert.assertEquals(100000, countAfter-countBefore);
  }

  /**
   * Test of duplicate check commit locking. We are testing that should a
   * duplicate arrive in two transactions at the same time, that we will accept
   * them both, but write only once.
   */
  @Test
  public void testCheckDuplicateDuplicateDuringCommit()
  {
    long    CDRKeyCounter;
    Boolean result = null;
    Boolean expResult;
    Date    eventDate = null;
    int     transNum1;
    int     transNum2;

    System.out.println("testCheckDuplicateCommitLocking");

    // *************************** First transaction ******************************
    // Simple good case - unknown record from cache
    CDRKeyCounter = 71123455;
    
    // Start a new transaction
    transNum1 = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened Concurrent transaction <" + transNum1 + ">");

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    // load an eventinto the transaction
    try
    {
      result = instance.CheckDuplicate(eventDate,"Lock_" + CDRKeyCounter);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = false;
    Assert.assertEquals(expResult, result);

    // *************************** Second transaction ******************************
    // Start a new transaction
    transNum2 = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened Concurrent transaction <" + transNum2 + ">");

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    // load some events into the transaction
    try
    {
      result = instance.CheckDuplicate(eventDate,"Lock_" + CDRKeyCounter);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = false;
    Assert.assertEquals(expResult, result);
    
    // *************************** close transactions ******************************
    // get the count before
    int countBefore = getTableRowCount(true);
    
    // Close the first transaction - this will write the row into the table
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNum1);
    System.out.println("testCheckDuplicate: Closed Concurrent transaction <" + transNum1 + ">");
    
    // Close the second transaction - this will write the row into the table
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNum2);
    System.out.println("testCheckDuplicate: Closed Concurrent transaction <" + transNum2 + ">");
    
    // get the count after the transactions have closed - waits for the transaction to close
    int countAfter = getTableRowCount(false);
    
    // Check that all were inserted properly - only 1
    Assert.assertEquals(1, countAfter-countBefore);
  }

  /**
   * Test of getBestMatch method, of class AbstractBestMatch.
   */
  @Test
  public void testCheckDuplicateDirectInsert()
  {
    String CDRKey;
    Boolean result = null;
    Boolean expResult;
    Date    eventDate = null;
    int     oldTransNum;

    System.out.println("testCheckDuplicateDirectInsert");

    // *************************** Direct Insert ***************************
    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Same event again from transaction - should be detected as a duplicate
    CDRKey = "12334457";

    // Check the validity end date
    try
    {
      // Current date (duplicate check uses relative dates to system time)
      eventDate = new Date();
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }
    
    // Check the validity end date
    try
    {
      // 100 days ago (buffer limit is 90 days)
      eventDate = ConversionUtils.getConversionUtilsObject().addDateSeconds(eventDate, -100*86400);
    }
    catch (Exception ex)
    {
      message = "Error getting event date in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    try
    {
      result = instance.CheckDuplicate(eventDate,CDRKey);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = false;
    Assert.assertEquals(expResult, result);

    // same event from same transaction
    try
    {
      result = instance.CheckDuplicate(eventDate,CDRKey);
    }
    catch (ProcessingException ex)
    {
      message = "Unexpected processing exception in test <AbstractDuplicateCheckTest>";
      Assert.fail(message);
    }

    expResult = true;
    Assert.assertEquals(expResult, result);

    // Close the transaction
    oldTransNum = transNumber;
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNumber);
    System.out.println("testCheckDuplicate: Closed transaction <" + oldTransNum + ">");

    // Check that we still have the row in the table, no more no less
    Assert.assertEquals(1, getTableRowCount(false, CDRKey));
    
    // We have to wait for all flushing to be finished before we can move onto the next test
    while (TransactionUtils.getOpenTransactionCount() > 0)
    {
      System.out.println("  Sleeping for 100mS to allow <"+TransactionUtils.getOpenTransactionCount()+"> transaction operations to close...");
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }    
  }
  
  /**
   * Stub out the calls to the implementation processing - we don't need these
   * for unit testing.
   */
  public class AbstractDuplicateCheckImpl extends AbstractDuplicateCheck
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
  * Get the row count from the table. Waits for transaction to end if needed.
  * If we indicate that we are in a transaction, we just get the value.
  *
  * @return
  */
  private int getTableRowCount(boolean inTransaction, String cdrKey)
  {
    ResultSet result;
    int rowCount = -1;

    // Check the number of rows in the table
    TM = TransactionUtils.getTM();

    if (inTransaction == false)
    {
      while (TransactionUtils.getOpenTransactionCount() > 0)
      {
        System.out.println("  Sleeping for 100mS to allow <"+TransactionUtils.getOpenTransactionCount()+"> transactions to close...");
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
        }
      }
    }

    try
    {
      // Get a connection
      Connection JDBCChcon = FrameworkUtils.getDBConnection("DuplicateCheckTestCache");
      result = JDBCChcon.prepareStatement("SELECT COUNT(*) FROM TEST_DUPLICATE_CHECK where CDR_KEY = '" + cdrKey + "'",ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery();
      result.next();
      rowCount = result.getInt(1);
      DBUtil.close(JDBCChcon);
    } catch (InitializationException | SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
      // Not OK, Assert.fail the case
      message = "Error counting rows in test <AbstractDuplicateCheckTest>, message <"+ex.getMessage()+">";
      Assert.fail(message);
    }

    return rowCount;
  }

 /**
  * Get the row count from the table. Waits for transaction to end if needed.
  * If we indicate that we are in a transaction, we just get the value.
  *
  * @return
  */
  private int getTableRowCount(boolean inTransaction)
  {
    ResultSet result;
    int rowCount = -1;

    // Check the number of rows in the table
    TM = TransactionUtils.getTM();

    if (inTransaction == false)
    {
      while (TransactionUtils.getOpenTransactionCount() > 0)
      {
        System.out.println("  Sleeping for 100mS to allow <"+TransactionUtils.getOpenTransactionCount()+"> transactions to close...");
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
        }
      }
    }

    try
    {
      // Get a connection
      Connection JDBCChcon = FrameworkUtils.getDBConnection("DuplicateCheckTestCache");
      result = JDBCChcon.prepareStatement("SELECT COUNT(*) FROM TEST_DUPLICATE_CHECK", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery();
      result.next();
      rowCount = result.getInt(1);
      DBUtil.close(JDBCChcon);
    } catch (InitializationException | SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
      // Not OK, Assert.fail the case
      message = "Error counting rows in test <AbstractDuplicateCheckTest>, message <"+ex.getMessage()+">";
      Assert.fail(message);
    }

    return rowCount;
  }

 /**
  * Method to get an instance of the implementation. Done this way to allow
  * tests to be executed individually.
  *
  * @throws InitializationException
  */
  private void getInstance() throws InitializationException
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractDuplicateCheckTest.AbstractDuplicateCheckImpl();

      // Get the instance
      instance.init("DBTestPipe", "AbstractDuplicateCheckTest");

      while (TransactionManagerFactory.getTransactionManager("DBTestPipe") == null)
      {
        System.out.println("  Sleeping for 100mS to allow transaction manager to settle...");
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
        }
      }
    }
    else
    {
      Assert.fail("Instance already allocated");
    }
  }

 /**
  * Method to get an instance of the implementation. Done this way to allow
  * tests to be executed individually.
  *
  * @throws InitializationException
  */
  private void releaseInstance() throws InitializationException
  {
    TransactionManagerFactory.getTransactionManager("DBTestPipe").close();
    
    instance = null;
  }
}
