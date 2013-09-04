/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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
    // Deallocate
    OpenRate.getApplicationInstance().cleanup();
    
    // Shut down
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
   * Test of getBestMatch method, of class AbstractBestMatch.
   */
  @Test
  public void testCheckDuplicate()
  {
    String CDRKey;
    Boolean result = null;
    Boolean expResult;
    Date    eventDate = null;
    int     oldTransNum;

    System.out.println("testCheckDuplicate");

    // *************************** In Transaction ******************************
    System.out.println("testCheckDuplicate: In-Transaction Tests");

    // Check that we don't have rows in the table
    Assert.assertEquals(0, getTableRowCount(false));

    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Simple good case - unknown record from cache
    CDRKey = "12334455";

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

    // Check that we don't have rows in the table
    Assert.assertEquals(0, getTableRowCount(true));

    // Close the transaction - this will write the row into the table
    oldTransNum = transNumber;
    transNumber = TransactionUtils.endTransactionPlugIn(instance,transNumber);
    System.out.println("testCheckDuplicate: Closed transaction <" + oldTransNum + ">");

    Assert.assertEquals(1, getTableRowCount(false));

    // *************************** Cross Transaction ***************************
    System.out.println("testCheckDuplicate: Cross-Transaction Tests");

    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Same event again from a new transaction  - should be detected as a duplicate
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

    // *************************** Direct Insert ***************************
    System.out.println("testCheckDuplicate: Direct Insert Tests");

    // Start a new transaction
    transNumber = TransactionUtils.startTransactionPlugIn(instance);
    System.out.println("testCheckDuplicate: Opened transaction <" + transNumber + ">");

    // Same event again from transaction - should be detected as a duplicate
    CDRKey = "12334457";

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

    // same event from new transaction
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
    Assert.assertEquals(3, getTableRowCount(false));

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
  private int getTableRowCount(boolean inTransaction)
  {
    ResultSet result;
    int rowCount = -1;

    // Check that we now have the row in the table - we might have to wait
    // a moment because the transaction closing is asynchronous
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
      result = JDBCChcon.prepareStatement("SELECT COUNT(*) FROM TEST_DUPLICATE_CHECK",ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery();
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
