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
import OpenRate.logging.AbstractLogFactory;
import OpenRate.logging.LogUtil;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.resource.DataSourceFactory;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import OpenRate.transaction.ITransactionManager;
import OpenRate.transaction.TransactionManagerFactory;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import TestUtils.TransactionUtils;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

  private static String cacheDataSourceName;

  private static String resourceName;
  private static String tmpResourceClassName;
  private static ResourceContext ctx = new ResourceContext();
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
    Class<?>          ResourceClass;
    IResource         Resource;

    FQConfigFileName = new URL("File:src/test/resources/TestDB.properties.xml");
    
    // Get a properties object
    try
    {
        PropertyUtils.getPropertyUtils().loadPropertiesXML(FQConfigFileName,"FWProps");
    }
    catch (InitializationException ex)
    {
        message = "Error reading the configuration file <" + FQConfigFileName + ">";
        Assert.fail(message);
    }

      // Set up the OpenRate internal logger - this is normally done by app startup
      OpenRate.getApplicationInstance();
      
    // Get the data source name
    cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef("CacheFactory",
                                                                                        "DuplicateCheckTestCache",
                                                                                        "DataSource",
                                                                                        "None");

    // Get a logger
    System.out.println("  Initialising Logger Resource...");
    resourceName         = "LogFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(AbstractLogFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
    System.out.println("  Initialised Logger Resource");

    // Get a data Source factory
    System.out.println("  Initialising Data Source Factory Resource...");
    resourceName         = "DataSourceFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(DataSourceFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
    System.out.println("  Initialised Data Source Factory Resource");

    // The datasource property was added to allow database to database
    // JDBC adapters to work properly using 1 configuration file.
    System.out.println("  Initialising Data Source...");
    if(DBUtil.initDataSource(cacheDataSourceName) == null)
    {
        message = "Could not initialise DB connection <" + cacheDataSourceName + "> in test <AbstractDuplicateCheckTest>.";
        Assert.fail(message);
    }
    System.out.println("  Initialised Data Source");

    // Get a connection
    Connection JDBCChcon = DBUtil.getConnection(cacheDataSourceName);

    try
    {
        JDBCChcon.prepareStatement("DROP TABLE TEST_DUPLICATE_CHECK;").execute();
    }
    catch (Exception ex)
    {
        if (ex.getMessage().startsWith("Unknown table"))
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
    JDBCChcon.prepareStatement("CREATE TABLE TEST_DUPLICATE_CHECK (CDR_KEY varchar(64),CDR_DATE timestamp);").execute();

    // Create the test table unique index
    JDBCChcon.prepareStatement("ALTER TABLE TEST_DUPLICATE_CHECK ADD UNIQUE INDEX idx_cdr_key (CDR_KEY);").execute();
    System.out.println("    Created Test Table and Index...");

    // Get a cache factory
    System.out.println("  Initialising Cache Factory Resource...");
    resourceName         = "CacheFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(CacheFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);

    // Get a transaction manager
    System.out.println("  Initialising Transaction Manager Resource...");
    resourceName         = "TransactionManagerFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(TransactionManagerFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);

      // Link the logger
      OpenRate.getApplicationInstance().setFwLog(LogUtil.getLogUtil().getLogger("Framework"));
      OpenRate.getApplicationInstance().setStatsLog(LogUtil.getLogUtil().getLogger("Statistics"));
      
      // Get the list of pipelines we are going to make
      ArrayList<String> pipelineList = PropertyUtils.getPropertyUtils().getGenericNameList("PipelineList");
      
      // Create the pipeline skeleton instance (assume only one for tests)
      OpenRate.getApplicationInstance().createPipeline(pipelineList.get(0));      
      
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
    TM.resetClients();
  }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

  /**
   * Test of init method, of class AbstractBestMatch.
   *
   * @throws Exception
   */
  @Test
  public void testInit() throws Exception
  {
    System.out.println("init");

    // get the instance
    getInstance();
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
      Connection JDBCChcon = DBUtil.getConnection(cacheDataSourceName);
      result = JDBCChcon.prepareStatement("SELECT COUNT(*) FROM TEST_DUPLICATE_CHECK;").executeQuery();
      result.first();
      rowCount = result.getInt(1);
      DBUtil.close(JDBCChcon);
    } catch (InitializationException | SQLException ex) {
      // Not OK, Assert.fail the case
      message = "Error counting rows in test <AbstractDuplicateCheckTest>, message <"+ex.getMessage()+">";
      Assert.fail(message);
    }

    return rowCount;
  }
}
