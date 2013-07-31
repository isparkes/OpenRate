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
import OpenRate.buffer.IConsumer;
import OpenRate.buffer.ISupplier;
import OpenRate.db.DBUtil;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.lang.BalanceGroup;
import OpenRate.lang.Counter;
import OpenRate.lang.DiscountInformation;
import OpenRate.logging.AbstractLogFactory;
import OpenRate.logging.LogUtil;
import OpenRate.record.BalanceImpact;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.resource.DataSourceFactory;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import OpenRate.transaction.ITransactionManager;
import OpenRate.transaction.TransactionManagerFactory;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import TestUtils.TestRatingRecord;
import TestUtils.TransactionUtils;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import org.junit.*;

/**
 * Unit Test for the balance handler processing plug in. This test builds the
 * test environment step by step (meaning that we create all of the pieces
 * manually). There is also the possibility to perform the same test using 
 * the automatic application creation).
 *
 * @author TGDSPIA1
 */
public class AbstractBalanceHandlerPlugInTest implements IPlugIn
{
  private static URL FQConfigFileName;

  private static String cacheDataSourceName;
  private static String resourceName;
  private static String tmpResourceClassName;
  private static ResourceContext ctx = new ResourceContext();
  private static AbstractBalanceHandlerPlugIn instance;
  private static ITransactionManager TM;
  
  // Used for logging and exception handling
  private static String message; 

    public AbstractBalanceHandlerPlugInTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
    Class<?>          ResourceClass;
    IResource         Resource;
    
    FQConfigFileName = new URL("File:src/test/resources/TestBalanceHandler.properties.xml");
    
      // Get a properties object
      try
      {
        PropertyUtils.getPropertyUtils().loadPropertiesXML(FQConfigFileName,"FWProps");
      }
      catch (InitializationException ex)
      {
        message = "Error reading the configuration file <" + FQConfigFileName + ">" + System.getProperty("user.dir");
        Assert.fail(message);
      }

      // Set up the OpenRate internal logger - this is normally done by app startup
      OpenRate.getApplicationInstance();
      
      // Get the data source name
      cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef("CacheFactory",
                                                                                          "BalCache",
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
      
      // set the log - this is normally done by application startup
      OpenRate.getApplicationInstance().setFwLog(LogUtil.getLogUtil().getLogger("Framework"));

      // Get a data Source factory
      System.out.println("  Initialising Data Source Resource...");
      resourceName         = "DataSourceFactory";
      tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(DataSourceFactory.RESOURCE_KEY,"ClassName");
      ResourceClass        = Class.forName(tmpResourceClassName);
      Resource             = (IResource)ResourceClass.newInstance();
      Resource.init(resourceName);
      ctx.register(resourceName, Resource);
      
      // The datasource property was added to allow database to database
      // JDBC adapters to work properly using 1 configuration file.
      if(DBUtil.initDataSource(cacheDataSourceName) == null)
      {
        message = "Could not initialise DB connection <" + cacheDataSourceName + "> in test <AbstractBestMatchTest>.";
        Assert.fail(message);
      }

      // Get a connection
      Connection JDBCChcon = DBUtil.getConnection(cacheDataSourceName);

      try
      {
        JDBCChcon.prepareStatement("DROP TABLE TEST_COUNTER_BALS;").execute();
      }
      catch (Exception ex)
      {
        if (ex.getMessage().startsWith("Unknown table"))
        {
          // It's OK
        }
        else
        {
          // Not OK, fail the case
          message = "Error dropping table TEST_COUNTER_BALS in test <AbstractBalanceHandlerPlugInTest>.";
          Assert.fail(message);
        }
      }

      // Create the test table
      JDBCChcon.prepareStatement("CREATE TABLE TEST_COUNTER_BALS (BALANCE_GROUP int NOT NULL, COUNTER_ID int NOT NULL, RECORD_ID int NOT NULL, VALID_FROM int NOT NULL, VALID_TO int NOT NULL, CURRENT_BAL double);").execute();

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
      
      // Check that we now have the row in the table - we might have to wait
      // a moment because the transaction closing is asynchronous
      TM = TransactionUtils.getTM();
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
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
     * Test of getBalanceGroup and addBalanceGroup method, of class
     * AbstractBalanceHandlerPlugIn.
     */
    @Test
    public void testBalanceGroup()
    {
        System.out.println("getBalanceGroup");

        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
          // Not OK, Assert.fail the case
          message = "Error getting cache instance in test <AbstractBalanceHandlerPlugInTest>";
          Assert.fail(message);
        }

        long BalanceGroupId = 12345L;

        // Balance group does not exist
        BalanceGroup expResult = null;
        BalanceGroup result = instance.getBalanceGroup(BalanceGroupId);
        Assert.assertEquals(expResult, result);

        // Create the balance group - check we get something back
        result = instance.addBalanceGroup(BalanceGroupId);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.getRecId());

        // Check we can get it back when we want
        result = instance.getBalanceGroup(BalanceGroupId);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.getRecId());
    }

    /**
     * Test of checkCounterExists,addCounter and getCounter methods, of class
     * AbstractBalanceHandlerPlugIn.
     */
    @Test
    public void testCheckBasicCounterManagement()
    {
        System.out.println("checkBasicCounterManagement");

        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
        // Not OK, Assert.fail the case
        message = "Error getting cache instance in test <AbstractBalanceHandlerPlugInTest>";
        Assert.fail(message);
        }

        long BalanceGroupId = 23456L;
        int CounterId = 1000;
        long UTCEventDate;
        long ValidFrom;
        long ValidTo;

        // set up the dates - create a balance valid for 10 seconds
        UTCEventDate = Calendar.getInstance().getTimeInMillis()/1000;
        ValidFrom = UTCEventDate;
        ValidTo = UTCEventDate + 10;

        // Create a balance group
        instance.addBalanceGroup(BalanceGroupId);

        // check if we find it - we should not
        Counter expResult = null;
        Counter result = instance.checkCounterExists(BalanceGroupId, CounterId, UTCEventDate);
        Assert.assertEquals(expResult, result);

        // Add the counter - give it an initial balance of 10
        double expCounterBal = 10;
        result = instance.addCounter(BalanceGroupId, CounterId, ValidFrom, ValidTo, 10);
        Assert.assertEquals(expCounterBal, result.CurrentBalance, 0.00001);

        // get the counter
        expResult = new Counter();
        expResult.CurrentBalance = 10;
        expResult.RecId = 1;
        expResult.validFrom = ValidFrom;
        expResult.validTo = ValidTo;
        result = instance.getCounter(BalanceGroupId, CounterId, UTCEventDate);
        Assert.assertEquals(expResult.CurrentBalance, result.CurrentBalance, 0.00001);
        Assert.assertEquals(expResult.RecId, result.RecId);
        Assert.assertEquals(expResult.validFrom, result.validFrom);
        Assert.assertEquals(expResult.validTo, result.validTo);

        // Now it should be expired
        expResult = null;
        result = instance.getCounter(BalanceGroupId, CounterId, UTCEventDate + 10);
        Assert.assertEquals(expResult, result);

        // Create a new one
        expCounterBal = 11;
        result = instance.addCounter(BalanceGroupId, CounterId, ValidFrom+10, ValidTo+10, 11);
        Assert.assertEquals(expCounterBal, result.CurrentBalance, 0.00001);

        // Get the first counter
        expCounterBal = 10;
        result = instance.getCounter(BalanceGroupId, CounterId, UTCEventDate);
        Assert.assertEquals(expCounterBal, result.CurrentBalance, 0.00001);

        // and the second counter
        expCounterBal = 11;
        result = instance.getCounter(BalanceGroupId, CounterId, UTCEventDate+10);
        Assert.assertEquals(expCounterBal, result.CurrentBalance, 0.00001);
        Assert.assertEquals(2, result.RecId);

        // and a non existent counter
        result = instance.getCounter(BalanceGroupId, CounterId, UTCEventDate+20);
        Assert.assertEquals(expResult, result);

        // Try to add a counter over the top of existing ones
        result = instance.addCounter(BalanceGroupId, CounterId, ValidFrom+5, ValidTo+5, 12);
        Assert.assertEquals(3, result.RecId);
        Assert.assertEquals(12, result.CurrentBalance, 0.00001);

        // Now try to get the overlaid balances back - we should bet the first
        expCounterBal = 10;
        result = instance.getCounter(BalanceGroupId, CounterId, UTCEventDate+6);
        Assert.assertEquals(expCounterBal, result.CurrentBalance, 0.00001);
        Assert.assertEquals(1, result.RecId);
    }

    /**
     * Test of discountConsumeRUM method, of class AbstractBalanceHandlerPlugIn.
     */
    @Test
    public void testDiscountConsumeRUM()
    {
        System.out.println("discountConsumeRUM");

        // get the instance
        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
          // Not OK, Assert.fail the case
          message = "Error getting cache instance in test <AbstractBalanceHandlerPlugInTest>";
          Assert.fail(message);
        }

        // Set up the record
        TestRatingRecord CurrentRecord1 = new TestRatingRecord();
        TestRatingRecord CurrentRecord2 = new TestRatingRecord();
        TestRatingRecord CurrentRecord3 = new TestRatingRecord();

        String DiscountName = "TestDiscount";
        long BalanceGroupId = 1000L;
        String RUMToUse = "RUM";
        int counterId = 100000;
        double initialBalance = 200.0;
        double rumValue1 = 123.0;
        long UTCBalanceStartValidity = ConversionUtils.getConversionUtilsObject().getUTCDayStart(new Date());
        long UTCBalanceEndValidity = ConversionUtils.getConversionUtilsObject().getUTCDayEnd(new Date());
        CurrentRecord1.setRUMValue("RUM", rumValue1);
        CurrentRecord2.setRUMValue("RUM", rumValue1);
        CurrentRecord3.setRUMValue("RUM", rumValue1);
        CurrentRecord1.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecord2.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecord3.UTCEventDate = UTCBalanceStartValidity;

        // ---------- First Event Fully Discounted ------------
        DiscountInformation result = instance.discountConsumeRUM(CurrentRecord1, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals((initialBalance-rumValue1), result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(rumValue1, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(true, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_FULLY_DISCOUNTED, result.getDiscountFlag());

        // Check the RUM value has been used up
        Assert.assertEquals(0,CurrentRecord1.getRUMValue("RUM"),0.000001);

        // Check the balance impacts in the record
        Assert.assertEquals(2,CurrentRecord1.getBalanceImpactCount());
        BalanceImpact balImp1 = CurrentRecord1.getBalanceImpact(0);
        Assert.assertEquals("CREATION",balImp1.ruleName);
        Assert.assertEquals("RUM",balImp1.rumUsed);
        Assert.assertEquals(200,balImp1.balanceDelta,0.000001);
        Assert.assertEquals(200,balImp1.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp1.counterID);
        Assert.assertEquals(1,balImp1.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp1.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp1.endDate);
        Assert.assertEquals(0,balImp1.rumValueUsed,0.000001);
        Assert.assertEquals(0,balImp1.rumValueAfter,0.000001);
        BalanceImpact balImp2 = CurrentRecord1.getBalanceImpact(1);
        Assert.assertEquals("ConsumeRUM",balImp2.ruleName);
        Assert.assertEquals("RUM",balImp2.rumUsed);
        Assert.assertEquals(-123,balImp2.balanceDelta,0.000001);
        Assert.assertEquals(77,balImp2.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp2.counterID);
        Assert.assertEquals(1,balImp2.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp2.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp2.endDate);
        Assert.assertEquals(123,balImp2.rumValueUsed,0.000001);
        Assert.assertEquals(0,balImp2.rumValueAfter,0.000001);

        // ---------- Second Event Partially Discounted ------------
        result = instance.discountConsumeRUM(CurrentRecord2, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals(0, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(77, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_PARTIALLY_DISCOUNTED, result.getDiscountFlag());

        // Check the RUM value has been partially used up
        Assert.assertEquals(46,CurrentRecord2.getRUMValue("RUM"),0.000001);

        // Check the balance impacts in the record
        Assert.assertEquals(1,CurrentRecord2.getBalanceImpactCount());
        BalanceImpact balImp3 = CurrentRecord2.getBalanceImpact(0);
        Assert.assertEquals("ConsumeRUM",balImp3.ruleName);
        Assert.assertEquals("RUM",balImp3.rumUsed);
        Assert.assertEquals(-77,balImp3.balanceDelta,0.000001);
        Assert.assertEquals(0,balImp3.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp3.counterID);
        Assert.assertEquals(1,balImp3.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp3.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp3.endDate);
        Assert.assertEquals(77,balImp3.rumValueUsed,0.000001);
        Assert.assertEquals(46,balImp3.rumValueAfter,0.000001);

        // ---------- Third Event not Discounted ------------
        result = instance.discountConsumeRUM(CurrentRecord1, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(false, result.isDiscountApplied());
        Assert.assertEquals(0, result.getCounterId());
        Assert.assertEquals(0, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(0, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(0, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_NO_DISCOUNT, result.getDiscountFlag());

        // Check the RUM value has been used up
        Assert.assertEquals(123,CurrentRecord3.getRUMValue("RUM"),0.000001);
    }

    /**
     * Test of refundConsumeRUM method, of class AbstractBalanceHandlerPlugIn.
     */
    @Test
    public void testRefundConsumeRUM()
    {
        System.out.println("refundConsumeRUM");

        // get the instance
        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
          // Not OK, Assert.fail the case
          message = "Error getting cache instance in test <AbstractBalanceHandlerPlugInTest>";
          Assert.fail(message);
        }

        // Set up the records for impacting
        TestRatingRecord CurrentRecord1 = new TestRatingRecord();
        TestRatingRecord CurrentRecord2 = new TestRatingRecord();
        TestRatingRecord CurrentRecord3 = new TestRatingRecord();

        // Set up the records for refunding
        TestRatingRecord CurrentRecordR1 = new TestRatingRecord();
        TestRatingRecord CurrentRecordR2 = new TestRatingRecord();
        TestRatingRecord CurrentRecordR3 = new TestRatingRecord();

        String DiscountName = "TestDiscount";
        long BalanceGroupId = 1001L;
        String RUMToUse = "RUM";
        int counterId = 100000;
        double initialBalance = 200.0;
        double rumValue1 = 123.0;
        long UTCBalanceStartValidity = ConversionUtils.getConversionUtilsObject().getUTCDayStart(new Date());
        long UTCBalanceEndValidity = ConversionUtils.getConversionUtilsObject().getUTCDayEnd(new Date());
        CurrentRecord1.setRUMValue("RUM", rumValue1);
        CurrentRecord2.setRUMValue("RUM", rumValue1);
        CurrentRecord3.setRUMValue("RUM", rumValue1);
        CurrentRecord1.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecord2.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecord3.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecordR1.setRUMValue("RUM", rumValue1);
        CurrentRecordR2.setRUMValue("RUM", rumValue1);
        CurrentRecordR3.setRUMValue("RUM", rumValue1);
        CurrentRecordR1.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecordR2.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecordR3.UTCEventDate = UTCBalanceStartValidity;

        // ---------- First Event Fully Discounted ------------
        DiscountInformation result = instance.discountConsumeRUM(CurrentRecord1, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals((initialBalance-rumValue1), result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(rumValue1, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(true, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_FULLY_DISCOUNTED, result.getDiscountFlag());

        // Check the RUM value has been used up
        Assert.assertEquals(0,CurrentRecord1.getRUMValue("RUM"),0.000001);

        // Check the balance impacts in the record
        Assert.assertEquals(2,CurrentRecord1.getBalanceImpactCount());
        BalanceImpact balImp1 = CurrentRecord1.getBalanceImpact(0);
        Assert.assertEquals("CREATION",balImp1.ruleName);
        Assert.assertEquals("RUM",balImp1.rumUsed);
        Assert.assertEquals(200,balImp1.balanceDelta,0.000001);
        Assert.assertEquals(200,balImp1.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp1.counterID);
        Assert.assertEquals(1,balImp1.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp1.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp1.endDate);
        Assert.assertEquals(0,balImp1.rumValueUsed,0.000001);
        Assert.assertEquals(0,balImp1.rumValueAfter,0.000001);
        BalanceImpact balImp2 = CurrentRecord1.getBalanceImpact(1);
        Assert.assertEquals("ConsumeRUM",balImp2.ruleName);
        Assert.assertEquals("RUM",balImp2.rumUsed);
        Assert.assertEquals(-123,balImp2.balanceDelta,0.000001);
        Assert.assertEquals(77,balImp2.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp2.counterID);
        Assert.assertEquals(1,balImp2.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp2.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp2.endDate);
        Assert.assertEquals(123,balImp2.rumValueUsed,0.000001);
        Assert.assertEquals(0,balImp2.rumValueAfter,0.000001);

        // ---------- Second Event Partially Discounted ------------
        result = instance.discountConsumeRUM(CurrentRecord2, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals(0, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(77, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_PARTIALLY_DISCOUNTED, result.getDiscountFlag());

        // Check the RUM value has been partially used up
        Assert.assertEquals(46,CurrentRecord2.getRUMValue("RUM"),0.000001);

        // Check the balance impacts in the record
        Assert.assertEquals(1,CurrentRecord2.getBalanceImpactCount());
        BalanceImpact balImp3 = CurrentRecord2.getBalanceImpact(0);
        Assert.assertEquals("ConsumeRUM",balImp3.ruleName);
        Assert.assertEquals("RUM",balImp3.rumUsed);
        Assert.assertEquals(-77,balImp3.balanceDelta,0.000001);
        Assert.assertEquals(0,balImp3.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp3.counterID);
        Assert.assertEquals(1,balImp3.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp3.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp3.endDate);
        Assert.assertEquals(77,balImp3.rumValueUsed,0.000001);
        Assert.assertEquals(46,balImp3.rumValueAfter,0.000001);

        // ---------- Third Event not Discounted ------------
        result = instance.discountConsumeRUM(CurrentRecord1, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(false, result.isDiscountApplied());
        Assert.assertEquals(0, result.getCounterId());
        Assert.assertEquals(0, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(0, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(0, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_NO_DISCOUNT, result.getDiscountFlag());

        // Check the RUM value has been used up
        Assert.assertEquals(123,CurrentRecord3.getRUMValue("RUM"),0.000001);

        // =================== Now try to refund it all ====================
        // First event should be able to refund it all
        result = instance.refundConsumeRUM(CurrentRecordR3, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals(123, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(123, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_REFUNDED, result.getDiscountFlag());

        // Check the balance impacts in the record
        Assert.assertEquals(1,CurrentRecordR3.getBalanceImpactCount());
        BalanceImpact balImpR3 = CurrentRecordR3.getBalanceImpact(0);
        Assert.assertEquals("RefundRUM",balImpR3.ruleName);
        Assert.assertEquals("RUM",balImpR3.rumUsed);
        Assert.assertEquals(123,balImpR3.balanceDelta,0.000001);
        Assert.assertEquals(123,balImpR3.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImpR3.counterID);
        Assert.assertEquals(1,balImpR3.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImpR3.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImpR3.endDate);
        Assert.assertEquals(123,balImpR3.rumValueUsed,0.000001);
        Assert.assertEquals(123,balImpR3.rumValueAfter,0.000001);

        // Second event should be able to refund part of it
        result = instance.refundConsumeRUM(CurrentRecordR2, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals(200, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(77, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_REFUNDED, result.getDiscountFlag());

        // Check the balance impacts in the record
        Assert.assertEquals(1,CurrentRecordR2.getBalanceImpactCount());
        BalanceImpact balImpR2 = CurrentRecordR2.getBalanceImpact(0);
        Assert.assertEquals("RefundRUM",balImpR2.ruleName);
        Assert.assertEquals("RUM",balImpR2.rumUsed);
        Assert.assertEquals(77,balImpR2.balanceDelta,0.000001);
        Assert.assertEquals(200,balImpR2.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImpR2.counterID);
        Assert.assertEquals(1,balImpR2.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImpR2.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImpR2.endDate);
        Assert.assertEquals(123,balImpR2.rumValueUsed,0.000001);
        Assert.assertEquals(200,balImpR2.rumValueAfter,0.000001);

        // Third event should not be refunded at all (we are already at the max balance)
        result = instance.refundConsumeRUM(CurrentRecordR1, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance);

        // Check the results of the discounting
        Assert.assertEquals(false, result.isDiscountApplied());
        Assert.assertEquals(0, result.getCounterId());
        Assert.assertEquals(0, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(0, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(0, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_NO_DISCOUNT, result.getDiscountFlag());

        // Check the balance impacts in the record
        Assert.assertEquals(0,CurrentRecordR1.getBalanceImpactCount());
    }

    /**
     * Test of discountAggregateRUM method, of class AbstractBalanceHandlerPlugIn.
     */
    @Test
    public void testDiscountAggregateRUM() {
        System.out.println("discountAggregateRUM");

        // get the instance
        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
          // Not OK, Assert.fail the case
          message = "Error getting cache instance in test <AbstractBalanceHandlerPlugInTest>";
          Assert.fail(message);
        }

        // Set up the records for impacting
        TestRatingRecord CurrentRecord1 = new TestRatingRecord();
        TestRatingRecord CurrentRecord2 = new TestRatingRecord();
        TestRatingRecord CurrentRecord3 = new TestRatingRecord();

        String DiscountName = "TestDiscount";
        long BalanceGroupId = 1002L;
        String RUMToUse = "RUM";
        int counterId = 100000;
        double initialBalance = 0.0;
        double rumValue1 = 123.0;
        double rumValue2 = 17.1234;
        long UTCBalanceStartValidity = ConversionUtils.getConversionUtilsObject().getUTCDayStart(new Date());
        long UTCBalanceEndValidity = ConversionUtils.getConversionUtilsObject().getUTCDayEnd(new Date());
        CurrentRecord1.setRUMValue("RUM", rumValue1);
        CurrentRecord2.setRUMValue("RUM", rumValue1);
        CurrentRecord3.setRUMValue("RUM", rumValue2);
        CurrentRecord1.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecord2.UTCEventDate = UTCBalanceStartValidity;
        CurrentRecord3.UTCEventDate = UTCBalanceStartValidity;

        DiscountInformation result = instance.discountAggregateRUM(CurrentRecord1, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals(rumValue1, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(rumValue1, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(true, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_AGGREGATED, result.getDiscountFlag());

        // Check the RUM value has been aggregated
        Assert.assertEquals(rumValue1,CurrentRecord1.getRUMValue("RUM"),0.000001);

        // Check the balance impacts in the record
        Assert.assertEquals(2,CurrentRecord1.getBalanceImpactCount());
        BalanceImpact balImp1 = CurrentRecord1.getBalanceImpact(0);
        Assert.assertEquals("CREATION",balImp1.ruleName);
        Assert.assertEquals("RUM",balImp1.rumUsed);
        Assert.assertEquals(0,balImp1.balanceDelta,0.000001);
        Assert.assertEquals(0,balImp1.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp1.counterID);
        Assert.assertEquals(1,balImp1.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp1.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp1.endDate);
        Assert.assertEquals(0,balImp1.rumValueUsed,0.000001);
        Assert.assertEquals(0,balImp1.rumValueAfter,0.000001);

        BalanceImpact balImp2 = CurrentRecord1.getBalanceImpact(1);
        Assert.assertEquals("AggregateRUM",balImp2.ruleName);
        Assert.assertEquals("RUM",balImp2.rumUsed);
        Assert.assertEquals(rumValue1,balImp2.balanceDelta,0.000001);
        Assert.assertEquals(rumValue1,balImp2.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp2.counterID);
        Assert.assertEquals(1,balImp2.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp2.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp2.endDate);
        Assert.assertEquals(rumValue1,balImp2.rumValueUsed,0.000001);
        Assert.assertEquals(rumValue1,balImp2.rumValueAfter,0.000001);

        result = instance.discountAggregateRUM(CurrentRecord2, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals(2*rumValue1, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(rumValue1, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_AGGREGATED, result.getDiscountFlag());

        // Check the RUM value has been aggregated
        Assert.assertEquals(rumValue1,CurrentRecord2.getRUMValue("RUM"),0.000001);

        // Check the balance impacts in the record
        Assert.assertEquals(1,CurrentRecord2.getBalanceImpactCount());
        BalanceImpact balImp3 = CurrentRecord2.getBalanceImpact(0);
        Assert.assertEquals("AggregateRUM",balImp3.ruleName);
        Assert.assertEquals("RUM",balImp3.rumUsed);
        Assert.assertEquals(rumValue1,balImp3.balanceDelta,0.000001);
        Assert.assertEquals(2*rumValue1,balImp3.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp3.counterID);
        Assert.assertEquals(1,balImp3.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp3.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp3.endDate);
        Assert.assertEquals(rumValue1,balImp3.rumValueUsed,0.000001);
        Assert.assertEquals(rumValue1,balImp3.rumValueAfter,0.000001);

        result = instance.discountAggregateRUM(CurrentRecord3, DiscountName, BalanceGroupId, RUMToUse, counterId, initialBalance, UTCBalanceStartValidity, UTCBalanceEndValidity);

        // Check the results of the discounting
        Assert.assertEquals(true, result.isDiscountApplied());
        Assert.assertEquals(counterId, result.getCounterId());
        Assert.assertEquals(2*rumValue1 + rumValue2, result.getNewBalanceValue(), 0.000001);
        Assert.assertEquals(rumValue2, result.getDiscountedValue(), 0.000001);
        Assert.assertEquals(1, result.getRecId());
        Assert.assertEquals(false, result.isBalanceCreated());
        Assert.assertEquals(AbstractBalanceHandlerPlugIn.DISCOUNT_FLAG_AGGREGATED, result.getDiscountFlag());

        // Check the RUM value has been aggregated
        Assert.assertEquals(rumValue1,CurrentRecord1.getRUMValue("RUM"),0.000001);

        // Check the balance impacts in the record
        Assert.assertEquals(1,CurrentRecord3.getBalanceImpactCount());
        BalanceImpact balImp4 = CurrentRecord3.getBalanceImpact(0);
        Assert.assertEquals("AggregateRUM",balImp4.ruleName);
        Assert.assertEquals("RUM",balImp4.rumUsed);
        Assert.assertEquals(rumValue2,balImp4.balanceDelta,0.000001);
        Assert.assertEquals(2*rumValue1+rumValue2,balImp4.balanceAfter,0.000001);
        Assert.assertEquals(counterId,balImp4.counterID);
        Assert.assertEquals(1,balImp4.recID);
        Assert.assertEquals(UTCBalanceStartValidity,balImp4.startDate);
        Assert.assertEquals(UTCBalanceEndValidity,balImp4.endDate);
        Assert.assertEquals(rumValue2,balImp4.rumValueUsed,0.000001);
        Assert.assertEquals(rumValue2,balImp4.rumValueAfter,0.000001);
    }

  // -----------------------------------------------------------------------------
  // ---------------- Start of abstract class stub functions ---------------------
  // -----------------------------------------------------------------------------
    
    @Override
    public void init(String PipelineName, String ModuleName) throws InitializationException {
    }

    @Override
    public void process() throws ProcessingException {
    }

    @Override
    public int getOutboundRecordCount() {
      return 0;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void setInbound(ISupplier c) {
    }

    @Override
    public void setOutbound(IConsumer c) {
    }

    @Override
    public void setErrorBuffer(IConsumer err) {
    }

    @Override
    public void setExceptionHandler(ExceptionHandler h) {
    }

    @Override
    public void markForClosedown() {
    }

    @Override
    public void reset() {
    }

    @Override
    public int numThreads() {
        return 1;
    }

    @Override
    public String getSymbolicName() {
      return "OpenRateTest";
    }

    @Override
    public void setSymbolicName(String name) {
    }

    @Override
    public IRecord procRTValidRecord(IRecord r) throws ProcessingException {
        return r;
    }

    @Override
    public IRecord procRTErrorRecord(IRecord r) throws ProcessingException {
        return r;
    }

    @Override
    public void run() {
    }

  /**
   * Stub out the calls to the implementation processing - we don't need these
   * for unit testing.
   */
  public class AbstractBalanceHandlerPlugInImpl extends AbstractBalanceHandlerPlugIn
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
      instance = new AbstractBalanceHandlerPlugInTest.AbstractBalanceHandlerPlugInImpl();

      // Get the instance
      instance.init("DBTestPipe", "AbstractBalanceHandlerPlugInTest");

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
}
