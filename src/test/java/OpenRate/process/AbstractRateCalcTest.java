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

import OpenRate.audit.AuditUtils;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.AbstractLogFactory;
import OpenRate.record.IRecord;
import OpenRate.record.RateMapEntry;
import OpenRate.record.RatingResult;
import OpenRate.resource.CacheFactory;
import OpenRate.resource.DataSourceFactory;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import static org.junit.Assert.*;
import org.junit.*;

/**
 *
 * @author TGDSPIA1
 */
public class AbstractRateCalcTest
{
  // local in-memory database for testing
  private static final String FQConfigFileName = "src/test/resources/TestRating.properties.xml";

  private static String cacheDataSourceName;
  private static String resourceName;
  private static String tmpResourceClassName;
  private static ResourceContext ctx = new ResourceContext();
  private static AbstractRateCalc instance;

    public AbstractRateCalcTest() {
    }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    Class<?>          ResourceClass;
    IResource         Resource;

      // Get a properties object
      try
      {
        PropertyUtils.getPropertyUtils().loadPropertiesXML(FQConfigFileName,"FWProps");
      }
      catch (InitializationException ex)
      {
        String Message = "Error reading the configuration file <" + FQConfigFileName + ">" + System.getProperty("user.dir");
        Assert.fail(Message);
      }

      // Get the data source name
      cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef("CacheFactory",
                                                                                          "RateTestCache",
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
        String Message = "Could not initialise DB connection <" + cacheDataSourceName + "> in test <AbstractRateCalcTest>.";
        Assert.fail(Message);
      }

      // Get a connection
      Connection JDBCChcon = DBUtil.getConnection(cacheDataSourceName);

      try
      {
        JDBCChcon.prepareStatement("DROP TABLE TEST_PRICE_MODEL;").execute();
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
          String Message = "Error dropping table TEST_PRICE_MODEL in test <AbstractRateCalcTest>.";
          Assert.fail(Message);
        }
      }

      // Create the test table
      JDBCChcon.prepareStatement("CREATE TABLE TEST_PRICE_MODEL (ID SERIAL,PRICE_MODEL varchar(64) NOT NULL,STEP int DEFAULT 0 NOT NULL,TIER_FROM int,TIER_TO int,BEAT int,FACTOR double,CHARGE_BASE int,VALID_FROM DATE, VALID_TO DATE);").execute();

      // Simplest price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating"
      JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM,VALID_TO) values ('TestModel1',1,0,999999,60,1,60,'2000-01-01','2020-12-31');").execute();

      // Simplest tiered price model possible - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating" for the first min, therefore 0.1 per min per second
      JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM,VALID_TO) values ('TestModel2',1,0,60,60,1,60,'2000-01-01','2020-12-31');").execute();
      JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM,VALID_TO) values ('TestModel2',2,60,999999,60,0.1,60,'2000-01-01','2020-12-31');").execute();

      // Time Bound tiered price model - 1 (FACTOR) per minute (CHARGE_BASE), with a charge increment of 1 (BEAT) = "per second rating" up until Jan 1 2013, thereafter 0.5
      JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM,VALID_TO) values ('TestModel3',1,0,999999,60,1,60,'2000-01-01','2012-12-31');").execute();
      JDBCChcon.prepareStatement("INSERT INTO TEST_PRICE_MODEL (PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM,VALID_TO) values ('TestModel3',1,0,999999,60,0.5,60,'2013-01-01','2020-12-31');").execute();

      // Get a cache factory
      System.out.println("  Initialising Cache Factory Resource...");
      resourceName         = "CacheFactory";
      tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(CacheFactory.RESOURCE_KEY,"ClassName");
      ResourceClass        = Class.forName(tmpResourceClassName);
      Resource             = (IResource)ResourceClass.newInstance();
      Resource.init(resourceName);
      ctx.register(resourceName, Resource);
  }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
     * uses a simple non-tiered model, with no time bounding.
     */
    @Test
    public void testRateCalculateTieredNonTimeBoundNonTiered() throws Exception {
        System.out.println("rateCalculateTieredNonTimeBoundNonTiered");

        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
          // Not OK, Assert.fail the case
          String Message = "Error getting cache instance in test <AbstractRateCalcTest>";
          Assert.fail(Message);
        }

        // Simple test using non-time bound non-tiered model
        String priceModel = "TestModel1";
        double valueToRate = 0.0;
        long CDRDate = Calendar.getInstance().getTimeInMillis()/1000;

        // zero value to rate
        double expResult = 0.0;
        double result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 1st beat - try all integer values
        for (int seconds = 1 ; seconds < 60 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 1.0;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // intra-beat 2, non integer value
        valueToRate = 2.654;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 2 beats - try all integer values
        for (int seconds = 61 ; seconds < 120 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 2.0;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // run some large values through
        valueToRate = 999999;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating
        valueToRate = 1000000;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating some more
        valueToRate = 1500000;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);
    }

    /**
     * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
     * uses a simple tiered model, with no time bounding.
     */
    @Test
    public void testRateCalculateTieredNonTimeBoundTiered() throws Exception {
        System.out.println("rateCalculateTieredNonTimeBoundTiered");

        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
          // Not OK, Assert.fail the case
          String Message = "Error getting cache instance in test <AbstractRateCalcTest>";
          Assert.fail(Message);
        }

        // Simple test using non-time bound non-tiered model
        String priceModel = "TestModel2";
        double valueToRate = 0.0;
        long CDRDate = Calendar.getInstance().getTimeInMillis()/1000;

        // zero value to rate
        double expResult = 0.0;
        double result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 1st beat - try all integer values
        for (int seconds = 1 ; seconds < 60 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 1.0;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // intra-beat 2, non integer value
        valueToRate = 2.654;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 2 beats - try all integer values
        for (int seconds = 61 ; seconds < 120 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 1.1;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // run some large values through
        valueToRate = 999999;
        expResult = 1667.60;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating
        valueToRate = 1000000;
        expResult = 1667.60;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating some more
        valueToRate = 1500000;
        expResult = 1667.60;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);
    }

    /**
     * Test of rateCalculateTiered method, of class AbstractRateCalc. This test
     * uses a simple non-tiered model, with no time bounding.
     */
    @Test
    public void testRateCalculateTieredTimeBoundNonTiered() throws Exception {
        System.out.println("rateCalculateTieredTimeBoundNonTiered");

        try
        {
          getInstance();
        }
        catch (InitializationException ie)
        {
          // Not OK, Assert.fail the case
          String Message = "Error getting cache instance in test <AbstractRateCalcTest>";
          Assert.fail(Message);
        }

        // Simple test using non-time bound non-tiered model
        String priceModel = "TestModel3";
        ConversionUtils conv = ConversionUtils.getConversionUtilsObject();
        conv.setInputDateFormat("yyyy-MM-dd hh:mm:ss");
        long CDRDate = conv.convertInputDateToUTC("2010-01-23 00:00:00");

        // zero value to rate
        double valueToRate = 0.0;
        double expResult = 0.0;
        double result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 1st beat - try all integer values
        for (int seconds = 1 ; seconds < 60 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 1.0;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // intra-beat 2, non integer value
        valueToRate = 2.654;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 2 beats - try all integer values
        for (int seconds = 61 ; seconds < 120 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 2.0;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // run some large values through
        valueToRate = 999999;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating
        valueToRate = 1000000;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating some more
        valueToRate = 1500000;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // Noww set the date after the change
        CDRDate = conv.convertInputDateToUTC("2013-01-23 00:00:00");

        // zero value to rate
        valueToRate = 0.0;
        expResult = 0.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 1st beat - try all integer values
        for (int seconds = 1 ; seconds < 60 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 0.5;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // intra-beat 2, non integer value
        valueToRate = 2.654;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // intra-beat 2 beats - try all integer values
        for (int seconds = 61 ; seconds < 120 ; seconds++ )
        {
          valueToRate = seconds;
          expResult = 1.0;
          result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
          assertEquals(expResult, result, 0.00001);
        }

        // run some large values through
        valueToRate = 999999;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating
        valueToRate = 1000000;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);

        // run off the end of the rating some more
        valueToRate = 1500000;
        expResult = 16667.0;
        result = instance.rateCalculateTiered(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.00001);
    }

    /**
     * Test of rateCalculateThreshold method, of class AbstractRateCalc.
     */
    @Test
    public void testRateCalculateThreshold() throws Exception {
        System.out.println("rateCalculateThreshold");
        String priceModel = "";
        double valueToRate = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.rateCalculateThreshold(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of rateCalculateFlat method, of class AbstractRateCalc.
     */
    @Test
    public void testRateCalculateFlat() throws Exception {
        System.out.println("rateCalculateFlat");
        String priceModel = "";
        double valueToRate = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.rateCalculateFlat(priceModel, valueToRate, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of rateCalculateEvent method, of class AbstractRateCalc.
     */
    @Test
    public void testRateCalculateEvent() throws Exception {
        System.out.println("rateCalculateEvent");
        String priceModel = "";
        long CDRDate = 0L;
        long valueToRate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.rateCalculateEvent(priceModel, CDRDate, valueToRate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of authCalculateFlat method, of class AbstractRateCalc.
     */
    @Test
    public void testAuthCalculateFlat() throws Exception {
        System.out.println("authCalculateFlat");
        String priceModel = "";
        double currentBalance = 0.0;
        double creditLimit = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.authCalculateFlat(priceModel, currentBalance, creditLimit, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of authCalculateEvent method, of class AbstractRateCalc.
     */
    @Test
    public void testAuthCalculateEvent() throws Exception {
        System.out.println("authCalculateEvent");
        String priceModel = "";
        double currentBalance = 0.0;
        double creditLimit = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        boolean expResult = false;
        boolean result = instance.authCalculateEvent(priceModel, currentBalance, creditLimit, CDRDate);
        assertEquals(expResult, result);
    }

    /**
     * Test of authCalculateTiered method, of class AbstractRateCalc.
     */
    @Test
    public void testAuthCalculateTiered() throws Exception {
        System.out.println("authCalculateTiered");
        String priceModel = "";
        double currentBalance = 0.0;
        double creditLimit = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.authCalculateTiered(priceModel, currentBalance, creditLimit, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of authCalculateThreshold method, of class AbstractRateCalc.
     */
    @Test
    public void testAuthCalculateThreshold() throws Exception {
        System.out.println("authCalculateThreshold");
        String priceModel = "";
        double currentBalance = 0.0;
        double creditLimit = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.authCalculateThreshold(priceModel, currentBalance, creditLimit, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of performRateEvaluationTiered method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformRateEvaluationTiered() throws Exception {
        System.out.println("performRateEvaluationTiered");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        double valueToRate = 0.0;
        long CDRDate = 0L;
        boolean BreakDown = false;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        RatingResult expResult = null;
        RatingResult result = instance.performRateEvaluationTiered(PriceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);
        assertEquals(expResult, result);
    }

    /**
     * Test of performRateEvaluationThreshold method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformRateEvaluationThreshold() throws Exception {
        System.out.println("performRateEvaluationThreshold");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        double valueToRate = 0.0;
        long CDRDate = 0L;
        boolean BreakDown = false;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        RatingResult expResult = null;
        RatingResult result = instance.performRateEvaluationThreshold(PriceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);
        assertEquals(expResult, result);
    }

    /**
     * Test of performRateEvaluationFlat method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformRateEvaluationFlat() throws Exception {
        System.out.println("performRateEvaluationFlat");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        double valueToRate = 0.0;
        long CDRDate = 0L;
        boolean BreakDown = false;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        RatingResult expResult = null;
        RatingResult result = instance.performRateEvaluationFlat(PriceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);
        assertEquals(expResult, result);
    }

    /**
     * Test of performRateEvaluationEvent method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformRateEvaluationEvent() throws Exception {
        System.out.println("performRateEvaluationEvent");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        long valueToRate = 0L;
        long CDRDate = 0L;
        boolean BreakDown = false;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        RatingResult expResult = null;
        RatingResult result = instance.performRateEvaluationEvent(PriceModel, tmpRateModel, valueToRate, CDRDate, BreakDown);
        assertEquals(expResult, result);
    }

    /**
     * Test of performAuthEvaluationFlat method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformAuthEvaluationFlat() throws Exception {
        System.out.println("performAuthEvaluationFlat");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        double availableBalance = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.performAuthEvaluationFlat(PriceModel, tmpRateModel, availableBalance, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of performAuthEvaluationEvent method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformAuthEvaluationEvent() throws Exception {
        System.out.println("performAuthEvaluationEvent");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        double availableBalance = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        boolean expResult = false;
        boolean result = instance.performAuthEvaluationEvent(PriceModel, tmpRateModel, availableBalance, CDRDate);
        assertEquals(expResult, result);
    }

    /**
     * Test of performAuthEvaluationTiered method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformAuthEvaluationTiered() throws Exception {
        System.out.println("performAuthEvaluationTiered");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        double availableBalance = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.performAuthEvaluationTiered(PriceModel, tmpRateModel, availableBalance, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of performAuthEvaluationThreshold method, of class AbstractRateCalc.
     */
    @Test
    public void testPerformAuthEvaluationThreshold() throws Exception {
        System.out.println("performAuthEvaluationThreshold");
        String PriceModel = "";
        ArrayList<RateMapEntry> tmpRateModel = null;
        double availableBalance = 0.0;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        double expResult = 0.0;
        double result = instance.performAuthEvaluationThreshold(PriceModel, tmpRateModel, availableBalance, CDRDate);
        assertEquals(expResult, result, 0.0);
    }

    /**
     * Test of getRateModelEntryForTime method, of class AbstractRateCalc.
     */
    @Test
    public void testGetRateModelEntryForTime() {
        System.out.println("getRateModelEntryForTime");
        RateMapEntry tmpEntry = null;
        long CDRDate = 0L;
        AbstractRateCalc instance = new AbstractRateCalcImpl();
        RateMapEntry expResult = null;
        RateMapEntry result = instance.getRateModelEntryForTime(tmpEntry, CDRDate);
        assertEquals(expResult, result);
    }

    public class AbstractRateCalcImpl extends AbstractRateCalc {
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
      instance = new AbstractRateCalcTest.AbstractRateCalcImpl();

      // Turn off audit logging (we don't need it for testing)
      AuditUtils.getAuditUtils().setAuditLogging(false);

      // Get the instance
      instance.init("DBTestPipe", "AbstractRateCalcTest");
    }
  }
}
