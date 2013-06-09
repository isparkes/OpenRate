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

import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.AbstractLogFactory;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.resource.DataSourceFactory;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import OpenRate.utils.PropertyUtils;
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

  private static String cacheDataSourceName;
  private static String resourceName;
  private static String tmpResourceClassName;
  private static ResourceContext ctx = new ResourceContext();
  private static AbstractMultipleValidityMatch instance;

 /**
  * Default constructor
  */
  public AbstractMultipleValidityMatchTest()
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
    String Message = "Error reading the configuration file <" + FQConfigFileName + ">";
    Assert.fail(Message);
    }

    // Get the data source name
    cacheDataSourceName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef("CacheFactory",
                                                                                        "BestMatchTestCache",
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
    String Message = "Could not initialise DB connection <" + cacheDataSourceName + "> in test <AbstractMultipleValidityMatchTest>.";
    Assert.fail(Message);
    }

    // Get a connection
    Connection JDBCChcon = DBUtil.getConnection(cacheDataSourceName);

    try
    {
    JDBCChcon.prepareStatement("DROP TABLE TEST_MULT_VALIDITY_MATCH;").execute();
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
        String Message = "Error dropping table TEST_MULT_VALIDITY_MATCH in test <AbstractMultipleValidityMatchTest>.";
        Assert.fail(Message);
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
  public static void tearDownClass()
  {
  }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

  /**
   * Test of init method, of class AbstractMultipleValidityMatch.
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

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      String Message = "Error getting cache instance in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
    }

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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      String Message = "Error getting cache instance in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
    }

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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      String Message = "Error getting cache instance in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
    }

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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      String Message = "Error getting cache instance in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
    }

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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
    ArrayList<ArrayList<String>> expResult = new ArrayList<>();
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("isValidMultipleValidityMatchResult");

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      String Message = "Error getting cache instance in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
    }

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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
    String expResult;
    String Group;
    String Resource;
    long   eventDate = 0;
    SimpleDateFormat sdfEvt = new SimpleDateFormat("yyyyMMddhhmmss");

    System.out.println("isValidMultipleValidityMatchResult");

    try
    {
      getInstance();
    }
    catch (InitializationException ie)
    {
      // Not OK, Assert.fail the case
      String Message = "Error getting cache instance in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
    }

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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
      String Message = "Error getting event date in test <AbstractMultipleValidityMatchTest>";
      Assert.fail(Message);
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
  private void getInstance() throws InitializationException
  {
    if (instance == null)
    {
      // Get an initialise the cache
      instance = new AbstractMultipleValidityMatchTest.AbstractMultipleValidityMatchImpl();

      // Get the instance
      instance.init("DBTestPipe", "AbstractMultipleValidityMatchTest");
    }
  }
}
