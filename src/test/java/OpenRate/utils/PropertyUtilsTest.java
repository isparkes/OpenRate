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
package OpenRate.utils;

import OpenRate.exception.InitializationException;
import OpenRate.logging.AbstractLogFactory;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import java.net.URL;
import org.junit.*;

/**
 *
 * @author TGDSPIA1
 */
public class PropertyUtilsTest
{
  private static URL FQConfigFileName;

  private static String resourceName;
  private static String tmpResourceClassName;
  private static ResourceContext ctx = new ResourceContext();

  // Used for logging and exception handling
  private static String message; 

  public PropertyUtilsTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class<?>          ResourceClass;
    IResource         Resource;

    FQConfigFileName = new URL("File:src/test/resources/TestUtils.properties.xml");
    
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

    // Get a logger
    System.out.println("  Initialising Logger Resource...");
    resourceName         = "LogFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(AbstractLogFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
  }

  @AfterClass
  public static void tearDownClass() {
  }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

  /**
   * Test of getPropertyUtils method, of class PropertyUtils.
   */
  @Test
  public void testGetPropertyUtils() {
    System.out.println("getPropertyUtils");
    String expResult = "OpenRate.utils.PropertyUtils";
    PropertyUtils result = PropertyUtils.getPropertyUtils();
    Assert.assertEquals(expResult, result.getClass().getCanonicalName());
  }

  /**
   * Test of getPropertyUtils method, of class PropertyUtils.
   */
  @Test
  public void testGetPropertyUtilsGetSymbolicName() {
    System.out.println("getPropertyUtils SymbolicName");
    String expResult = "PropertyUtils";
    String result = PropertyUtils.getPropertyUtils().getSymbolicName();
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getPipelinePropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetPipelinePropertyValue() {
    System.out.println("getPipelinePropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    // Good values
    String pipeName = "DBTestPipe";
    String groupPrefix = "Configuration";
    String propertyName = "TestValue";

    // Test the good case
    String expResult = "testPipelinePropertyValue";
    String result = instance.getPipelinePropertyValue(pipeName, groupPrefix, propertyName);
    Assert.assertEquals(expResult, result);

    // Missing property
    propertyName = "TestValueNotThere";
    expResult = null;
    result = instance.getPipelinePropertyValue(pipeName, groupPrefix, propertyName);
    Assert.assertEquals(expResult, result);

    // Missing group
    propertyName = "TestValue";
    groupPrefix = "ConfigurationNotThere";
    result = instance.getPipelinePropertyValue(pipeName, groupPrefix, propertyName);
    Assert.assertEquals(expResult, result);

    // Missing pipe
    pipeName = "DBTestPipeNotThere";
    groupPrefix = "Configuration";
    result = instance.getPipelinePropertyValue(pipeName, groupPrefix, propertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getPipelinePropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetPipelinePropertyValueDef() {
    System.out.println("getPipelinePropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    // Good values
    String pipeName = "DBTestPipe";
    String groupPrefix = "Configuration";
    String propertyName = "TestValue";
    String defaultValue = "DefVal";

    // Test the good case
    String expResult = "testPipelinePropertyValue";
    String result = instance.getPipelinePropertyValueDef(pipeName, groupPrefix, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Missing property
    propertyName = "TestValueNotThere";
    expResult = defaultValue;
    result = instance.getPipelinePropertyValueDef(pipeName, groupPrefix, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Missing group
    propertyName = "TestValue";
    groupPrefix = "ConfigurationNotThere";
    result = instance.getPipelinePropertyValueDef(pipeName, groupPrefix, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Missing pipe
    pipeName = "DBTestPipeNotThere";
    groupPrefix = "Configuration";
    result = instance.getPipelinePropertyValueDef(pipeName, groupPrefix, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBatchInputAdapterPropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetBatchInputAdapterPropertyValue() {
    System.out.println("getBatchInputAdapterPropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String pipeName = "DBTestPipe";
    String moduleName = "TestInpAdapter";
    String propertyName = "ClassName";
    String expResult = "RatingTest.RateInputAdapter";

    // Simple good case
    String result = instance.getBatchInputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // property missing bad case
    propertyName = "ClassNameNotFound";
    expResult = null;
    result = instance.getBatchInputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // module missing bad case
    propertyName = "ClassName";
    moduleName = "TestInpAdapterNotFound";
    result = instance.getBatchInputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // pipe missing bad case
    pipeName = "DBTestPipeNotFound";
    moduleName = "TestInpAdapter";
    result = instance.getBatchInputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBatchInputAdapterPropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetBatchInputAdapterPropertyValueDef() {
    System.out.println("getBatchInputAdapterPropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String pipeName = "DBTestPipe";
    String moduleName = "TestInpAdapter";
    String propertyName = "ClassName";
    String defaultValue = "DefVal";
    String expResult = "RatingTest.RateInputAdapter";

    // Simple good case
    String result = instance.getBatchInputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // property missing bad case
    propertyName = "ClassNameNotFound";
    expResult = defaultValue;
    result = instance.getBatchInputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // module missing bad case
    propertyName = "ClassName";
    moduleName = "TestInpAdapterNotFound";
    result = instance.getBatchInputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // pipe missing bad case
    pipeName = "DBTestPipeNotFound";
    moduleName = "TestInpAdapter";
    result = instance.getBatchInputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getRTAdapterPropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetRTAdapterPropertyValue() {
    System.out.println("getRTAdapterPropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String PipeName = "DBRTTestPipe";
    String ModuleName = "RTInpAdapter";
    String PropertyName = "ListenerPort";
    String expResult = "8204";

    // Simple good result
    String result = instance.getRTAdapterPropertyValue(PipeName, ModuleName, PropertyName);
    Assert.assertEquals(expResult, result);

    // Bad result Property wrong
    PropertyName = "ListenerPortNotFound";
    expResult = null;
    result = instance.getRTAdapterPropertyValue(PipeName, ModuleName, PropertyName);
    Assert.assertEquals(expResult, result);

    // Bad result Module wrong
    PropertyName = "ListenerPort";
    ModuleName = "RTInpAdapterNotFound";
    result = instance.getRTAdapterPropertyValue(PipeName, ModuleName, PropertyName);
    Assert.assertEquals(expResult, result);

    // Bad result Pipe wrong
    ModuleName = "RTInpAdapter";
    PipeName = "DBRTTestPipeNotFound";
    result = instance.getRTAdapterPropertyValue(PipeName, ModuleName, PropertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getRTAdapterPropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetRTAdapterPropertyValueDef() {
    System.out.println("getRTAdapterPropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String PipeName = "DBRTTestPipe";
    String ModuleName = "RTInpAdapter";
    String PropertyName = "ListenerPort";
    String expResult = "8204";
    String defaultValue = "DefVal";

    // Simple good result
    String result = instance.getRTAdapterPropertyValueDef(PipeName, ModuleName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad result Property wrong
    PropertyName = "ListenerPortNotFound";
    expResult = defaultValue;
    result = instance.getRTAdapterPropertyValueDef(PipeName, ModuleName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad result Module wrong
    PropertyName = "ListenerPort";
    ModuleName = "RTInpAdapterNotFound";
    result = instance.getRTAdapterPropertyValueDef(PipeName, ModuleName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad result Pipe wrong
    ModuleName = "RTInpAdapter";
    PipeName = "DBRTTestPipeNotFound";
    result = instance.getRTAdapterPropertyValueDef(PipeName, ModuleName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getPluginPropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetPluginPropertyValue() throws Exception {
    System.out.println("getPluginPropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String pipeName = "DBTestPipe";
    String moduleName = "DestLookup";
    String propertyName = "BatchSize";
    String expResult = "5000";

    // Simple good case
    String result = instance.getPluginPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // property name wrong bad case
    propertyName = "BatchSizeNotFound";
    expResult = null;
    result = instance.getPluginPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // module name wrong bad case
    moduleName = "DestLookupNotFound";
    propertyName = "BatchSize";
    result = instance.getPluginPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // pipe name wrong bad case
    pipeName = "DBTestPipeNotFound";
    moduleName = "DestLookup";
    result = instance.getPluginPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getPluginPropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetPluginPropertyValueDef() throws Exception {
    System.out.println("getPluginPropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String pipeName = "DBTestPipe";
    String moduleName = "DestLookup";
    String propertyName = "BatchSize";
    String defaultValue = "DefVal";
    String expResult = "5000";

    // Simple good case
    String result = instance.getPluginPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // property name wrong bad case
    propertyName = "BatchSizeNotFound";
    expResult = defaultValue;
    result = instance.getPluginPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // module name wrong bad case
    moduleName = "DestLookupNotFound";
    propertyName = "BatchSize";
    result = instance.getPluginPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // pipe name wrong bad case
    pipeName = "DBTestPipeNotFound";
    moduleName = "DestLookup";
    result = instance.getPluginPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBatchOutputAdapterPropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetBatchOutputAdapterPropertyValue() throws Exception {
    System.out.println("getBatchOutputAdapterPropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String pipeName = "DBTestPipe";
    String moduleName = "SOutAdapter";
    String propertyName = "OutputFilePrefix";
    String defaultValue = "DefVal";
    String expResult = "testpipeline";

    // Simple good case
    String result = instance.getBatchOutputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // Bad case property wrong
    propertyName = "OutputFilePrefixNotFound";
    expResult = null;
    result = instance.getBatchOutputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // Bad case module wrong
    moduleName = "SOutAdapterNotFound";
    propertyName = "OutputFilePrefix";
    result = instance.getBatchOutputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);

    // Bad case pipe wrong
    pipeName = "DBTestPipeNotFound";
    moduleName = "SOutAdapter";
    result = instance.getBatchOutputAdapterPropertyValue(pipeName, moduleName, propertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getBatchOutputAdapterPropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetBatchOutputAdapterPropertyValueDef() throws Exception {
    System.out.println("getBatchOutputAdapterPropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String pipeName = "DBTestPipe";
    String moduleName = "SOutAdapter";
    String propertyName = "OutputFilePrefix";
    String defaultValue = "DefVal";
    String expResult = "testpipeline";

    // Simple good case
    String result = instance.getBatchOutputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad case property wrong
    propertyName = "OutputFilePrefixNotFound";
    expResult = defaultValue;
    result = instance.getBatchOutputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad case module wrong
    moduleName = "SOutAdapterNotFound";
    propertyName = "OutputFilePrefix";
    result = instance.getBatchOutputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad case pipe wrong
    pipeName = "DBTestPipeNotFound";
    moduleName = "SOutAdapter";
    result = instance.getBatchOutputAdapterPropertyValueDef(pipeName, moduleName, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getResourcePropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetResourcePropertyValue() throws Exception {
    System.out.println("getResourcePropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String resource = "ECI";
    String propertyName = "Port";
    String expResult = "8086";

    // Simple good case
    String result = instance.getResourcePropertyValue(resource, propertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getResourcePropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetResourcePropertyValueDef() throws Exception {
    System.out.println("getResourcePropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String resource = "ECI";
    String propertyName = "Port";
    String expResult = "8086";
    String defaultValue = "DefVal";

    // Simple good case
    String result = instance.getResourcePropertyValueDef(resource, propertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDataCachePropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetDataCachePropertyValue() throws Exception {
    System.out.println("getDataCachePropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String cacheManName = "CacheFactory";
    String CacheName = "RegexMatchTestCache";
    String PropertyName = "DataSourceType";
    String expResult = "File";

    // Simple good case
    String result = instance.getDataCachePropertyValue(cacheManName, CacheName, PropertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDataCachePropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetDataCachePropertyValueDef() throws Exception {
    System.out.println("getDataCachePropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String cacheManName = "CacheFactory";
    String CacheName = "RegexMatchTestCache";
    String PropertyName = "DataSourceType";
    String expResult = "File";
    String defaultValue = "NONE";

    // Simple good case
    String result = instance.getDataCachePropertyValueDef(cacheManName, CacheName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad case
    PropertyName = "DataSourceTypeNotFound";
    expResult = defaultValue;
    result = instance.getDataCachePropertyValueDef(cacheManName, CacheName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDataSourcePropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetDataSourcePropertyValue() throws Exception {
    System.out.println("getDataSourcePropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String DataSourceName = "TestDB";
    String PropertyName = "username";
    String expResult = "root";

    // Simple good case
    String result = instance.getDataSourcePropertyValue(DataSourceName, PropertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getDataSourcePropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetDataSourcePropertyValueDef() throws Exception {
    System.out.println("getDataSourcePropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String DataSourceName = "TestDB";
    String PropertyName = "username";
    String expResult = "root";
    String defaultValue = "NONE";

    // Simple good case
    String result = instance.getDataSourcePropertyValueDef(DataSourceName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Bad case
    expResult = defaultValue;
    PropertyName = "usernameNotFound";
    result = instance.getDataSourcePropertyValueDef(DataSourceName, PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getFrameworkPropertyValue method, of class PropertyUtils.
   */
  @Test
  public void testGetFrameworkPropertyValue() throws Exception {
    System.out.println("getFrameworkPropertyValue");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String PropertyName = "Application";
    String expResult = "UtilsTest";

    // Simple good case
    String result = instance.getFrameworkPropertyValue(PropertyName);
    Assert.assertEquals(expResult, result);
  }

  /**
   * Test of getFrameworkPropertyValueDef method, of class PropertyUtils.
   */
  @Test
  public void testGetFrameworkPropertyValueDef() throws Exception {
    System.out.println("getFrameworkPropertyValueDef");

    PropertyUtils instance = PropertyUtils.getPropertyUtils();

    String PropertyName = "Application";
    String expResult = "UtilsTest";
    String defaultValue = "NONE";

    // Simple good case
    String result = instance.getFrameworkPropertyValueDef(PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);

    // Simple bad case
    PropertyName = "ApplicationNotFound";
    expResult = defaultValue;
    result = instance.getFrameworkPropertyValueDef(PropertyName, defaultValue);
    Assert.assertEquals(expResult, result);
  }
}
