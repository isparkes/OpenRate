/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package TestUtils;

import OpenRate.OpenRate;
import OpenRate.Pipeline;
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import OpenRate.logging.AbstractLogFactory;
import OpenRate.logging.LogUtil;
import OpenRate.resource.CacheFactory;
import OpenRate.resource.ConversionCache;
import OpenRate.resource.DataSourceFactory;
import OpenRate.resource.IResource;
import OpenRate.resource.ResourceContext;
import OpenRate.transaction.TransactionManagerFactory;
import OpenRate.utils.PropertyUtils;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import org.junit.Assert;

/**
 *
 * @author TGDSPIA1
 */
public class FrameworkUtils {

 /**
  * Load the properties object
  * 
  * @param configFileName 
  */
  public static void loadProperties(URL configFileName)
  {
    // Get a properties object
    try
    {
      PropertyUtils.getPropertyUtils().loadPropertiesXML(configFileName,"FWProps");
    }
    catch (InitializationException ex)
    {
      String message = "Error reading the configuration file <" + System.getProperty("user.dir") + "/" + configFileName + ">";
      Assert.fail(message);
    }
    
    // Check for exceptions
    if (OpenRate.getFrameworkExceptionHandler().hasError())
    {
      Assert.fail("Exception: " + OpenRate.getFrameworkExceptionHandler().getExceptionList().get(0).getLocalizedMessage());
    }
   }

  /**
   * Read the logger configuration and create the logger instances. These have
   * to be linked to the OpenRate instance later.
   * 
   * @throws InitializationException
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException 
   */
  public static void startupLoggers() throws InitializationException, ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    String resourceName;
    String tmpResourceClassName;
    ResourceContext ctx = new ResourceContext();
    Class<?>          ResourceClass;
    IResource         Resource;
    
    // Get a logger
    System.out.println("  Initialising Logger Resource...");
    resourceName         = "LogFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(AbstractLogFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
    
    // Link the loggers
    OpenRate.getApplicationInstance().setFwLog(LogUtil.getLogUtil().getLogger("Framework"));
    OpenRate.getApplicationInstance().setStatsLog(LogUtil.getLogUtil().getLogger("Statistics"));
    OpenRate.getApplicationInstance().setErrorLog(LogUtil.getLogUtil().getLogger("Error"));

    // Set the logger for the pipeline - we have to inject this into the pipe
    Pipeline testpipe = new Pipeline();
    testpipe.setPipeLog(LogUtil.getLogUtil().getLogger("DBTestPipePipeline"));
    testpipe.setSymbolicName("DBTestPipe");
    OpenRate.addPipelineToMap("DBTestPipe",testpipe);    
        
    // Check for exceptions
    if (OpenRate.getFrameworkExceptionHandler().hasError())
    {
      Assert.fail("Exception: " + OpenRate.getFrameworkExceptionHandler().getExceptionList().get(0).getLocalizedMessage());
    }
  }

  public static void startupDataSources() throws InitializationException, ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    String resourceName;
    String tmpResourceClassName;
    ResourceContext ctx = new ResourceContext();
    Class<?>          ResourceClass;
    IResource         Resource;
    
   // Get a data Source factory
    System.out.println("  Initialising Data Source Resource...");
    resourceName         = "DataSourceFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(DataSourceFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
    
    // Check for exceptions
    if (OpenRate.getFrameworkExceptionHandler().hasError())
    {
      ArrayList<Exception> exceptionList = OpenRate.getFrameworkExceptionHandler().getExceptionList();
      for (Exception ex : exceptionList) {
        System.err.println("Exception: " + ex.getMessage());
        ex.printStackTrace();
      }
      Assert.fail("Exception(s) in startupDataSources()");
    }
  }  
  
  public static void startupCaches() throws InitializationException, ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    String resourceName;
    String tmpResourceClassName;
    ResourceContext ctx = new ResourceContext();
    Class<?>          ResourceClass;
    IResource         Resource;
    
    // Get a cache factory - we do the catch here, because we want to see the
    // diagnostic message from the Framework exception handler. If we don't catch,
    // we won't see it.
    try {
      System.out.println("  Initialising Cache Factory Resource...");
      resourceName         = "CacheFactory";
      tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(CacheFactory.RESOURCE_KEY,"ClassName");
      ResourceClass        = Class.forName(tmpResourceClassName);
      Resource             = (IResource)ResourceClass.newInstance();
      Resource.init(resourceName);
      ctx.register(resourceName, Resource);
    } catch (InitializationException ex) {
      System.out.println("Exception starting up Cache Factory: " + ex.getMessage());
    }

    // Check for exceptions
    if (OpenRate.getFrameworkExceptionHandler().hasError())
    {
      for (Exception exception : OpenRate.getFrameworkExceptionHandler().getExceptionList()) {
        System.err.println("Exception: " + exception.getMessage());
      }
      Assert.fail("Exception szatzing up the cache factory");
    }
  }
  
  public static void startupTransactionManager() throws InitializationException, ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    String resourceName;
    String tmpResourceClassName;
    ResourceContext ctx = new ResourceContext();
    Class<?>          ResourceClass;
    IResource         Resource;
    
    // Get a transaction manager
    System.out.println("  Initialising Transaction Manager Resource...");
    resourceName         = "TransactionManagerFactory";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(TransactionManagerFactory.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
    // Check for exceptions
    if (OpenRate.getFrameworkExceptionHandler().hasError())
    {
      Assert.fail("Exception: " + OpenRate.getFrameworkExceptionHandler().getExceptionList().get(0).getLocalizedMessage());
    }    
  }
  
  public static void startupConversionCache() throws InitializationException, ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    String resourceName;
    String tmpResourceClassName;
    ResourceContext ctx = new ResourceContext();
    Class<?>          ResourceClass;
    IResource         Resource;
    
    // Get a conversion cache
    System.out.println("  Initialising Conversion Cache Resource...");
    resourceName         = "ConversionCache";
    tmpResourceClassName = PropertyUtils.getPropertyUtils().getResourcePropertyValue(ConversionCache.RESOURCE_KEY,"ClassName");
    ResourceClass        = Class.forName(tmpResourceClassName);
    Resource             = (IResource)ResourceClass.newInstance();
    Resource.init(resourceName);
    ctx.register(resourceName, Resource);
  }
    
 /**
  * Gets the DB connection for a given cache.
  * 
   * @param cacheName
  * @return
  * @throws InitializationException
  * @throws ClassNotFoundException
  * @throws InstantiationException
  * @throws IllegalAccessException 
  */
  public static Connection getDBConnection(String cacheName) throws InitializationException, ClassNotFoundException, InstantiationException, IllegalAccessException
  {
    // Get the data source name
    String connectionName = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef("CacheFactory",
                                                                                        cacheName,
                                                                                        "DataSource",
                                                                                        "None");
   // The datasource property was added to allow database to database
    // JDBC adapters to work properly using 1 configuration file.
    if(DBUtil.initDataSource(connectionName) == null)
    {
      String message = "Could not initialise DB connection <" + connectionName + "> in test <AbstractBestMatchTest>.";
      Assert.fail(message);
    }

    // Get a connection
    Connection JDBCChcon = DBUtil.getConnection(connectionName);
    
    return JDBCChcon;
  }
}
