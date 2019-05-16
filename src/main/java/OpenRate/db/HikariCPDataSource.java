package OpenRate.db;

import OpenRate.OpenRate;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.utils.PropertyUtils;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The data source is a a pooled collection of connections that can be used
 * by elements of the framework.
 * 
 * * @author ddijak
 */


public class HikariCPDataSource implements IDBDataSource
{
  // The symbolic name is used in the management of the pipeline (control and
  // thread monitoring) and logging.
  private String SymbolicName = "HikariDataSource";

  // reference to the exception handler
  private ExceptionHandler handler;
  
  // used to simplify logging and exception handling
  public String message;
  
  // -----------------------------------------------------------------------------------------------
  // ------------------ Hikari Connection Pool specific configuration options  ---------------------
  // -----------------------------------------------------------------------------------------------

  /**
   * minimal number of connections per partition
   */
  public static final String MIN_CONN_KEY = "MinPoolConnections";
  public static final String DEFAULT_MIN_CONN = "5";
  
  /**
   * maximal number of connections
   */
  public static final String MAX_CONN_KEY = "MaxPoolConnections";
  public static final String DEFAULT_MAX_CONN = "10";
     
  /**
   * Max Connection Age in seconds
   */
  
  public static final String CONN_TIMEOUT_KEY = "ConnectionTimeout";
  public static final String DEFAULT_CONN_TIMEOUT = "30000";
  
  /**
   * pool name
   */
  public static final String POOL_NAME_KEY = "Name";
  public static final String DEFAULT_POOL_NAME = "HikariDefined";
  
  // -----------------------------------------------------------------------------
  // --------------------------- Implementation  ---------------------------------
  // -----------------------------------------------------------------------------

 /**
  * Constructor: log our version info to the audit system
  */
  public HikariCPDataSource()
  {
  }

 /**
  * Create new data source from provided properties.
  *
  * @param ResourceName The name of the DataSourceFactory in the properties
  * @param dataSourceName The name of the data source to create
  * @return The created data source
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public DataSource getDataSource(String ResourceName, String dataSourceName)
    throws InitializationException
  {
    String db_url = null;
    String driver = null;
    String username = null;
    String password = null;
    String validationSQL = null;

    OpenRate.getOpenRateFrameworkLog().debug("Creating new DataSource <" + dataSourceName + ">");

    try
    {
      // get the connection parameters
      db_url     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DB_URL_KEY);
      driver     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DRIVER_KEY);
      username   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, USERNAME_KEY);
      password   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, PASSWORD_KEY);  
      validationSQL = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, VALIDATION_QUERY_KEY);
      
      if (db_url == null || db_url.isEmpty())
      {
        message = "Error recovering data source parameter <db_url> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
      
      if (driver == null || driver.isEmpty())
      {
        message = "Error recovering data source parameter <driver> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }

      if (username == null || username.isEmpty())
      {
        message = "Error recovering data source parameter <username> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }

      if (password == null)
      {
        message = "Error recovering data source parameter <password> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }

      OpenRate.getOpenRateFrameworkLog().info("Creating DataSource <" + dataSourceName + "> using driver <" + driver + "> from URL <" + db_url + ">");
      Class<?> driverClass = Class.forName(driver);
      OpenRate.getOpenRateFrameworkLog().debug("jdbc driver loaded. name = <" + driverClass.getName() + ">");
    }
    catch (ClassNotFoundException cnfe)
    {
      message = "Driver class <" + driver + "> not found for data source <" + dataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
    
    // Initialize pool 
    HikariDataSource dataSource = new HikariDataSource();
    
    // Set driver class 
    dataSource.setDriverClassName(driver);
    //dataSource.setDataSourceClassName(dsName);
        
    // Options with defaults
    int minConn 	  = Integer.parseInt(PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MIN_CONN_KEY, DEFAULT_MIN_CONN));
    int maxConn 	  = Integer.parseInt(PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MAX_CONN_KEY, DEFAULT_MAX_CONN));
    Long connTimeOut  = Long.parseLong(PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,CONN_TIMEOUT_KEY, DEFAULT_CONN_TIMEOUT));
    String poolName   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,POOL_NAME_KEY, DEFAULT_POOL_NAME);
    // Perform the initialization      
    dataSource.setJdbcUrl(db_url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);

    // Pooling configuration
    dataSource.setMaximumPoolSize(maxConn);    
    dataSource.setMinimumIdle(minConn);
    dataSource.setConnectionTimeout(connTimeOut);
    if (!poolName.equals("HikariDefined"))
    {
    	dataSource.setPoolName(poolName);
    }
    
       
    if (validationSQL == null || validationSQL.isEmpty())
    {
    	OpenRate.getOpenRateFrameworkLog().warning("No SQL validation statement found for Datasource <" + dataSourceName + ">");
    }
    else
    {
      dataSource.setConnectionTestQuery(validationSQL);
           
      // Test the data source
      try 
      {
        Connection testConn = dataSource.getConnection();
        PreparedStatement stmt = testConn.prepareStatement(validationSQL);
        stmt.executeQuery();
        
        // tidy up
        stmt.close();
        testConn.close();
      } 
      catch (SQLException ex) 
      {
        message = "Connection test failed for connection <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message,getSymbolicName());
      }
    }
    return dataSource;
  }
  
  /**
   * Set the exception handler for handling any exceptions.
   *
   * @param handler the handler to set
   */
  @Override
  public void setHandler(ExceptionHandler handler)
  {
    this.handler = handler;
  }

  /**
   * @return the SymbolicName
   */
  public String getSymbolicName() 
  {
	return SymbolicName;
  }
	
  /**
   * @param SymbolicName the SymbolicName to set
   */
  public void setSymbolicName(String SymbolicName) 
  {
	this.SymbolicName = SymbolicName;
  }
	
  /**
   * @return the handler
   */
  public ExceptionHandler getHandler()
  {
	return handler;
  }
}