

package OpenRate.db;

import OpenRate.OpenRate;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.utils.PropertyUtils;
import com.jolbox.bonecp.BoneCPDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

public class BCPDataSource implements IDBDataSource
{
  // The symbolic name is used in the management of the pipeline (control and
  // thread monitoring) and logging.
  private String SymbolicName = "BCPDataSource";

  // reference to the exception handler
  private ExceptionHandler handler;

  // used to simplify logging and exception handling
  public String message;
  
 /**
  * The data source is a a pooled collection of connections that can be used
  * by elements of the framework.
  *
  * * @author ddijak
  */

  // -----------------------------------------------------------------------------
  // ------------------ BoneCP specific configuration options  ---------------------
  // -----------------------------------------------------------------------------

  /**
   * minimal number of connections per partition
   *  Sets the minimum number of connections that will be contained in every partition.
   */
  public static final String MIN_CONN_KEY = "MinConnectionsPerPartiton";
  public static final String DEFAULT_MIN_CONN = "5";

  /**
   * maximal number of connections per partition
   *  Sets the maximum number of connections that will be contained in every partition. Setting this to 5 with 3 partitions means you will have 15 unique connections to the database.
   *  Note that the connection pool will not create all these connections in one go but rather start off with minConnectionsPerPartition and gradually increase connections as required.
   */
  public static final String MAX_CONN_KEY = "MaxConnectionsPerPartiton";
  public static final String DEFAULT_MAX_CONN = "10";

  /**
   * number of partitions
   *  Sets number of partitions to use. In order to reduce lock contention and thus improve performance, each incoming connection request picks off a connection from a pool that has thread-affinity,
   *  i.e. pool[threadId % partition_count]. The higher this number, the better your performance will be for the case when you have plenty of short-lived threads. Beyond a certain threshold,
   *  maintenance of these pools will start to have a negative effect on performance (and only for the case when connections on a partition start running out).
   *  Default: 1, minimum: 1, recommended: 2-4 (but very app specific)
   */
  public static final String PARTITION_KEY = "PartitionCount";
  public static final String DEFAULT_PARTITON_COUNT = "1";

  /**
   * number of acquire increments
   *  Sets the acquireIncrement property. When the available connections are about to run out, BoneCP will dynamically create new ones in batches.
   *  This property controls how many new connections to create in one go (up to a maximum of maxConnectionsPerPartition).
   *  Note: This is a per partition setting.
   */
  public static final String ACQUIRE_INCREMENT_KEY = "AcquireIncrement";
  public static final String DEFAULT_ACQUIRE_INCREMENT_COUNT = "1";

  /**
   * number of cached statements
   *  Sets statementsCacheSize setting. The number of statements to cache.
   */
  public static final String STMT_CACHE_KEY = "StatementsCacheSize";
  public static final String DEFAULT_STMTS_CACHE_SIZE = DEFAULT_MAX_STMTS; // (DEFAULT_MAX_STMTS = 25)

  // -----------------------------------------------------------------------------
  // --------------------------- Implementation  ---------------------------------
  // -----------------------------------------------------------------------------

 /**
  * Constructor: log our version info to the audit system
  */
  public BCPDataSource()
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
    String db_url;
    String driver = null;
    String username;
    String password;
    String validationSQL;
    int MaxConnPerPartition;
    int MinConnPerPartition;
    int AcquireIncrement;
    int StatementsCacheSize;
    int PartitionCount;

    BoneCPDataSource dataSource = new BoneCPDataSource();

    OpenRate.getOpenRateFrameworkLog().debug("Creating new DataSource <" + dataSourceName + ">");

    try
    {
      // get the connection parameters
      db_url     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DB_URL_KEY);
      driver     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DRIVER_KEY);
      username   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, USERNAME_KEY);
      password   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, PASSWORD_KEY);

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

    // Set driver class
      dataSource.setDriverClass(driver);

    // set Test Query

    validationSQL = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, VALIDATION_QUERY_KEY);

    // Options with defaults
    String numberOfPartitions   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,PARTITION_KEY, DEFAULT_PARTITON_COUNT);
    PartitionCount 		  		= Integer.parseInt(numberOfPartitions);

    String minConn 				= PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MIN_CONN_KEY, DEFAULT_MIN_CONN);
    MinConnPerPartition			= Integer.parseInt(minConn);

    String maxConn 				= PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MAX_CONN_KEY, DEFAULT_MAX_CONN);
    MaxConnPerPartition 		= Integer.parseInt(maxConn);

    String acquireInc 			= PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,ACQUIRE_INCREMENT_KEY, DEFAULT_ACQUIRE_INCREMENT_COUNT);
    AcquireIncrement            = Integer.parseInt(acquireInc);
    // Acquire Increment can't be bigger than MaxConnectionsPerPartition
    // If AcquireIncrement is > MaxConnectionsPerPartition then use default value of 1
    if (AcquireIncrement > MaxConnPerPartition)
    {
    	OpenRate.getOpenRateFrameworkLog().warning("AcquireIncrement can't be bigger than MaxConnectionsPerPartition. Setting default value of: "+DEFAULT_ACQUIRE_INCREMENT_COUNT);
    	AcquireIncrement = Integer.parseInt(DEFAULT_ACQUIRE_INCREMENT_COUNT);
    }
    String stmtCacheSize 		= PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,STMT_CACHE_KEY, DEFAULT_STMTS_CACHE_SIZE);
    StatementsCacheSize         = Integer.parseInt(stmtCacheSize);

    String connTimeOut    		= PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,CONN_TIMEOUT_KEY, DEFAULT_CONN_TIMEOUT);
    int    timeoutPeriod  		= Integer.parseInt(connTimeOut);

    // Perform the initialisation
    dataSource.setJdbcUrl(db_url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);

    // Pooling configuration
    dataSource.setMinConnectionsPerPartition(MinConnPerPartition);
    dataSource.setMaxConnectionsPerPartition(MaxConnPerPartition);
    dataSource.setPartitionCount(PartitionCount);
    dataSource.setAcquireIncrement(AcquireIncrement);
    dataSource.setStatementsCacheSize(StatementsCacheSize);
    dataSource.setMaxConnectionAgeInSeconds(timeoutPeriod);

    if (validationSQL == null || validationSQL.isEmpty())
    {
      OpenRate.getOpenRateFrameworkLog().warning("No SQL validation statement found for Datasource <" + dataSourceName + ">");
    }
    else
    {
      dataSource.setConnectionTestStatement(validationSQL);

      // Test the data source
      try
      {
        try (Connection testConn = dataSource.getConnection(); PreparedStatement stmt = testConn.prepareStatement(validationSQL))
        {
          stmt.executeQuery();
        }
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
    public String getSymbolicName() {
        return SymbolicName;
    }

    /**
     * @param SymbolicName the SymbolicName to set
     */
    public void setSymbolicName(String SymbolicName) {
        this.SymbolicName = SymbolicName;
    }

    /**
     * @return the handler
     */
    public ExceptionHandler getHandler() {
        return handler;
    }
}
