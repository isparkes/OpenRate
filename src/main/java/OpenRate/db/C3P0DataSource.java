
package OpenRate.db;

import OpenRate.OpenRate;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.utils.PropertyUtils;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;

/**
 * The data source is a a pooled collection of connections that can be used by
 * elements of the framework, taken more or less intact from Apache because it
 * works, and it's stable.
 *
 * Please
 * <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Data_Source_Manager'>click
 * here</a> to go to wiki page.
 * <br>
 * <p>
 * DataSourceBuilderImpl
 *
 */
public class C3P0DataSource implements IDBDataSource {

  // reference to the exception handler
  private ExceptionHandler handler;

  // The symbolic name is used in the management of the pipeline (control and
  // thread monitoring) and logging.
  private String SymbolicName = "C3P0DataSource";

  // used to simplify logging and exception handling
  public String message;

  // -----------------------------------------------------------------------------
  // ------------------ C3P0 specific configuration options  ---------------------
  // -----------------------------------------------------------------------------
  /**
   * max number of Statements to be stored per connection.
   */
  public static final String TEST_CONN_KEY = "TestConnectionPeriod";

  /**
   * max number of Statements to be stored per connection default value
   */
  public static final String DEFAULT_TEST_CONN = "600";

  // -----------------------------------------------------------------------------
  // --------------------------- Implementation  ---------------------------------
  // -----------------------------------------------------------------------------
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
          throws InitializationException {
    String db_url;
    String driver = null;
    String username;
    String password;
    String maxStatementsStr;
    int maxStatements;
    String maxStatementPerConnectionStr;
    int maxStatementPerConnection;
    String validationSQL;
    String testConnPeriodStr;
    int testConnPeriod;

    // Suppress the log messages
    Properties p = new Properties(System.getProperties());
    p.put("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
    p.put("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "OFF");
    System.setProperties(p);

    ComboPooledDataSource dataSource = new ComboPooledDataSource();

    OpenRate.getOpenRateFrameworkLog().debug("Creating new DataSource <" + dataSourceName + ">");

    try {
      // get the connection parameters
      db_url = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DB_URL_KEY);
      driver = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DRIVER_KEY);
      username = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, USERNAME_KEY);
      password = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, PASSWORD_KEY);

      if (db_url == null || db_url.isEmpty()) {
        message = "Error recovering data source parameter <db_url> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }

      if (driver == null || driver.isEmpty()) {
        message = "Error recovering data source parameter <driver> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }

      if (username == null || username.isEmpty()) {
        message = "Error recovering data source parameter <username> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }

      if (password == null) {
        message = "Error recovering data source parameter <password> for data source <" + dataSourceName + ">";
        OpenRate.getOpenRateFrameworkLog().error(message);
        throw new InitializationException(message, getSymbolicName());
      }

      OpenRate.getOpenRateFrameworkLog().info("Creating DataSource <" + dataSourceName + "> using driver <" + driver + "> from URL <" + db_url + ">");

      Class<?> driverClass = Class.forName(driver);
      OpenRate.getOpenRateFrameworkLog().debug("jdbc driver loaded. name = <" + driverClass.getName() + ">");
    } catch (ClassNotFoundException cnfe) {
      message = "Driver class <" + driver + "> not found for data source <" + dataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    try {
      dataSource.setDriverClass(driver);
    } catch (PropertyVetoException ex) {
      message = "Property veto for driver  <" + driver + "> for data source <" + dataSourceName + ">";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message, getSymbolicName());
    }

    validationSQL = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, VALIDATION_QUERY_KEY);

    boolean testOnBorrow = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, TEST_ON_BORROW, DEFAULT_TEST_ON_BORROW).equalsIgnoreCase("true");
    boolean testOnReturn = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, TEST_ON_RETURN, DEFAULT_TEST_ON_RETURN).equalsIgnoreCase("true");

    String maxSizeStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, MAX_SIZE_KEY, DEFAULT_MAX_SIZE);
    int maxSize = Integer.parseInt(maxSizeStr);

    String minSizeStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, MIN_SIZE_KEY, DEFAULT_MIN_SIZE);
    int minSize = Integer.parseInt(minSizeStr);

    // Pool culling management
    String maxIdleTimeStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, MAX_IDLE_KEY, DEFAULT_MAX_IDLE);
    int maxIdleTime = Integer.parseInt(maxIdleTimeStr);

    // Options with defaults
    maxStatementsStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, MAX_STMTS_KEY, DEFAULT_MAX_STMTS);
    maxStatements = Integer.parseInt(maxStatementsStr);
    testConnPeriodStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, TEST_CONN_KEY, DEFAULT_TEST_CONN);
    testConnPeriod = Integer.parseInt(testConnPeriodStr);

    maxStatementPerConnectionStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, MAX_STMTS_PER_CONNECTION_KEY, DEFAULT_MAX_STMTS);
    maxStatementPerConnection = Integer.parseInt(maxStatementPerConnectionStr);

    String connTimeOut = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, CONN_TIMEOUT_KEY, DEFAULT_CONN_TIMEOUT);
    int timeoutPeriod = Integer.parseInt(connTimeOut) * 1000;

    // Perform the initialisation
    dataSource.setJdbcUrl(db_url);
    dataSource.setUser(username);
    dataSource.setPassword(password);

    // Pooling configuration
    dataSource.setMaxStatements(maxStatements);
    dataSource.setInitialPoolSize(minSize);
    dataSource.setMaxPoolSize(maxSize);
    dataSource.setMinPoolSize(minSize);
    dataSource.setMaxIdleTime(maxIdleTime);
    dataSource.setTestConnectionOnCheckout(testOnBorrow);
    dataSource.setTestConnectionOnCheckin(testOnReturn);
    dataSource.setIdleConnectionTestPeriod(testConnPeriod);
    dataSource.setCheckoutTimeout(timeoutPeriod);
    dataSource.setMaxStatementsPerConnection(maxStatementPerConnection);

    if (validationSQL == null || validationSQL.isEmpty()) {
      OpenRate.getOpenRateFrameworkLog().warning("No SQL validation statement found for Datasource <" + dataSourceName + ">");
    } else {
      dataSource.setPreferredTestQuery(validationSQL);

      // Test the data source
      try {
        try (Connection testConn = dataSource.getConnection(); PreparedStatement stmt = testConn.prepareStatement(validationSQL)) {
          stmt.executeQuery();
        }

        OpenRate.getOpenRateFrameworkLog().debug("Data source <" + dataSourceName + "> num_connections: " + dataSource.getNumConnectionsDefaultUser());
        OpenRate.getOpenRateFrameworkLog().debug("Data source <" + dataSourceName + "> max_pool:        " + dataSource.getMaxPoolSize());
        OpenRate.getOpenRateFrameworkLog().debug("Data source <" + dataSourceName + "> min_pool:        " + dataSource.getMinPoolSize());
      } catch (SQLException ex) {
        message = "Connection test failed for connection <" + dataSourceName + ">";
        throw new InitializationException(message, ex, getSymbolicName());
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
  public void setHandler(ExceptionHandler handler) {
    this.handler = handler;
  }

  /**
   * @return the handler
   */
  public ExceptionHandler getHandler() {
    return handler;
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
}
