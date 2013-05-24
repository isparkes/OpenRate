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

package OpenRate.db;

import OpenRate.audit.AuditUtils;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Data_Source_Manager'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * DataSourceBuilderImpl
 *
 */
public class C3P0DataSource implements IDBDataSource
{
 /**
  * The data source is a a pooled collection of connections that can be used
  * by elements of the framework, taken more or less intact from Apache
  * because it works, and it's stable.
  *
  */
  private static final ILogger FWlog = LogUtil.getLogUtil().getLogger("Framework");

  // reference to the exception handler
  private ExceptionHandler handler;

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
  * Constructor: log our version info to the audit system
  */
  public C3P0DataSource()
  {
    // Log ourselves to the audit map
    AuditUtils.getAuditUtils().buildVersionMap(this.getClass());
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
    String  maxStatementsStr;
    int     maxStatements;
    String  maxStatementPerConnectionStr;
    int     maxStatementPerConnection;
    String validationSQL;
    String  testConnPeriodStr;
    int     testConnPeriod;

    ComboPooledDataSource dataSource = new ComboPooledDataSource();

    FWlog.debug("Creating new DataSource <" + dataSourceName + ">");

    try
    {
      // get the connection parameters
      db_url     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DB_URL_KEY);
      driver     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, DRIVER_KEY);
      username   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, USERNAME_KEY);
      password   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, PASSWORD_KEY);

      if (db_url == null || db_url.isEmpty())
      {
        String Message = "Error recovering data source parameter <db_url> for data source <" + dataSourceName + ">";
        FWlog.error(Message);
        throw new InitializationException(Message);
      }

      if (driver == null || driver.isEmpty())
      {
        String Message = "Error recovering data source parameter <driver> for data source <" + dataSourceName + ">";
        FWlog.error(Message);
        throw new InitializationException(Message);
      }

      if (username == null || username.isEmpty())
      {
        String Message = "Error recovering data source parameter <username> for data source <" + dataSourceName + ">";
        FWlog.error(Message);
        throw new InitializationException(Message);
      }

      if (password == null)
      {
        String Message = "Error recovering data source parameter <password> for data source <" + dataSourceName + ">";
        FWlog.error(Message);
        throw new InitializationException(Message);
      }

      FWlog.info("Creating DataSource <" + dataSourceName + "> using driver <" + driver + "> from URL <" + db_url + ">");

      Class<?> driverClass = Class.forName(driver);
      FWlog.debug("jdbc driver loaded. name = <" + driverClass.getName() + ">");
    }
    catch (ClassNotFoundException cnfe)
    {
      String Message = "Driver class <" + driver + "> not found for data source <" + dataSourceName + ">";
      FWlog.error(Message);
      throw new InitializationException(Message);
    }

    try
    {
      dataSource.setDriverClass(driver);
    }
    catch (PropertyVetoException ex)
    {
      String Message = "Property veto for driver  <" + driver + "> for data source <" + dataSourceName + ">";
      FWlog.error(Message);
      throw new InitializationException(Message);
    }

    validationSQL = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(dataSourceName, VALIDATION_QUERY_KEY);

    boolean testOnBorrow  = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,TEST_ON_BORROW, DEFAULT_TEST_ON_BORROW).equalsIgnoreCase("true");
    boolean testOnReturn  = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,TEST_ON_RETURN, DEFAULT_TEST_ON_RETURN).equalsIgnoreCase("true");

    String maxSizeStr     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MAX_SIZE_KEY, DEFAULT_MAX_SIZE);
    int    maxSize        = Integer.parseInt(maxSizeStr);

    String minSizeStr     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MIN_SIZE_KEY, DEFAULT_MIN_SIZE);
    int    minSize        = Integer.parseInt(minSizeStr);

    // Pool culling management
    String maxIdleTimeStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MAX_IDLE_KEY, DEFAULT_MAX_IDLE);
    int    maxIdleTime    = Integer.parseInt(maxIdleTimeStr);

    // Options with defaults
    maxStatementsStr      = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, MAX_STMTS_KEY,DEFAULT_MAX_STMTS);
    maxStatements         = Integer.parseInt(maxStatementsStr);
    testConnPeriodStr     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, TEST_CONN_KEY,DEFAULT_TEST_CONN);
    testConnPeriod        = Integer.parseInt(testConnPeriodStr);

    maxStatementPerConnectionStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName, MAX_STMTS_PER_CONNECTION_KEY ,DEFAULT_MAX_STMTS);
    maxStatementPerConnection    = Integer.parseInt(maxStatementPerConnectionStr);

    String connTimeOut    = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,CONN_TIMEOUT_KEY, DEFAULT_CONN_TIMEOUT);
    int    timeoutPeriod  = Integer.parseInt(connTimeOut) * 1000;

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

    if (validationSQL == null || validationSQL.isEmpty())
    {
      FWlog.warning("No SQL validation statement found for Datasource <" + dataSourceName + ">");
    }
    else
    {
      dataSource.setPreferredTestQuery(validationSQL);

      // Test the data source
      try
      {
        try (Connection testConn = dataSource.getConnection(); PreparedStatement stmt = testConn.prepareStatement(validationSQL)) {
          stmt.executeQuery();
        }

        FWlog.debug("Data source <" + dataSourceName + "> num_connections: " + dataSource.getNumConnectionsDefaultUser());
        FWlog.debug("Data source <" + dataSourceName + "> max_pool:        " + dataSource.getMaxPoolSize());
        FWlog.debug("Data source <" + dataSourceName + "> min_pool:        " + dataSource.getMinPoolSize());
      }
      catch (SQLException ex)
      {
        String Message = "Connection test failed for connection <" + dataSourceName + ">";
        throw new InitializationException(Message);
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
}
