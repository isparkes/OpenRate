/* ====================================================================
 * Limited Evaluation License:
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
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.KeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPoolFactory;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Data_Source_Manager'>click here</a> to go to wiki page.
 * <br>
 * <p> 
 * DataSourceBuilderImpl
 *
 */
public class DBCPDataSource implements IDBDataSource
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: DBCPDataSource.java,v $, $Revision: 1.9 $, $Date: 2013-05-13 18:12:12 $";

  /**
   * configuration key for optional SQL string to be run upon new Connection checkout.
   */
  public static final String INIT_SQL_KEY = "InitQuery";

 /**
  * The data source is a a pooled collection of connections that can be used
  * by elements of the framework, taken more or less intact from Apache
  * because it works, and it's stable.
  *
  */
  private static final ILogger FWlog = LogUtil.getLogUtil().getLogger("Framework");

  // refresh data pool when a block is exhausted
  private static byte action = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;

  // reference to the exception handler
  private ExceptionHandler handler;

 /**
  * Constructor: log our version info to the audit system
  */
  public DBCPDataSource()
  {
    // Log ourtselves to the audit map
    AuditUtils.getAuditUtils().buildVersionMap(CVS_MODULE_INFO, this.getClass());
  }

 /**
  * create new data source from provided properties file. Extracts
  * info from properties file & calls buildDataSource.
  *
  * @param ResourceName The name of the DataSourceFactory in the properties
  * @param DataSourceName The name of the data source to create
  * @return The created data source
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public PoolingDataSource getDataSource(String ResourceName, String DataSourceName)
    throws InitializationException
  {
    FWlog.debug("Creating new DataSource <" + DataSourceName + ">");

    String db_url;
    String driver;
    String username;
    String password;

    try
    {
      // get the connection parameters
      db_url     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(DataSourceName,DB_URL_KEY);
      driver     = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(DataSourceName, DRIVER_KEY);
      username   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(DataSourceName, USERNAME_KEY);
      password   = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(DataSourceName, PASSWORD_KEY);

      if (db_url == null)
      {
        FWlog.error("Error recovering data source parameter <db_url> for data source <" + DataSourceName + ">");
        throw new InitializationException("Error recovering data source parameter <db_url> for data source <" + DataSourceName + ">");
      }

      if (driver == null)
      {
        FWlog.error("Error recovering data source parameter <driver> for data source <" + DataSourceName + ">");
        throw new InitializationException("Error recovering data source parameter <driver> for data source <" + DataSourceName + ">");
      }

      if (username == null)
      {
        FWlog.error("Error recovering data source parameter <username> for data source <" + DataSourceName + ">");
        throw new InitializationException("Error recovering data source parameter <username> for data source <" + DataSourceName + ">");
      }

      if (password == null)
      {
        FWlog.error("Error recovering data source parameter <password> for data source <" + DataSourceName + ">");
        throw new InitializationException("Error recovering data source parameter <password> for data source <" + DataSourceName + ">");
      }

      FWlog.info("Creating DataSource <" + DataSourceName + "> using driver <" + driver + "> from URL <" + db_url + ">");

      Class driverClass = Class.forName(driver);
      FWlog.debug("jdbc driver loaded. name = <" + driverClass.getName() + ">");
    }
    catch (ClassNotFoundException cnfe)
    {
      FWlog.error(cnfe.toString());
      throw new InitializationException(cnfe.toString());
    }

    PoolingDataSource dataSource = buildDataSource(DataSourceName, db_url, username, password, ResourceName);

    return dataSource;
  }

 /**
  * build data source.
  */
  private PoolingDataSource buildDataSource(String DataSourceName, String db_url, String username, String password, String ResourceName)
    throws InitializationException
  {
    GenericObjectPool pool = buildObjectPool(DataSourceName);

    // Factory to create Connections.
    ConnectionFactory conFactory = new DriverManagerConnectionFactory(db_url,
        username, password);

    // Wrapper factory that allows execution of initialization SQL
    String initSQL = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(DataSourceName, INIT_SQL_KEY);

    if (initSQL == null)
    {
      // it is unlikely that we will be able to open a connection without
      // some form of initialisation. So we'll warn
      FWlog.warning("No SQL init statement found for Datasource <" + DataSourceName +
        ">");
      initSQL = "";
    }

    ConnectionFactory initFactory = new InitializingConnectionFactory(conFactory,initSQL);

    // Pool for prepared statements.
    String  maxStatementsStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(DataSourceName, MAX_STMTS_KEY,DEFAULT_MAX_STMTS);
    int     maxStatements    = Integer.parseInt(maxStatementsStr);
    KeyedObjectPoolFactory kopf             = new StackKeyedObjectPoolFactory(null,
        maxStatements, 0);

    /*
     * try to find a validation query specific to this datasource. If not
     * found, try to find a generic one. The validation query should return
     * at least one row. If you do not want to test the connection, the value
     * should be set to null.
     */
    String validationSQL = PropertyUtils.getPropertyUtils().getDataSourcePropertyValue(DataSourceName, VALIDATION_QUERY_KEY);

    if (validationSQL == null)
    {
      FWlog.warning("No SQL validation statement found for Datasource <" +
        DataSourceName + ">");
      validationSQL = "";
    }

    try
    {
      PoolableConnectionFactory poolFactory = new PoolableConnectionFactory(initFactory,
          pool, kopf, validationSQL, /* validation query */
          false, /* default read only */
          true /* default auto-commit */);
      FWlog.debug("poolableConnectionFactory built = " +
        poolFactory.getClass().getName());
    }
    catch (Exception e)
    {
      FWlog.error("Configuration error. PoolableConnectionFactory" +
        "could not be created successfully.");
      throw new InitializationException(e.toString());
    }

    // Finally, the actual DataSource. (our own version w/ a get
    // method for pool retrieval)
    PoolingDataSource dataSource = new PoolingDataSource(pool);

    return dataSource;
  }

  /**
   * buildObjectPool creates a pool of connection objects
   *
   * @param dataSourceName The data source name to build the pool for
   * @return The pool
   * @throws InitializationException  
   */
  protected GenericObjectPool buildObjectPool(String dataSourceName)
    throws InitializationException
  {
    try
    {
      String maxSizeStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MAX_SIZE_KEY, DEFAULT_MAX_SIZE);
      int    maxSize    = Integer.parseInt(maxSizeStr);
      String maxWaitStr = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,MAX_WAIT_KEY, DEFAULT_MAX_WAIT);
      int    maxWait    = Integer.parseInt(maxWaitStr);

      // default maxIdle to maxSize
      // having maxIdle less than maxSize could potentially
      // cause the driver to exceed max connections in database.
      int     maxIdle       = maxSize;
      boolean testOnBorrow  = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,TEST_ON_BORROW, DEFAULT_TEST_ON_BORROW).equalsIgnoreCase("true");
      boolean testOnReturn  = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,TEST_ON_RETURN, DEFAULT_TEST_ON_RETURN).equalsIgnoreCase("true");
      boolean testWhileIdle = PropertyUtils.getPropertyUtils().getDataSourcePropertyValueDef(dataSourceName,TEST_WHILE_IDLE, DEFAULT_TEST_WHILE_IDLE).equalsIgnoreCase("true");

      // Pool for storing Connection objects.
      GenericObjectPool pool = new GenericObjectPool(null);
      pool.setMaxActive(maxSize);
      pool.setMaxIdle(maxIdle);
      pool.setMaxWait(maxWait);
      pool.setWhenExhaustedAction(action);
      pool.setTestOnBorrow(testOnBorrow);
      pool.setTestOnReturn(testOnReturn);
      pool.setTestWhileIdle(testWhileIdle);

      return pool;
    }
    catch (NumberFormatException nfe)
    {
      FWlog.error("NumberFormatException. e = " + nfe);
      throw new InitializationException(nfe);
    }
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
