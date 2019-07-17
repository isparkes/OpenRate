

package OpenRate.db;

import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import javax.sql.DataSource;

/**
 * Abstracts the creation of the DataSource from the resource management. This
 * will allow people to use different DataSource implementations and vary
 * the configuration easily.
 *
 */
public interface IDBDataSource
{
  // -----------------------------------------------------------------------------
  // -------------------------- configuration options  ---------------------------
  // -----------------------------------------------------------------------------

  /**
   * the config key for the DB url.
   */
  public static final String DB_URL_KEY = "db_url";

  /**
   * the config key for the jdbc driver name.
   */
  public static final String DRIVER_KEY = "driver";

  /**
   * the config key for the db user name.
   */
  public static final String USERNAME_KEY = "username";

  /**
   * the config key for the db password.
   */
  public static final String PASSWORD_KEY = "password";

 /** This is part of the outgoing model - we are going to remove this soon
  * Keeping it at the moment because it seems that the data pool class
  * requires it. Need to do a bit of research to see if we can get rid of it
  */
  public static final String VALIDATION_QUERY_KEY = "ValidationQuery";

   /**
   * max number of Statements to be stored per connection.
   */
  public static final String MAX_STMTS_KEY = "MaxStatements";

    /**
     * max number of statement to be stored per connection
     */
  public static final String  MAX_STMTS_PER_CONNECTION_KEY = "MaxStatementsPerConnection";

  /**
   * max number of Statements to be stored per connection default value
   */
  public static final String DEFAULT_MAX_STMTS = "25";

  /**
   * flag on whether or not to test connections when borrowed from the pool.
   */
  public static final String TEST_ON_BORROW = "TestOnBorrow";

  /**
   * flag on whether or not to test connections when borrowed from the pool default
   */
  public static final String DEFAULT_TEST_ON_BORROW = "false";

  /**
   * flag on whether or not to test connections when returned to the pool.
   */
  public static final String TEST_ON_RETURN = "TestOnReturn";

  /**
   * flag on whether or not to test connections when returned to the pool default
   */
  public static final String DEFAULT_TEST_ON_RETURN = "true";

  /**
   * flag on whether or not to test connections in the pool at some interval.
   */
  public static final String TEST_WHILE_IDLE = "TestWhileIdle";

  /**
   * flag on whether or not to test connections in the pool at some interval default
   */
  public static final String DEFAULT_TEST_WHILE_IDLE = "true";

  /**
   * The max size of the DB connection pool
   */
  public static final String MAX_SIZE_KEY = "MaxPoolSize";

  /**
   * The max size of the DB connection pool default value
   */
  public static final String DEFAULT_MAX_SIZE = "10";

  /**
   * The min size of the DB connection pool
   */
  public static final String MIN_SIZE_KEY = "MinPoolSize";

  /**
   * The max size of the DB connection pool default value
   */
  public static final String DEFAULT_MIN_SIZE = "3";

  /**
   * the max wait for a connection before timing out & throwing an exception
   */
  public static final String MAX_WAIT_KEY = "MaxWait";

  /**
   * the max wait for a connection before timing out & throwing an exception default value
   */
  public static final String DEFAULT_MAX_WAIT = "-1"; /* block forever */

  /**
   * the max idle time for a connection in the pool before it is closed.
   */
  public static final String MAX_IDLE_KEY = "IdleBeforeClose";

  /**
   * the max idle time for a connection in the pool before it is closed default value
   */
  public static final String DEFAULT_MAX_IDLE = "5";

  /**
   * the initial size of the connection pool. (# of connections checked out
   * immediately)
   */
  public static final String INIT_SIZE_KEY = "InitialPoolSize";

  /**
   * the initial size of the connection pool default value
   */
  public static final String DEFAULT_INIT_SIZE = "1";

  /**
   * The timeout we will apply for connections
   */
  public static final String CONN_TIMEOUT_KEY = "ConnectionTimeout";

  /**
   * The default timeout in seconds
   */
  public static final String DEFAULT_CONN_TIMEOUT = "5";

  // -----------------------------------------------------------------------------
  // ------------------------------ Interface  -----------------------------------
  // -----------------------------------------------------------------------------

 /**
   * build a new data source using the properties provided. for the given
   * data source name.
   *
   * @param ResourceName The Name of the DataSourceFactory in the properties
   * @param DataSourceName The name of the data source we are to create
   * @return The created data source
   * @throws OpenRate.exception.InitializationException
   */
  public DataSource getDataSource(String ResourceName, String DataSourceName)
    throws InitializationException;

  /**
   * Set the exception handler for handling any exceptions.
   *
   * @param handler the handler to set
   */
  public void setHandler(ExceptionHandler handler);
}

