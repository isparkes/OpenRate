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
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: IDBDataSource.java,v $, $Revision: 1.28 $, $Date: 2013-05-13 18:12:12 $";

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

