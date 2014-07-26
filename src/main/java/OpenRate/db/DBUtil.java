/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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

import OpenRate.OpenRate;
import OpenRate.resource.DataSourceFactory;
import OpenRate.exception.InitializationException;
import OpenRate.resource.ResourceContext;
import java.sql.*;
import javax.sql.DataSource;

/**
 * Helper to manage database connections
 *
 */
public class DBUtil
{
  // module symbolic name: set during initialisation
  private static String symbolicName = "Unknown";

  /**
   * Get a specific DataSource
   *
   * @param dataSourceName The data source name to get
   * @return The DataSource object
   * @throws OpenRate.exception.ConfigurationException
   */
  private static DataSource getDataSource(String dataSourceName) throws InitializationException
  {
    ResourceContext   ctx     = new ResourceContext();

    // ToDo: Find a better way of referencing the DS Builder without a literal
    DataSourceFactory factory = (DataSourceFactory) ctx.get(DataSourceFactory.RESOURCE_KEY);

    if (factory == null)
    {
      OpenRate.getOpenRateFrameworkLog().error("DataSourceFactory invalid.");
      throw new InitializationException("unable to load datasourcefactory.",symbolicName);
    }

    DataSource ds = factory.getDataSource(dataSourceName);

    return ds;
  }

  /**
   * helper method for closing connection objects. Ignores exceptions that
   * occur during the close() method.
   *
   * @param conn The connection to close
   */
  public static void close(Connection conn)
  {
    if (conn != null)
    {
      try
      {
        conn.close();
      }
      catch (SQLException Sex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error closing connection. Message <" + Sex.getMessage() + ">");
      }
    }
  }

  /**
   * helper method for closing statement objects. Ignores exceptions that
   * occur during the close() method.
   *
   * @param statement The statement to close
   */
  public static void close(Statement statement)
  {
    if (statement != null)
    {
      try
      {
        statement.close();
      }
      catch (SQLException Sex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error closing statement. Message <" + Sex.getMessage() + ">");
      }
    }
  }

  /**
   * helper method for closing result set objects. Ignores exceptions that
   * occur during the close() method.
   *
   * @param rs The result set to close
   */
  public static void close(ResultSet rs)
  {
    if (rs != null)
    {
      try
      {
        rs.close();
      }
      catch (SQLException Sex)
      {
        OpenRate.getOpenRateFrameworkLog().error("Error closing result set. Message <" + Sex.getMessage() + ">");
      }
    }
  }

 /**
  * Initialise the data source - intended for use in startup. Once the
  *
  * @param dataSourceName The data source to initialise
  * @return The initialised DataSource object
  */
  public static DataSource initDataSource(String dataSourceName) throws InitializationException
  {
    DataSource ds;

    // try and get the parsed datasource from the data pool
    ds = DBUtil.getDataSource(dataSourceName);

    return ds;
  }

  /**
   * This method gets a connection from the named data source, testing it
   * before we return it.
   *
   * We offer an SQL statement to test the connection, and if we can execute
   * it without problems, we assume that the connection is configured OK.
   *
   * @param dataSourceName the data source name to open
   * @return The JDBC connection
   * @throws SQLException
   */
  public static Connection getConnection(String dataSourceName) throws InitializationException
  {
    Connection tmpConn = null;

    // get the reference to the DataSource
    DataSource JDBCds = getDataSource(dataSourceName);

    // Now try to use the connection that we have, if at any point we get an
    // error, we know it is time to go home, and give diagnostics as best
    // we can
    int retries = 0;

    // deal with the case that we do not know the data source
    if (JDBCds == null)
    {
      String message = "Data source <" + dataSourceName + "> not known.";
      throw new InitializationException(message,symbolicName);
    }

    // try to get a connection from the data source
    while (tmpConn == null & retries < 3)
    {
      try {
        tmpConn = JDBCds.getConnection();
      } catch (SQLException ex) {
        String message = "Exception getting Data source connection <" + dataSourceName + "> not known.";
        throw new InitializationException(message,ex,symbolicName);
      }

      // Increment the retries, we don't want to do this forever
      retries++;
      try {
        if (tmpConn.isClosed())
        {
          String message = "Data source <" + dataSourceName + "> provided a closed connection.";
          throw new InitializationException(message,symbolicName);
        }
      } catch (SQLException ex) {
        String message = "Exception checking Data source connection <" + dataSourceName + ">.";
        throw new InitializationException(message,ex,symbolicName);
      }
    }

    return tmpConn;
  }

 /**
  * Prepare a statement, passed as a string. The statement will be pooled in
  * the connection pool.
  *
  * @param JDBCcon The connection to prepare the statement for
  * @param StatementToPrep The statement to prepare
  * @return The prepared statement
  * @throws InitializationException
  */
  public static PreparedStatement prepareStatement(Connection JDBCcon,
                                                    String StatementToPrep)
    throws InitializationException
  {
    PreparedStatement tmpPrepStmt;

    try
    {
      // prepare the SQL for the Insert Statement
      tmpPrepStmt = JDBCcon.prepareStatement(StatementToPrep,
                                             ResultSet.TYPE_SCROLL_INSENSITIVE,
                                             ResultSet.CONCUR_UPDATABLE);
    }
    catch (SQLException | NullPointerException ex)
    {
      OpenRate.getOpenRateFrameworkLog().error("Error preparing the statement <" + StatementToPrep + ">");
      throw new InitializationException("Error preparing the statement <" +
                                        StatementToPrep + ">",ex,symbolicName);
    }

    return tmpPrepStmt;
  }

    /**
     * @return the SymbolicName
     */
    public static String getSymbolicName() {
        return symbolicName;
    }

    /**
     * @param SymbolicName the SymbolicName to set
     */
    public static void setSymbolicName(String SymbolicName) {
        symbolicName = SymbolicName;
    }

}