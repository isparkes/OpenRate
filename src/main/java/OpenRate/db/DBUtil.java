

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