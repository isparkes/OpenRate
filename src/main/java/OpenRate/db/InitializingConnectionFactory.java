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
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.commons.dbcp.ConnectionFactory;

/**
 * Wrapper/Decorator Factory which allows you to run an arbitrary
 * SQL statement after each connection is created. This can be
 * useful for calling "use schema ?" or or any number of other
 * connection intialization statements before passing the
 * connection back to the client.
 *
 */
public class InitializingConnectionFactory implements ConnectionFactory
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: InitializingConnectionFactory.java,v $, $Revision: 1.26 $, $Date: 2013-05-13 18:12:12 $";

  private ILogger FWLog = LogUtil.getLogUtil().getLogger("Framework");
  private ConnectionFactory factory = null;
  private String        sql         = null;

  /**
   * Constructor - the Connection factory you wish to supplement.
   * (can be any type), and the SQL which you wish to run after
   * each connection is opened. If the SQL String is null, nothing is done
   * and the connection is returned.
   *
   * @param factory The connection factory object
   * @param sql The SQL
   */
  public InitializingConnectionFactory(ConnectionFactory factory, String sql)
  {
    this.factory   = factory;
    this.sql       = sql;

    AuditUtils.getAuditUtils().buildVersionMap(CVS_MODULE_INFO, this.getClass());
  }

  /**
   * Create the connection using the provided connection factory. After
   * the connection is created, call the initialization statement to
   * "prepare" the connection. Then return it. If a null or empty SQL
   * string is passed, no statement is executed.
   *
   * @return The connection
   * @throws java.sql.SQLException
   * @see org.apache.commons.dbcp.ConnectionFactory#createConnection()
   */
  @Override
  public Connection createConnection() throws SQLException
  {
    Connection conn;

    try
    {
      conn = factory.createConnection();

      if (conn == null)
      {
        FWLog.error("Cannot open the connection.");
        throw new SQLException("createConnection() returns null");
      }
    }
    catch (SQLException | NullPointerException e)
    {
      FWLog.error("Cannot open the connection to DB.");
      throw e;
    }
    catch (Exception e)
    {
      FWLog.error("Error opening the connection to DB.");
      throw new SQLException("Error opening connection to DB <" + e.getMessage() +">");
    }

    PreparedStatement stmt = null;

    try
    {
      if ((conn != null) && (sql != null) && (sql.length() > 0))
      {
        stmt = conn.prepareStatement(sql);
        stmt.execute();
      }
    }
    catch (SQLException e)
    {
      FWLog.error("Error validating connection. Message <" + e.getMessage() + ">");
      if (stmt != null)
      {
        try
        {
          stmt.close();
        }
        catch (SQLException Sex)
        {
          FWLog.error("Exception closing statement in createConnection. Message <" + Sex.getMessage() + ">");
        }
      }
    }

    return conn;
  }
}
