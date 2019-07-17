

package OpenRate.configurationmanager;

import java.io.PrintStream;

/**
 * @author = g.z.
 * This class helps the socket listener execute methods that could be<br/>
 * used by other objects. This usually involves displaying of the <br/>
 * listener's response to the command issued, or manipulation of the <br/>
 * SockectConnectionData object.
 *
 */

public class SocketHelper
{
  /**
   * This method displays the welcome message when an allowed connection <br/>
   * is made.
   *
   * @param out The print stream to display the message on
   */

  public static void displayWelcomeMessage(PrintStream out)
  {
    out.print(appendOpenRate(SocketProtocol.getWelcomeMessage()));
    out.flush();
  }

  /**
   * This method displays the response of the listener depending on the <br/>
   * String output passed in the parameter.
   *
   * @param out The print stream to display the message on
   * @param output The message to display
   */

  public static void displayResponse(PrintStream out, String output)
  {
    out.print(appendOpenRate(output));
    out.flush();
  }

  /**
   * This method assists the display***() methods by appending a String <br/>
   * 'openrate> ' to the response for display purposes.
   * @param output
   * @return
   */
  private static String appendOpenRate(String output)
  {
    return output.concat(SocketConstants.OPENRATETAB);
  }

  /**
   * This method displays the quit message when user or admin quits from<br/>
   * the console.
   * @param out
   */
  public static void displayQuitMessage(PrintStream out)
  {
    out.println(SocketConstants.OPENRATETAB.concat(SocketConstants.GOODBYE));
  }

  /**
   * This method deducts from the number of connection from the <br/>
   * SocketConnectionData.
   * @param socData
   */
  public static void removeAConnectionCount(SocketConnectionData socData)
  {
    int conNum = socData.getConnectionNumber();
    socData.setConnectionNumber(--conNum);
  }

  /**
   * This method adds 1 to the number of connection from the <br/>
   * SocketConnectionData.
   * @param socData
   */
  public static void addAConnectionCount(SocketConnectionData socData)
  {
    int conNum = socData.getConnectionNumber();
    socData.setConnectionNumber(++conNum);
  }
}
