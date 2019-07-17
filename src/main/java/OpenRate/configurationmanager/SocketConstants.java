

package OpenRate.configurationmanager;

/**
 * @author = g.z.
 *
 * This object holds static final String objects used by the socket listener<br/>
 * classes
 */
public class SocketConstants
{
  /**
   * This is the response displayed when user or admin issues the command 'help'
   */
  public static final String HELPMESSAGE = "\t\r\n" +
    "Help (H)          Shows this message.\r\n" +
    "Quit (Q)          Terminates the admin console.\r\n" +
    "Exit (X)          Terminates the admin console.\r\n" +
    "ListModules (M)   Shows the modules available in the framework.\r\n" +
    "ListCommands (C)  Shows the modules and commands.\r\n" +
    "ThreadStatus (S)  Shows the status of all threads.\r\n";

  /**
   * This is the response displayed when the command issued is unknown to the
   * protocol
   */
  public static final String UNKNOWNCOMMAND = " is not a recognized command. " +
    "Type 'help' for more information.\r\n";

  /**
   * This is the response displayed when the listener has reached the
   *  maximum allowed number of connection and new connections are attempted
   */
  public static final String CONNECTIONMAXMESSAGE = "The allowed number of " +
    "connection has been reached. Please try again later. Thanks!";

  /**
   * The long help command
   */
  public static final String HELP = "Help";

  /**
   * The short help command
   */
  public static final String HELP_SHORT = "H";

  /**
   * The long quit command
   */
  public static final String QUIT = "Quit";

  /**
   * The short quit command
   */
  public static final String QUIT_SHORT = "Q";

  /**
   * The long exit command
   */
  public static final String EXIT = "Exit";

  /**
   * The short exit command
   */
  public static final String EXIT_SHORT = "X";

  /**
   * The long list modules command
   */
  public static final String LISTMODULES = "ListModules";

  /**
   * The short list modules command
   */
  public static final String LISTMODULES_SHORT = "M";

  /**
   * The long list commands command
   */
  public static final String LISTCOMMANDS = "ListCommands";

  /**
   * The short list commands command
   */
  public static final String LISTCOMMANDS_SHORT = "C";

  /**
   * The long thread status command
   */
  public static final String THREADSTATUS = "ThreadStatus";

  /**
   * The short thread status command
   */
  public static final String THREADSTATUS_SHORT = "S";
  /**
   * The long GUI mode toggle command
   */
  public static final String GUIMODE = "GUIMode";

  /**
   * The goodbye message command
   */
  public static final String GOODBYE = "goodbye!";

  /**
   * The OpenRate command prompt
   */
  public static final String OPENRATETAB = "openrate> ";

  /**
   * properties attributes - port number
   */
  public static final String SOCKETPORT = "Socket.Port";

  /**
   * properties attributes - maximum number of connections
   */
  public static final String SOCKETMAXCONNECTION = "Socket.MaxConnection";

  /**
   * This is the response displayed when command issued is known and has been
   * accepted
   */
  public static final String OKMESSAGE = OPENRATETAB + " OK";

}
