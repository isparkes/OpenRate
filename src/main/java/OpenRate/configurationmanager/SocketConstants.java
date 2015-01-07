/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
