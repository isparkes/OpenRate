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

package OpenRate.configurationmanager;

import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author = g.z.
 * This class is a socket listener that would enable administrators to<br/>
 * control the application. They could issue commands like abort, quit, <br/>
 * reload, etc. depending on the number of available commands. <br/>
 * Administrators would just need to connect to the socket using a telnet<br/>
 * client and control the application via the command line interface.
 *
 */
public final class OpenRateSocket implements Runnable, IEventInterface
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: OpenRateSocket.java,v $, $Revision: 1.6 $, $Date: 2013-05-13 18:12:12 $";

  // The port we listen on
  private int port;
  
  // The maximum number of connections we will serve
  private int maxConnections;

  // Access to the logger
  private ILogger FWLog = LogUtil.getLogUtil().getLogger("Framework");

  // List of Services that this Client supports
  private final static String SERVICE_PORT = "Port";
  private final static String DEFAULT_PORT = "8081";
  private final static String SERVICE_CONNECTIONS = "MaxConnection";
  private final static String DEFAULT_MAX_CON = "2";

  // Shows if we are running OK
  static boolean started = false;
  static boolean initialised = false;

  // module symbolic name: set during initalisation
  private String SymbolicName = "OpenRateListener";
  
  // This is the socket for the ECI
  private ServerSocket serverSocket;
  
  /**
   * Default constructor with Properties object as a parameter. This
   * sets the configured maximum number of allowed connection at a time
   * and the port that the socket would bind itself to.
   *
   * @param ResourceName The name of the resource
   */
  public OpenRateSocket(String ResourceName) throws InitializationException
  {
    String ConfigHelper;

    try
    {
      ConfigHelper = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"Port",DEFAULT_PORT);
      processControlEvent(SERVICE_PORT,true,ConfigHelper);

      ConfigHelper = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MaxConnections",DEFAULT_MAX_CON);
      processControlEvent(SERVICE_CONNECTIONS,true,ConfigHelper);
      
      // Get the socket
      serverSocket = getServerSocket();
    }
    catch(NumberFormatException nfe)
    {
      String Message = "OpenRateSocket constructor error";
      throw new InitializationException(Message, nfe);
    }
    catch (IOException ie)
    {
      String Message = "OpenRateSocket.getServerSocket(): Could not listen on port <"
        + this.port + ">. Message = <" + ie.getMessage() + ">. Aborting.";
      throw new InitializationException(Message);
    }
  }

 /**
  * Launch the listener thread
  */
  @Override
  public void run()
  {
    if(serverSocket!=null)
    {
      SocketConnectionData socData = new SocketConnectionData();
      Socket socket;
      System.out.println("Listener on port <" + this.port + "> is running...");

      started = true;

      //while connection is still accepted
      while(socData.isLoop())
      {
        socket = getSocket(serverSocket);
        if(socData.getConnectionNumber() < maxConnections)
        {
          //This block happens when connection is still allowed
          SocketHelper.addAConnectionCount(socData);

          SocketListener socLis = new SocketListener(socket,socData);

          //start thread
          Thread t = new Thread(socLis, "Listener");
          t.start();
        }
        else
        {
          //This block is executed if the number of allowed connection has
          //been reached
          blockConnection(socket);
        }
      }
    }
    else
    {
      FWLog.debug("OpenRateSocket.run() error: ServerSocket is null. Could not " +
        "bind to port " + this.port);
    }

    // show that we have done with the init
    initialised = true;
  }

  /**
   * This method returns a ServerSocket object binding to the configured<br/>
   * port number.
   *
   * @return ServerSocket
   */
  private ServerSocket getServerSocket() throws IOException
  {
    // Try to get the socket
    serverSocket = new ServerSocket(this.port);
    
    return serverSocket;
  }

  /**
   * This method returns a Socket object created when the ServerSocket <br/>
   * object accepts the connection.
   *
   * @param serverSocket
   * @return Socket
   */
  private Socket getSocket(ServerSocket serverSocket)
  {
    Socket socket = null;
    try
    {
      socket = serverSocket.accept();
    }
    catch (IOException e)
    {
      FWLog.error("OpenRateSocket.getSocket(): Accept failed.");
    }
    return socket;
  }

 /**
  * Returns the state of the started flag. This is used to check that the
  * listener is active before allowing the framework to start
  *
  * @return true if the socket listener was created correctly
  */
  public boolean getStarted()
  {
    return started;
  }

 /**
  * Returns the state of the initialise flag. This is used to check that the
  * listener is active before allowing the framework to start
  *
  * @return true if the socket listener has finished initialisation
  */
  public boolean getInitialised()
  {
    return initialised;
  }

  /**
   * This method is executed when the maximum number of connection has<br/>
   * been reached and new connections are made. This shows a message <br/>
   * telling the one making the connection that the maximum number of<br/>
   * connection has been reached and to try again later.
   * @param socket
   */
  private void blockConnection(Socket socket)
  {
    PrintStream out = null;
    try
    {
      out = new PrintStream(new BufferedOutputStream(socket
        .getOutputStream(), 1024), false);
      //displays the maximum connection message
      out.println(SocketConstants.CONNECTIONMAXMESSAGE);
      out.flush();
    }
    catch (IOException e)
    {
      FWLog.error("OpenRateSocket.blockConnection() error");
      FWLog.error(e.getClass() + ": " + e.getMessage(), e);
    }
    finally
    {
      try
      {
        //closes the PrintStream and Socket objects
        if(out!=null)
          out.close();
        if(socket!=null)
          socket.close();
      }
      catch(IOException e)
      {
        FWLog.error("OpenRateSocket.blockConnection() finally clause error.",e);
      }
    }
  }

// -----------------------------------------------------------------------------
// ------------- Start of inherited IEventInterface functions ------------------
// -----------------------------------------------------------------------------

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  * @throws InitializationException  
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    //Register this Client
    ClientManager.registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PORT, ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_CONNECTIONS, ClientManager.PARAM_MANDATORY);
  }

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
  */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {
    String logStr;
    int    ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_PORT))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(port);
      }
      else
      {
        try
        {
          port = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          FWLog.error("Invalid number for port. Passed value = <" + Parameter + ">");
        }
        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_CONNECTIONS))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(maxConnections);
      }
      else
      {
        try
        {
          maxConnections = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          FWLog.error("Invalid number for maximum connections. Passed value = <" + Parameter + ">");
        }
        ResultCode = 0;
      }
    }

    // Currently this cannot handle any dynamic events
    if (ResultCode == 0)
    {
        logStr = "Command " + Command + " handled";
        FWLog.debug(logStr);
        return SocketConstants.OKMESSAGE;
    }
    else
    {
      return "Command Not Understood";
    }
  }
  
 /**
  * return the symbolic name
  *
  * @return The symbolic name for this plugin
  */
  public String getSymbolicName()
  {
      return SymbolicName;
  }

 /**
  * set the symbolic name
  *
  * @param Name The new symbolic name for this plugin
  */
  public void setSymbolicName(String Name)
  {
      SymbolicName=Name;
  }
}
