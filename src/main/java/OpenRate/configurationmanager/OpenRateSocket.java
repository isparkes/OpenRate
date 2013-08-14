/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
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

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
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
  // The port we listen on
  private int port;

  // The maximum number of connections we will serve
  private int maxConnections;

  // List of Services that this Client supports
  private final static String SERVICE_PORT = "Port";
  private final static String DEFAULT_PORT = "8081";
  private final static String SERVICE_CONNECTIONS = "MaxConnection";
  private final static String DEFAULT_MAX_CON = "2";

  // Shows if we are running OK
  static boolean started = false;
  static boolean initialised = false;

  // module symbolic name: never changes in this module, so not set dynamically
  private String SymbolicName = "OpenRateListener";

  // This is the socket for the ECI
  private Socket socket;
        
  private ServerSocket serverSocket;
  
  // Simple socket management interface
  SocketConnectionData socData = new SocketConnectionData();
      
  // Thead group for managing lauched console threads
  ThreadGroup consoleGroup;
  
  // used to simplify logging and exception handling
  private String message;

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
    catch(NumberFormatException ex)
    {
      message = "OpenRateSocket constructor error";
      throw new InitializationException(message,ex,getSymbolicName());
    }
    catch (IOException ex)
    {
      message = "OpenRateSocket.getServerSocket(): Could not listen on port <"
        + this.port + ">. Message = <" + ex.getMessage() + ">. Aborting.";
      throw new InitializationException(message,getSymbolicName());
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
      System.out.println("Listener on port <" + this.port + "> is running...");

      started = true;

      // Thread group for managing the console threads shutdown
      consoleGroup = new ThreadGroup("consoleThreads");
      
      //while connection is still accepted
      while(socData.isLoop())
      {
        socket = getSocket(serverSocket);

        // Only open up a new socket if we are still running
        if (socData.isLoop())
        {
          if(socData.getConnectionNumber() < maxConnections)
          {
            //This block happens when connection is still allowed
            SocketHelper.addAConnectionCount(socData);

            SocketListener socLis = new SocketListener(socket,socData);

            //start thread
            Thread t = new Thread(consoleGroup, socLis, "Listener");
            t.start();
          }
        }
        else
        {
          //This block is executed if the number of allowed connection has
          //been reached
          blockConnection(socket);
        }
      }
      
      System.out.println("Stopped listener");
    }
    else
    {
      OpenRate.getOpenRateFrameworkLog().debug("OpenRateSocket.run() error: ServerSocket is null. Could not " +
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
  * Shuts down the listener
  */
  public void stop() {
    // Stop any new threads opening
    socData.setLoop(false);
    
    // Close the current listener
    try {
      serverSocket.close();
    }
    catch (IOException ex) {
      System.out.println("Message " + ex.getMessage());
    }
    
    // Interrupt any threads open
    consoleGroup.interrupt();    
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
    try
    {
      socket = serverSocket.accept();
    }
    catch (IOException e)
    {
      // if we are stopping, just ignore
      if (socData.isLoop())
      {
        ProcessingException ex = new ProcessingException("Socket Accept Failed",getSymbolicName());
        OpenRate.getFrameworkExceptionHandler().reportException(ex);
      }
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
      // On shut down we cannot be sure that the socket did not already close down
      if (socket != null)
      {
        out = new PrintStream(new BufferedOutputStream(socket.getOutputStream(), 1024), false);

        //displays the maximum connection message
        out.println(SocketConstants.CONNECTIONMAXMESSAGE);
        out.flush();
      }
    }
    catch (IOException e)
    {
      OpenRate.getOpenRateFrameworkLog().error("OpenRateSocket.blockConnection() error");
      OpenRate.getOpenRateFrameworkLog().error(e.getClass() + ": " + e.getMessage(), e);
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
        OpenRate.getOpenRateFrameworkLog().error("OpenRateSocket.blockConnection() finally clause error.",e);
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
    ClientManager.getClientManager().registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PORT, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_CONNECTIONS, ClientManager.PARAM_MANDATORY);
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
          OpenRate.getOpenRateFrameworkLog().error("Invalid number for port. Passed value = <" + Parameter + ">");
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
          OpenRate.getOpenRateFrameworkLog().error("Invalid number for maximum connections. Passed value = <" + Parameter + ">");
        }
        ResultCode = 0;
      }
    }

    // Currently this cannot handle any dynamic events
    if (ResultCode == 0)
    {
        logStr = "Command " + Command + " handled";
        OpenRate.getOpenRateFrameworkLog().debug(logStr);
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
