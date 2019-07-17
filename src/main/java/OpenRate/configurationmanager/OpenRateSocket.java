

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
      OpenRate.getOpenRateFrameworkLog().info("Listener on port <" + this.port + "> is running...");
      System.out.println("    Listener on port <" + this.port + "> is running...");

      started = true;

      // Thread group for managing the console threads shutdown
      consoleGroup = new ThreadGroup("consoleThreads");
      
      //while connection is still accepted
      while(socData.isLoop())
      {
        socket = getSocket(serverSocket);

        // Only open up a new socket if we are still running
       
        if(socData.getConnectionNumber() < maxConnections)
        {
        	if (socData.isLoop())
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
      
      OpenRate.getOpenRateFrameworkLog().info("Closing <Listener>");
      System.out.println("Stopped listener");
      
      // Mark that we are down
      started = false;
    }
    else
    {
      // only print a message if we are active
      if (socData.isLoop())
      {
        OpenRate.getOpenRateFrameworkLog().debug("OpenRateSocket.run() error: ServerSocket is null. Could not bind to port " + this.port);
      }
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
