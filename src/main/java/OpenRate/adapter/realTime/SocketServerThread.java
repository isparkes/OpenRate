
package OpenRate.adapter.realTime;

import OpenRate.configurationmanager.SocketConnectionData;
import OpenRate.configurationmanager.SocketConstants;
import OpenRate.exception.ExceptionHandler;
import OpenRate.logging.ILogger;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This module implements a socket server that listens on a TCP port for
 * incoming connections and instantiates a listener thread for each of the
 * connections requested. A maximum number of connections is defined. Any
 * connection that exceeds the maximum number will be refused.
 *
 * @author ian
 */
public class SocketServerThread implements Runnable
{
  // listener TCP port
  private int port = 0;

  // maximum number of concurrent connections
  private int maxConnections = 10;

  // This is the reference to the RT adapter that will handle requests
  private IRTAdapter ParentRTAdapter;

  // this is the name of the pipeline log that we will write to
  private String pipeName;

  /**
   * The PipeLog is the logger which should be used for all pipeline level
   * messages. This is instantiated during pipe startup, because at this
   * point we don't know the name of the pipe and therefore the logger to use.
   */
  protected ILogger PipeLog = null;

  // This is used to manage the number of connections
  private SocketConnectionData socData;

  // The name of this thread
  private String threadName;

 /**
  * Constructor
  */
  public void SocketServerThread()
  {
  }

 /**
  * Set the port number that we are listening on.
  *
  * @param newPort The port number to listen on
  */
  public void setPort(int newPort)
  {
    this.port = newPort;
  }

 /**
  * Set the maximum number of simultaneous connections we want to handle
  *
  * @param newMaxConnections The maximum number of connections to allow
  */
  public void setMaxConnections(int newMaxConnections)
  {
    this.maxConnections = newMaxConnections;
  }

 /**
  * This thread serves as a socket spawner for the real time adapter.
  */
  @Override
  public void run()
  {
    // This is the server socket that we are dealing with
    ServerSocket serverSocket = getServerSocket();

    if(serverSocket!=null)
    {
      socData = new SocketConnectionData();
      Socket socket;
      System.out.println("Real Time processing listener on port <" + this.port + "> is running...");

      //while connection is still accepted
      while(socData.isLoop())
      {
        socket = getSocket(serverSocket);
        if(socData.getConnectionNumber() < maxConnections)
        {
          //This block happens when connection is still allowed
          socData.incrementCount();

          RTSocketListener socLis = new RTSocketListener(socket,socData);

          // set the input to the pipeline
          socLis.setParentAdapter(ParentRTAdapter);
          socLis.setPipelineLog(this.PipeLog);
          socLis.setThreadId("RTListener-" + socData.getConnectionNumber());

          //start thread
          Thread t = new Thread(socLis, pipeName + "-" + threadName + "-" + Integer.toString(socData.getConnectionNumber()));
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
      PipeLog.debug("OpenRateSocket.run() error: ServerSocket is null. Could not " +
        "bind to port <" + this.port + ">");
    }
  }

 /**
  * Shut down the listener thread
  */
  void markForClosedown()
  {
    // Break the loop for this socket
    socData.setLoop(false);
  }

 /**
  * Set the log location for this thread
  *
  * @param newPipeLog
  */
  void setPipelineLog(ILogger newPipeLog)
  {
    this.PipeLog = newPipeLog;
  }

 /**
  * Set the thread ID for file naming
  *
  * @param string The thread ID
  */
  void setThreadId(String string)
  {
    this.threadName = string;
  }

  /**
   * This method returns a ServerSocket object binding to the configured
   * port number.
   *
   * @return ServerSocket
   */
  private ServerSocket getServerSocket()
  {
    ServerSocket serverSocket = null;
    try
    {
      serverSocket = new ServerSocket(this.port);
    }
    catch (IOException e)
    {
      PipeLog.error("RTListenerSocket(): Could not listen on port <" + this.port + ">");
    }

    return serverSocket;
  }

  /**
   * This method returns a Socket object created when the ServerSocket
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
      PipeLog.error("RTListenerSocket: Accept failed.");
    }
    return socket;
  }


 /**
  * Set the name of the pipe we are listening for
  *
  * @param newPipelineName
  */
  public void setPipelineName(String newPipelineName)
  {
    this.pipeName = newPipelineName;
  }

  /**
   * This method is executed when the maximum number of connection has
   * been reached and new connections are made. This shows a message
   * telling the one making the connection that the maximum number of
   * connection has been reached and to try again later.
   *
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
      PipeLog.error("OpenRateSocket.blockConnection() error");
      PipeLog.error(e.getClass() + ": " + e.getMessage(), e);
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
        PipeLog.error("RTListenerSocket finally clause error.",e);
      }
    }
  }

 /**
  * Set the reference of the parent adapter, so that we can send received records
  * there for serialisation and processing.
  *
  * @param parentAdapter
  */
  void setParentAdapter(IRTAdapter parentAdapter)
  {
    this.ParentRTAdapter = parentAdapter;
  }
}
