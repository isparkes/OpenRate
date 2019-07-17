
package OpenRate.adapter.realTime;

import OpenRate.configurationmanager.SocketConnectionData;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.record.FlatRecord;
import java.io.*;
import java.net.Socket;

/**
 * This class implements real time socket listener used in real time processing.
 * The client process connects via TCP socket and is given a dedicated listener
 * instance. After this, the client process can use the socket session to
 * perform real time rating.
 *
 * When finished, the client should disconnect. In normal processing the server
 * will not terminate the connection. If the connection is dropped, this is a
 * sign of a fatal processing exception in the pipe.
 */
public class RTSocketListener implements Runnable, IRTListener
{
  private Socket socket = null;
  private SocketConnectionData socData = null;
  private IRTAdapter ParentRTAdapter;

  /**
   * The PipeLog is the logger which should be used for all pipeline level
   * messages. This is instantiated during pipe startup, because at this
   * point we don't know the name of the pipe and therefore the logger to use.
   */
  protected ILogger pipeLog = null;

  /**
   * The PipeLog is the logger which should be used for all statistics related
   * messages.
   */
  protected ILogger StatsLog = LogUtil.getLogUtil().getLogger("Statistics");

  // This is the output stream to reply to the socket
  private PrintStream out = null;

  // This is our connection identifier
  private int ourConnectionID;

  // The identifier of this thread
  private String threadId;

  // used for reporting exceptions to the pipe handler
  private ExceptionHandler handler;

  /**
   * Constructor with Socket and SocketConnectionData objects passed as<br/>
   * parameters.
   *
   * @param socket
   * @param socData
   */
  public RTSocketListener(Socket socket, SocketConnectionData socData)
  {
    super();

    // set the internal references
    this.socket = socket;
    this.socData = socData;
  }

  /**
   * Method that executes the processes after connection has been accepted<br/>
   * by the listener.
   */
  @Override
  public void run()
  {
    BufferedReader br = null;
    FlatRecord RTRecordToProcess;
    String input;
    FlatRecord OutRecord;
    //int recordCounter = 0;

    // get our connection identifier
    ourConnectionID = socData.getConnectionNumber();

    // perform the thread loop
    try
    {
      //instantiates the PrintStream object
      out = new PrintStream(new BufferedOutputStream(socket.getOutputStream(), 1024), false);

      //isntantiates the BufferedReader object
      br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

      // Go into the accept loop
      pipeLog.debug("Started to listed on <" + threadId + ">");

      //reads the input data
      while((input = br.readLine()) != null)
      {
        // tell what info we have got
        pipeLog.debug("Got data <" + input + "> on <" + threadId + ">");

        // Create the carrier record, and set the data and ID
        RTRecordToProcess = new FlatRecord();
        RTRecordToProcess.setData(input);
        RTRecordToProcess.setRecordID(ourConnectionID);

        // Put the record into the event processor
        OutRecord = (FlatRecord) ParentRTAdapter.processRTRecord(RTRecordToProcess);

        // Output the processed data
        if (OutRecord != null)
        {
          pipeLog.debug("sent <" + OutRecord.getData() + ">");
          out.print(OutRecord.getData());
          out.flush();
        }
      }
    }
    catch(IOException e)
    {
        String message = "OpenRate RT Listener IO error: " + e.getClass() + ": " +
        e.getMessage();

      // if we get a "Connection reset", ignore it
      if (e.getMessage().contains("Connection reset"))
      {
        // suppress it
      }
      else
      {
        // report the exception
        handler.reportException(new ProcessingException(message,threadId));
      }
    }
    catch(ProcessingException pe)
    {
      // just pass it up
      handler.reportException(pe);
    }
    catch(Exception ex)
    {
      String message = "OpenRate RT Listener exception: " + ex.getClass() + ": " +
      ex.getMessage();

      // report the exception
      handler.reportException(new ProcessingException(message,ex,threadId));
    }
    finally
    {
      //closes PrintStream, BufferedReader and Socket objects.
      try
      {
        if(out!=null)
        {
          out.close();
        }

        if(br!=null)
        {
          br.close();
        }

        if(socket!=null)
        {
          socket.close();
        }
      }
      catch(IOException e)
      {
        pipeLog.error("SocketListener.run() error closing objects: " + e.getClass()
        + ": " + e.getMessage(),e);
      }
      //removes one connection count from the SockectConnectionData since
      //this connection has been terminated or stopped.
      socData.decrementCount();
    }
  }

 /**
  * This tells the socket where to direct processing requests for incoming
  * connections that should be processed
  *
  * @param newParentRTAdapter
  */
  void setParentAdapter(IRTAdapter newParentRTAdapter)
  {
    this.ParentRTAdapter = newParentRTAdapter;
  }

 /**
  * Set the log that we are to write to
  *
  * @param newPipeLog
  */
  void setPipelineLog(ILogger newPipeLog)
  {
    this.pipeLog = newPipeLog;
  }

 /**
  * Set the thread identifier for this thread.
  *
  * @param threadId The new thread Id.
  */
  void setThreadId(String threadId)
  {
    this.threadId = threadId;
  }

 /**
  * Get the thread identifier for this thread.
  *
  * @return The thread Id.
  */
  String getThreadId()
  {
    return threadId;
  }

 /**
  * Set the parent exception handler
  *
  * @param handler The handler to set
  */
  void setHandler(ExceptionHandler handler)
  {
    this.handler = handler;
  }
}
