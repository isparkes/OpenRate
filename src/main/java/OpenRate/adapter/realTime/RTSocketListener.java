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
package OpenRate.adapter.realTime;

import OpenRate.audit.AuditUtils;
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
  protected ILogger PipeLog = null;

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
  private String ThreadId;

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

    // Add the version map
    AuditUtils.getAuditUtils().buildVersionMap(this.getClass());

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
      PipeLog.debug("Started to listed on <" + ThreadId + ">");

      //reads the input data
      while((input = br.readLine()) != null)
      {
        // tell what info we have got
        PipeLog.debug("Got data <" + input + "> on <" + ThreadId + ">");

        // Create the carrier record, and set the data and ID
        RTRecordToProcess = new FlatRecord();
        RTRecordToProcess.setData(input);
        RTRecordToProcess.setRecordID(ourConnectionID);

        // Put the record into the event processor
        OutRecord = (FlatRecord) ParentRTAdapter.processRTRecord(RTRecordToProcess);

        // Output the processed data
        if (OutRecord != null)
        {
          PipeLog.debug("sent <" + OutRecord.getData() + ">");
          out.print(OutRecord.getData());
          out.flush();
        }
      }
    }
    catch(IOException e)
    {
      String Message = "OpenRate RT Listener IO error: " + e.getClass() + ": " +
        e.getMessage();

      // if we get a "Connection reset", ignore it
      if (e.getMessage().contains("Connection reset"))
      {
        // suppress it
      }
      else
      {
        // report the exception
        handler.reportException(new ProcessingException(Message));
      }
    }
    catch(ProcessingException pe)
    {
      // just pass it up
      handler.reportException(pe);
    }
    catch(Exception e)
    {
      String Message = "OpenRate RT Listener exception: " + e.getClass() + ": " +
        e.getMessage();

      // report the exception
      handler.reportException(new ProcessingException(Message,e));
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
        PipeLog.error("SocketListener.run() error closing objects: " + e.getClass()
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
    this.PipeLog = newPipeLog;
  }

 /**
  * Set the thread identifier for this thread.
  *
  * @param threadId The new thread Id.
  */
  void setThreadId(String threadId)
  {
    this.ThreadId = threadId;
  }

 /**
  * Get the thread identifier for this thread.
  *
  * @return The thread Id.
  */
  String getThreadId()
  {
    return ThreadId;
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
