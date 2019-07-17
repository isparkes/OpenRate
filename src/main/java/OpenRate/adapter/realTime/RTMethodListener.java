
package OpenRate.adapter.realTime;

import OpenRate.buffer.IConsumer;
import OpenRate.configurationmanager.SocketConnectionData;
import OpenRate.logging.AstractLogger;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

/**
 * This class implements real time listener used in real time processing, based
 * on calling a method to perform the processing.
 */
public class RTMethodListener
{
  private Socket socket = null;
  private SocketConnectionData socData = null;
  private IConsumer PipeInputBuffer;
  private IRTAdapter ParentRTAdapter;

  /**
   * The PipeLog is the logger which should be used for all pipeline level
   * messages. This is instantiated during pipe startup, because at this
   * point we don't know the name of the pipe and therefore the logger to use.
   */
  protected AstractLogger pipeLog = null;

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

  /**
   * Constructor with Socket and SocketConnectionData objects passed as<br/>
   * parameters.
   *
   * @param socket
   * @param socData
   */
  public RTMethodListener(Socket socket, SocketConnectionData socData)
  {
    super();

    this.socket = socket;
    this.socData = socData;
  }

 /**
  * Set the buffer that we are to feed events into
  *
  * @param pipeConsumer
  */
  public void setRTConsumer(IConsumer pipeConsumer)
  {
    this.PipeInputBuffer = pipeConsumer;
  }

  /**
   * Method that executes the processes after connection has been accepted<br/>
   * by the listener.
   *
   * @param input The input string to process
   */
  public void processObject(String input)
  {
    FlatRecord RTRecordToProcess;
    IRecord ProcessedRecord;
    Collection<IRecord> Outbatch;

    HeaderRecord tmpHeader;
    TrailerRecord tmpTrailer;

    try
    {
        // Create the carrier record, and set the data and ID
        RTRecordToProcess = new FlatRecord();
        RTRecordToProcess.setData(input);
        RTRecordToProcess.setRecordID(ourConnectionID);

        ProcessedRecord = ParentRTAdapter.performInputMapping(RTRecordToProcess);

        // Re-set the record ID
        ProcessedRecord.setRecordID(ourConnectionID);

        // Send the record for processing
        Outbatch = new ArrayList<IRecord>();

        // we need to wrap the RT batch as a false transaction
        String TransId = Long.toString(Calendar.getInstance().getTimeInMillis());
        tmpHeader = new HeaderRecord();
        tmpHeader.setStreamName(TransId);
        tmpHeader.setTransactionNumber(0);
        Outbatch.add(tmpHeader);
        Outbatch.add(ProcessedRecord);
        tmpTrailer = new TrailerRecord();
        tmpTrailer.setStreamName(TransId);
        Outbatch.add(tmpTrailer);

        // Done! Push it!
        //PipeInputBuffer.pushRealtime(Outbatch);

    }
    catch(Exception e)
    {
      pipeLog.error("OpenRate RT Listener error: " + e.getClass() + ": " +
      e.getMessage());
    }
    finally
    {
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
  void setPipelineLog(AstractLogger newPipeLog)
  {
    this.pipeLog = newPipeLog;
  }

  void setThreadId(int connectionNumber)
  {
    this.ThreadId = Integer.toString(connectionNumber);
  }

 /**
  * Get an instance of a real time processing object. This object is used to
  * call the pipeline processing.
  *
  * @return
  */
  public RTProcessingProxy getProcessingProxy()
  {
    RTProcessingProxy tmpProxy = new RTProcessingProxy();

    tmpProxy.setRTConsumer(PipeInputBuffer);
    tmpProxy.setParentAdapter(ParentRTAdapter);
    tmpProxy.setPipelineLog(this.pipeLog);

    return tmpProxy;
  }

 /**
  * Get an instance of a real time processing object. This object is used to
  * call the pipeline processing.
  *
  */
  public void closeProcessingProxy()
  {
  }
}
