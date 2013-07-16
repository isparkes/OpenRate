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
