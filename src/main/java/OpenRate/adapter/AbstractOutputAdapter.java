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

package OpenRate.adapter;

import OpenRate.CommonConfig;
import OpenRate.audit.AuditUtils;
import OpenRate.buffer.IConsumer;
import OpenRate.buffer.IEvent;
import OpenRate.buffer.IMonitor;
import OpenRate.buffer.ISupplier;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;


/**
 * AbstractSTOutputAdapter - a single threaded output adapter implementation.
 */
public abstract class AbstractOutputAdapter
  implements IOutputAdapter,
             IEventInterface,
             IMonitor
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractOutputAdapter.java,v $, $Revision: 1.79 $, $Date: 2013-05-13 18:12:11 $";

  // This is the symbolic name that we use to identify individual instances
  private String SymbolicName;

  /**
   * The PipeLog is the logger which should be used for all pipeline level
   * messages. This is instantiated during pipe startup, because at this
   * point we don't know the name of the pipe and therefore the logger to use.
   */
  protected ILogger        pipeLog = null;

  /**
   * The PipeLog is the logger which should be used for all statistics related
   * messages.
   */
  protected ILogger        statsLog = LogUtil.getLogUtil().getLogger("Statistics");

  private int              sleepTime = 100;
  private ISupplier        inputValidBuffer = null;
  private IConsumer        outputValidBuffer = null;
  private ExceptionHandler handler;

  // number of records to persist at once
  private int              BatchSize;
  private int              BufferSize;

  // Whether we are to shut down or not
  private volatile boolean ShutdownFlag = false;

  // Used to store the name of this output, for deciding if records should be
  // written to this output or not
  private String           OutputName;

  /**
   * This is the pipeline that we are in, used for logging
   */
  protected String pipeName;

  // This logs records to the log if they are discarded
  private boolean LogDiscardedRecords = false;

  // List of Services that this Client supports
  private final static String SERVICE_BATCHSIZE  = CommonConfig.BATCH_SIZE;
  private final static String SERVICE_BUFFERSIZE = CommonConfig.BUFFER_SIZE;
  private final static String DEFAULT_BATCHSIZE  = CommonConfig.DEFAULT_BATCH_SIZE;
  private final static String DEFAULT_BUFFERSIZE = CommonConfig.DEFAULT_BUFFER_SIZE;
  private final static String SERVICE_MAX_SLEEP  = CommonConfig.MAX_SLEEP;
  private final static String DEFAULT_MAX_SLEEP  = CommonConfig.DEFAULT_MAX_SLEEP;
  private final static String SERVICE_LOG_DISC   = "LogDiscardedRecords";
  private final static String SERVICE_STATS      = CommonConfig.STATS;
  private final static String SERVICE_STATSRESET = CommonConfig.STATS_RESET;
  private final static String SERVICE_OUTPUTNAME = "OutputName";

  //performance counters
  private long processingTime = 0;
  private long recordsProcessed = 0;
  private long streamsProcessed = 0;
  private int  outBufferCapacity = 0;
  private int  bufferHits = 0;

  // If we are the terminating output adapter, default no
  private boolean TerminatingAdaptor = false;

 /**
  * Default constructor
  */
  public AbstractOutputAdapter()
  {
    super();

    // Add the version map
    AuditUtils.getAuditUtils().buildHierarchyVersionMap(this.getClass());
  }

 /**
  * Initialise the attributes relevant to this part of the output adapter
  * stack.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    String ConfigHelper;
    setSymbolicName(ModuleName);

    // store the pipe we are in
    this.pipeName = PipelineName;

    // Get the pipe PipeLog
    pipeLog = LogUtil.getLogUtil().getLogger(PipelineName);

    registerClientManager();
    ConfigHelper = initGetBatchSize();
    processControlEvent(SERVICE_BATCHSIZE, true, ConfigHelper);
    ConfigHelper = initGetBufferSize();
    processControlEvent(SERVICE_BUFFERSIZE, true, ConfigHelper);
    ConfigHelper = initGetMaxSleep();
    processControlEvent(SERVICE_MAX_SLEEP, true, ConfigHelper);
    ConfigHelper = initGetOutputName();
    processControlEvent(SERVICE_OUTPUTNAME, true, ConfigHelper);
    ConfigHelper = initLogDiscardedRecords();
    processControlEvent(SERVICE_LOG_DISC, true, ConfigHelper);
  }

  /**
   * Thread execution method. Inherited from Runnable. All this method does is
   * call write() and catch any processing exception. Any exceptions that
   * occur in the processing are intercepted and passed back via the exception
   * handler that we nominated during the pipeline creation
   */
  @Override
  public void run()
  {
    getBatchInboundValidBuffer().registerMonitor(this);

    // Write the records
    try
    {
      write();
    }
    catch (ProcessingException pe)
    {
      handler.reportException(pe);
    }    
  }

  /**
   * The write method iterates through the batch and drives the processing thus:
   * 1) The iterator checks the streams which the record should be written to
   *    and if this stream should be written to, fires either the prepValid or
   *    prepError method. (Headers and trailers always fire)
   * 2) The prepValid/prepError method triggers the procValid/procError method,
   *    which is where the concrete implementation class changes the record type
   *    from that used in the pipeline to the required type for the output
   *    adapter, and performs record decompression
   * 3) The prepValid/prepError method then writes the record (uncompressed by
   *    now) to the media
   * 4) If the record has been consumed, it is dropped, otherwise it passes into
   *    the output batch.
   * 5) If this is an output terminator, any record which was not consumed is written to
   *    the PipeLog file.
   * @throws ProcessingException 
   */
  public void write() throws ProcessingException
  {
    Collection<IRecord> in;
    Collection<IRecord> out;
    Iterator<IRecord> iter;
    boolean OutBatchHasValidRecords = false;
    long startTime;
    long endTime;
    long BatchTime;
    int  ThisBatchRecordCount;
    int  ThisBatchRecordsWritten;
    boolean inTransaction = false;

    while (true)
    {
      // Start the timing for the statistics
      startTime = System.currentTimeMillis();

      in = getBatchInboundValidBuffer().pull(BatchSize);
      ThisBatchRecordCount = in.size();
      ThisBatchRecordsWritten = 0;

      if (ThisBatchRecordCount > 0)
      {
        pipeLog.debug("Output <" + getSymbolicName() + "> Processing a batch of " + ThisBatchRecordCount + " valid records.");
        out = new ArrayList<>();

        // Check for the case that we have an aborted transaction
        if (inTransaction && SkipRestOfStream())
        {
          int SkipCount = 0;
          Iterator<IRecord> SkipIter = in.iterator();

          // fast forward to the end of the stream
          while (SkipIter.hasNext())
          {
            IRecord r = SkipIter.next();
            if (r instanceof TrailerRecord)
            {
              // Log how many we discarded
              pipeLog.warning("Output <" + getSymbolicName() + "> discarded <" + SkipCount + "> records because of transaction abort");

              //reset the iterator
              break;
            }
            else
            {
              // zap the record
              SkipIter.remove();
              SkipCount++;
            }
          }
        }

        iter = in.iterator();

        while (iter.hasNext())
        {
          // Get the formatted information from the record
          IRecord r = iter.next();

          if (r.isValid())
          {
            // this is a call to the "prepare" class, which in turn will call
            // the procValidRecord method, which is where the implementation
            // class gets its say.
            if (r.getOutput(OutputName))
            {
              ThisBatchRecordsWritten++;

              try
              {
                r = prepValidRecord(r);
              }
              catch (ProcessingException pe)
              {
                handler.reportException(pe);
              }

              if (!r.deleteOutput(OutputName,TerminatingAdaptor))
              {
                // pass the record into the output stream
                out.add(r);
                OutBatchHasValidRecords = true;
              }
            }
            else
            {
              // pass the record into the output stream
              out.add(r);
              OutBatchHasValidRecords = true;
            }
          }
          else
          {
            if (r.isErrored())
            {
              // this is a call to the "prepare" class, which in turn will call
              // the procErrorRecord method, which is where the implementation
              // class gets its say
              if (r.getOutput(OutputName))
              {
                ThisBatchRecordsWritten++;
                
                try
                {
                  r = prepErrorRecord(r);
                }
                catch (ProcessingException pe)
                {
                  handler.reportException(pe);
                }

                if (!r.deleteOutput(OutputName,TerminatingAdaptor))
                {
                  // drop the record
                  out.add(r);
                  OutBatchHasValidRecords = true;
                }
              }
              else
              {
                // pass the record into the output stream
                out.add(r);
                OutBatchHasValidRecords = true;
              }
            }
            else
            {
              if (r instanceof HeaderRecord)
              {
                ThisBatchRecordsWritten++;
                streamsProcessed++;
                procHeader(r);
                out.add(r);
                inTransaction = true;
              }

              if (r instanceof TrailerRecord)
              {
                ThisBatchRecordsWritten++;

                // Flush out the rest of the stream
                try
                {
                  flushStream();
                }
                catch (Exception e)
                {
                  handler.reportException(new ProcessingException(e));
                }
                
                // Process the trailer and pass it on
                procTrailer(r);
                out.add(r);
                
                // Mark that we have finished this stream
                inTransaction = false;                
              }
            }
          }
        }

        // block flush
        // We have to be a bit careful with flushing, as there is a difference
        // between the way that file streams and DB streams. The difference
        // comes from the fact that we allow 1 block to hold many file streams
        // but only 1 DB stream. If we flushed the stream, we can't flush the 
        // block for DB streams (the flush causes the DB connection to close).
        try
        {
          flushBlock();
        }
        catch (ProcessingException pe)
        {
          handler.reportException(pe);
        }
        
        // clean up the input buffer
        in.clear();

        // Push the records that survived into the next output
        if (OutBatchHasValidRecords)
        {
          if (TerminatingAdaptor)
          {
            pipeLog.error("Output <" + getSymbolicName() + "> discarded <" +
                  out.size() + "> records at the end of the output adapter chain.");

            // dump the information out
            if (LogDiscardedRecords)
            {
              iter = out.iterator();
              while (iter.hasNext())
              {
                 //Get the formatted information from the record
                 IRecord r = iter.next();

                Iterator<String> dumpIter = r.getDumpInfo().iterator();
                while(dumpIter.hasNext())
                {
                  pipeLog.info(dumpIter.next());
                }
              }
            }
          }
          else
          {
            // push the remaining records to the next adapter
            getBatchOutboundValidBuffer().push(out);

            outBufferCapacity = getBatchOutboundValidBuffer().getEventCount();

            while (outBufferCapacity > BufferSize)
            {
              bufferHits++;
              statsLog.debug("Output <" + getSymbolicName() + "> buffer high water mark! Buffer max = <" + BufferSize + "> current count = <" + outBufferCapacity + ">");
              try {
                Thread.sleep(sleepTime);
              } catch (InterruptedException ex) {
               //
              }
              outBufferCapacity = getBatchOutboundValidBuffer().getEventCount();
            }
          }
        }
        else
        {
          // even if there are no valid records, we have to push the header/trailer
          // to allow the transactions to be managed
          if (!TerminatingAdaptor)
          {
            getBatchOutboundValidBuffer().push(out);
          }
        }

        endTime = System.currentTimeMillis();
        BatchTime = (endTime - startTime);
        processingTime += BatchTime;

        recordsProcessed += ThisBatchRecordCount;
        statsLog.info(
              "Output <" + getSymbolicName() + "> persisted <" +
              ThisBatchRecordsWritten + "> events from a batch of <" +
              ThisBatchRecordCount + "> events in <" + BatchTime + "> ms" );
      }
      else
      {
        pipeLog.debug(
            "Output <" + getSymbolicName() +
            ">, Idle Cycle, thread <" + Thread.currentThread().getName() + ">");

        // We have finished the
        if (ShutdownFlag == true)
        {
        pipeLog.debug(
            "Output <" + getSymbolicName() +
            ">, thread <" + Thread.currentThread().getName() + "> shut down. Exiting.");
          break;
        }

        // If not marked for shutdown, wait for notification from the
        // supplier that new records are available for processing.
        try
        {
          synchronized (this)
          {
            wait();
          }
        }
        catch (InterruptedException e)
        {
          // ignore
        }
      }
    } // while loop
  }

 /**
  * This is used in the case that we want to skip to the end of the stream
  * discarding records as we go. This is primarily used in the abort
  * processing, and so here never triggers the skip. If you want to use the
  * skip, you need to over write this method.
  *
  * @return true if we skip, otherwise false
  */
  public boolean SkipRestOfStream()
  {
    return false;
  }

  /**
   * Do any non-record level processing required to finish this
   * batch cycle.
   *
   * @return The number of records that are in the output buffer
   */
  @Override
  public int getOutboundRecordCount()
  {
    if (TerminatingAdaptor)
    {
      return 0;
    }
    else
    {
      outBufferCapacity = getBatchOutboundValidBuffer().getEventCount();
      return outBufferCapacity;
    }
  }

  /**
   * Do any required processing prior to completing the stream. The flushStream() 
   * method is called for transaction stream. This differs from the flushBlock(), 
   * which is called at the end of each block and the cleanup() method, which 
   * is called only once upon application shutdown.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void flushStream() throws ProcessingException
  {
    // no op
  }

  /**
   * Do any required processing prior to completing the batch block. The 
   * flushBlock() method is called for block processed and is intended for
   * batch commit control.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void flushBlock() throws ProcessingException
  {
    // no op
  }

  /**
   * Reset the adapter in to ensure that it's ready to process records again
   * after it has been exited. This method must be called after calling
   * MarkForClosedown() to reset the state.
   */
  @Override
  public void reset()
  {
    //PipeLog.debug("reset called on Output Adapter <" + getSymbolicName() + ">");
    this.ShutdownFlag = false;
  }

  /**
   * MarkForClosedown tells the adapter thread to close at the first chance,
   * usually as soon as an idle cycle is detected
   */
  @Override
  public void markForClosedown()
  {
    this.ShutdownFlag = true;

    // notify any listeners that are waiting that we are flushing
    synchronized (this)
    {
      notifyAll();
    }
  }

 /**
  * Do anything necessary before shutting down the output adapter
  *
  * @throws OpenRate.exception.ProcessingException
  */
  @Override
  public void close() throws ProcessingException
  {
    pipeLog.debug("close");
  }

 /**
  * Do any cleanup before closing
  */
  @Override
  public void cleanup()
  {
    pipeLog.debug("cleanup");
  }

 /**
  * Set the inbound buffer for valid records
  *
  * @param ch The supplier buffer to set
  */
  @Override
  public void setBatchInboundValidBuffer(ISupplier ch)
  {
    this.inputValidBuffer = ch;
  }

 /**
  * Get the inbound buffer for valid records
  *
  * @return ch The current supplier buffer
  */
  @Override
  public ISupplier getBatchInboundValidBuffer()
  {
    return this.inputValidBuffer;
  }

 /**
  * Set the outbound buffer for valid records
  *
  * @param ch The consumer buffer to set
  */
  @Override
  public void setBatchOutboundValidBuffer(IConsumer ch)
  {
    this.outputValidBuffer = ch;
  }

 /**
  * Get the outbound buffer for valid records
  *
  * @return ch The current consumer buffer
  */
  @Override
  public IConsumer getBatchOutboundValidBuffer()
  {
    return this.outputValidBuffer;
  }

 /**
  * The parent exception handler to set
  *
  * @param h The parent exception handler
  */
  @Override
  public void setExceptionHandler(ExceptionHandler h)
  {
    this.handler = h;
  }

 /**
  * Get the batch size for commits
  *
  * @return The current batch size
  */
  public int getBatchSize()
  {
    return this.BatchSize;
  }

 /**
  * Return the current parent exception handler
  *
  * @return The current parent exception handler
  */
  public ExceptionHandler getExceptionHandler()
  {
    return this.handler;
  }

 /**
  * Prepare the current (valid) record for outputting. The prepValidRecord
  * calls the procValidRecord() method for the record, and then writes the
  * resulting records to the output file one at a time. This is the "record
  * expansion" part of the "record compression" strategy.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException  
  */
  public abstract IRecord prepValidRecord(IRecord r) throws ProcessingException;

 /**
  * Prepare the current (error) record for outputting. The prepValidRecord
  * calls the procValidRecord() method for the record, and then writes the
  * resulting records to the output file one at a time. This is the "record
  * expansion" part of the "record compression" strategy.
  *
  * @param r The current record we are working on
  * @return The prepared record
  * @throws ProcessingException  
  */
  public abstract IRecord prepErrorRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting. This is for information to the
  * implementing module only, and need not be hooked, as it is handled
  * internally by the child class
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException  
  */
  public abstract IRecord procHeader(IRecord r) throws ProcessingException;

 /**
  * This is called when a data record is encountered. You should do any normal
  * processing here. Note that the result is a collection for the case that we
  * have to re-expand after a record compression input adapter has done
  * compression on the input stream.
  *
  * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException  
  */
  public abstract Collection<IRecord> procValidRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  *
  * @param r The record we are working on
   * @return The collection of processed records
   * @throws ProcessingException  
  */
  public abstract Collection<IRecord> procErrorRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished. This returns void, because we do
  * not write stream headers, thus this is for information to the implementing
  * module only.
  *
  * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException  
  */
  public abstract IRecord procTrailer(IRecord r)  throws ProcessingException;

 /**
  * return the symbolic name
  *
  * @return The symbolic name for this class stack
  */
  @Override
  public String getSymbolicName()
  {
    return SymbolicName;
  }

 /**
  * set the symbolic name
  *
  * @param name The symbolic name to set for this class stack
  */
  @Override
  public void setSymbolicName(String name)
  {
    SymbolicName = name;
  }

// -----------------------------------------------------------------------------
// ----------------------- Start of IMonitor functions -------------------------
// -----------------------------------------------------------------------------

 /**
  * Simple implementation of Monitor interface based on Thread
  * wait/notify mechanism.
  *
  * @param e The event notifier
  */
  @Override
  public void notify(IEvent e)
  {
    synchronized (this)
    {
      notifyAll();
    }
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

  /**
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    // Set the client reference and the base services first
    ClientManager.registerClient(pipeName,getSymbolicName(), this);

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_BATCHSIZE,  ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_BUFFERSIZE, ClientManager.PARAM_MANDATORY);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_MAX_SLEEP,  ClientManager.PARAM_NONE);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_STATS,      ClientManager.PARAM_NONE);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_STATSRESET, ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_OUTPUTNAME, ClientManager.PARAM_MANDATORY);
  }

  /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
  *
  * @param Command The command that we are to work on
  * @param Init True if the pipeline is currently being constructed
  * @param Parameter The parameter value for the command
  * @return The result message of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;
    double CDRsPerSec;

    // Reset the Statistics
    if (Command.equalsIgnoreCase(SERVICE_STATSRESET))
    {
      ResultCode = 0;
      // Only reset if we are told to
      switch (Parameter) {
        case "true":
          processingTime = 0;
          recordsProcessed = 0;
          streamsProcessed = 0;
          bufferHits = 0;
          break;
        case "":
          return "false";
      }
    }

    // Return the Statistics
    if (Command.equalsIgnoreCase(SERVICE_STATS))
    {
      if (processingTime == 0)
      {
        CDRsPerSec = 0;
      }
      else
      {
        CDRsPerSec = (double)((recordsProcessed*1000)/processingTime);
      }

      return Long.toString(recordsProcessed) + ":" +
             Long.toString(processingTime) + ":" +
             Long.toString(streamsProcessed) + ":" +
             Double.toString(CDRsPerSec) + ":" +
             Long.toString(outBufferCapacity) + ":" +
             Long.toString(bufferHits);
    }

    if (Command.equalsIgnoreCase(SERVICE_BUFFERSIZE))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(BufferSize);
      }
      else
      {
        try
        {
          BufferSize = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          pipeLog.error(
                "Invalid number for batch size. Passed value = <" +
                Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_BATCHSIZE))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(BatchSize);
      }
      else
      {
        try
        {
          BatchSize = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          pipeLog.error(
                "Invalid number for batch size. Passed value = <" +
                Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_OUTPUTNAME))
    {
      if (Init)
      {
          OutputName = Parameter;
          ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return OutputName;
        }
        else
        {
          return CommonConfig.NON_DYNAMIC_PARAM;
        }
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_MAX_SLEEP))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(sleepTime);
      }
      else
      {
        try
        {
          sleepTime = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          pipeLog.error(
                "Invalid number for sleep time. Passed value = <" +
                Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_LOG_DISC))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        LogDiscardedRecords = true;
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("false"))
      {
        LogDiscardedRecords = false;
        ResultCode = 0;
      }
      else
      {
        // return the current status
        if (LogDiscardedRecords)
        {
          return "true";
        }
        else
        {
          return "false";
        }
      }
    }

    if (ResultCode == 0)
    {
      pipeLog.debug(LogUtil.LogECIPipeCommand(getSymbolicName(), pipeName, Command, Parameter));

      return "OK";
    }
    else
    {
      return "Command Not Understood \n";
    }
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of initialisation functions ----------------------
  // -----------------------------------------------------------------------------

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetBatchSize() throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(pipeName, SymbolicName,
                                                  SERVICE_BATCHSIZE,DEFAULT_BATCHSIZE);

    return tmpFile;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetBufferSize()
                           throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(pipeName, SymbolicName,
                                                  SERVICE_BUFFERSIZE,DEFAULT_BUFFERSIZE);

    return tmpFile;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetMaxSleep()
                          throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(pipeName, SymbolicName,
                                                  SERVICE_MAX_SLEEP,DEFAULT_MAX_SLEEP);

    return tmpFile;
  }

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetOutputName()
                           throws InitializationException
  {
    String tmpParam;
    tmpParam = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(pipeName, SymbolicName,
                                                  SERVICE_OUTPUTNAME,"");

    if (tmpParam.equals(""))
    {
      throw new InitializationException ("Output Adapter Name <" +
                                         getSymbolicName() +
                                         ".OutputName> not set for <" +
                                         getSymbolicName() + ">");
    }

    return tmpParam;
  }


  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initLogDiscardedRecords()
                           throws InitializationException
  {
    String tmpParam;
    tmpParam = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(pipeName, SymbolicName,
                                                  SERVICE_LOG_DISC,"false");

    return tmpParam;
  }
  
 /**
  * Set if we are a terminating output adapter or not
  *
  * @param Terminator The new value to set
  */
  @Override
  public void setTerminator(boolean Terminator)
  {
    TerminatingAdaptor = Terminator;
  }
}
