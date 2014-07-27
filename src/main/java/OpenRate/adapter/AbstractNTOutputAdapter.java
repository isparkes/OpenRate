/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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

package OpenRate.adapter;

import OpenRate.CommonConfig;
import OpenRate.IPipeline;
import OpenRate.OpenRate;
import OpenRate.buffer.IConsumer;
import OpenRate.buffer.IEvent;
import OpenRate.buffer.IMonitor;
import OpenRate.buffer.ISupplier;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.utils.PropertyUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * AbstractSTOutputAdapter - a single threaded output adapter implementation. Without
 * transaction handling
 */
public abstract class AbstractNTOutputAdapter
  implements IOutputAdapter,
             IMonitor
{
  private String           symbolicName;

  private int              sleepTime = 50;
  private ISupplier        inputValidBuffer = null;
  private IConsumer        outputValidBuffer = null;

  // number of records to persist at once
  private int              batchSize;
  private int              bufferSize;

  // Whether we are to shut down or not
  private volatile boolean shutdownFlag = false;

  // Used to store the name of this output, for deciding if records should be
  // written to this output or not
  private String           outputName;

  // List of Services that this Client supports
  private final static String SERVICE_BATCHSIZE  = CommonConfig.BATCH_SIZE;
  private final static String SERVICE_BUFFERSIZE = CommonConfig.BUFFER_SIZE;
  private final static String SERVICE_MAX_SLEEP  = CommonConfig.MAX_SLEEP;
  private final static String SERVICE_STATS      = CommonConfig.STATS;
  private final static String SERVICE_STATSRESET = CommonConfig.STATS_RESET;
  private final static String SERVICE_OUTPUTNAME = "OutputName";
  private final static String DEFAULT_BATCHSIZE  = CommonConfig.DEFAULT_BATCH_SIZE;
  private final static String DEFAULT_BUFFERSIZE = CommonConfig.DEFAULT_BUFFER_SIZE;
  private final static String DEFAULT_MAX_SLEEP  = CommonConfig.DEFAULT_MAX_SLEEP;

  //performance counters
  private long processingTime = 0;
  private long recordsProcessed = 0;
  private long streamsProcessed = 0;
  private int  outBufferCapacity = 0;
  private int  bufferHits = 0;

  // If we are the terminating output adapter, default no
  private boolean TerminatingAdaptor = false;

  // This is the pipeline that we are in, used for logging and property retrieval
  private IPipeline pipeline;

 /**
  * Default constructor
  */
  public AbstractNTOutputAdapter()
  {
    super();
  }

 /**
  * Initialise the attributes relevant to this part of the output adapter
  * stack.
  *
  * @param ModuleName The module name of this module
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    String ConfigHelper;
    setSymbolicName(ModuleName);

    // store the pipe we are in
    setPipeline(OpenRate.getPipelineFromMap(PipelineName));

    RegisterClientManager();

    ConfigHelper = initGetBatchSize();
    processControlEvent(SERVICE_BATCHSIZE, true, ConfigHelper);
    ConfigHelper = initGetBufferSize();
    processControlEvent(SERVICE_BUFFERSIZE, true, ConfigHelper);
    ConfigHelper = initGetMaxSleep();
    processControlEvent(SERVICE_MAX_SLEEP, true, ConfigHelper);
    ConfigHelper = initGetOutputName();
    processControlEvent(SERVICE_OUTPUTNAME, true, ConfigHelper);
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
    long startTime;
    long endTime;

    // Start the timing for the statistics
    startTime = System.currentTimeMillis();

    getBatchInboundValidBuffer().registerMonitor(this);

    try
    {
      // 'localDone' variable is used to ensure that write() is
      // called once after the this.ShutdownFlag variable is set to true.
      // This will force all the records in the output & error
      // buffers to be flushed prior to exit. Otherwise you have
      // a race condition where records could be added to the
      // buffers after write() is called, but before the loop
      // resets.
      boolean localDone;

      startTime = System.currentTimeMillis();

      do
      {
        Thread.sleep(this.sleepTime);
        localDone = this.shutdownFlag;
        write();
      }
      while ((!localDone) && (getExceptionHandler().hasError() == false));

      // Do any flush processing that is required
      flush();
    }
    catch (ProcessingException pe)
    {
            getPipeLog().error("Processing exception caught in Output Adapter <" +
                getSymbolicName() + ">", pe);
      getExceptionHandler().reportException(pe);
    }
    catch (Throwable t)
    {
            getPipeLog().fatal("Unexpected exception caught in Output Adapter <" +
                getSymbolicName() + ">", t);
      getExceptionHandler().reportException(new ProcessingException(t,getSymbolicName()));
    }

    endTime = System.currentTimeMillis();
    processingTime += (endTime - startTime);

    this.shutdownFlag = false;
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
   *    the log file.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void write()
    throws ProcessingException
  {
    int ThisBatchCounter = 0;
    long size;
    Collection<IRecord> in;
    Collection<IRecord> out;
    Iterator<IRecord> iter;
    int recordCount = 0;
    long startTime;
    long endTime;
    boolean OutBatchHasValidRecords = false;

    // Start the timing for the statistics
    startTime = System.currentTimeMillis();

    try
    {
    do
    {
        in = getBatchInboundValidBuffer().pull(batchSize);
        size = in.size();
        recordsProcessed += size;

      if (size > 0)
      {
                    getPipeLog().debug("Processing a batch of " + size + " valid records.");

          out = new ArrayList<>();

          iter = in.iterator();

          while (iter.hasNext())
          {
            ThisBatchCounter++;

            // Get the formatted information from the record
            IRecord r = (IRecord)iter.next();

            if (r.isValid())
            {
              // this is a call to the prep class, which in turn will call
              // the procValidRecord method, which is where the implementation
              // class gets its say.
              if (r.getOutput(outputName))
              {
                r = prepValidRecord(r);

                if (!r.deleteOutput(outputName,TerminatingAdaptor))
                {
                  // pass the record into the output stream
                  out.add(r);
                  OutBatchHasValidRecords = true;
                }
              }
            }
            else
            {
              if (r.isErrored())
              {
                // this is a call to the prep class, which in turn will call
                // the procErrorRecord method, which is where the implementation
                // class gets its say
                if (r.getOutput(outputName))
                {
                  r = prepErrorRecord(r);

                  if (!r.deleteOutput(outputName,TerminatingAdaptor))
                  {
                    // drop the record
                    out.add(r);
                    OutBatchHasValidRecords = true;
                  }
                }
              }
              else
              {
                if (r instanceof HeaderRecord)
                {
                  streamsProcessed++;
                  procHeader(r);
                  out.add(r);
                }

                if (r instanceof TrailerRecord)
                {
                  procTrailer(r);
                  out.add(r);
                }
              }
            }
          }

          //validWriter.flush();
                    getPipeLog().debug("persisted " + ThisBatchCounter + " valid records.");

          // Push the records that survived into the output
          if (OutBatchHasValidRecords)
          {
            if (TerminatingAdaptor)
            {
                            getPipeLog().error("Output adapter <" + getSymbolicName() + "> discarded <" +
                    out.size() + "> records at the end of the output adapter chain.");
            }
            else
            {
              // push the remaining records to the next adapter
              getBatchOutboundValidBuffer().push(out);

              while (outBufferCapacity > bufferSize)
              {
                bufferHits++;
                OpenRate.getOpenRateStatsLog().debug("Output <" + getSymbolicName() + "> buffer high water mark! Buffer max = <" + bufferSize + "> current count = <" + outBufferCapacity + ">");
                try {
                  Thread.sleep(sleepTime);
                } catch (InterruptedException ex) {
                 //
                }
                outBufferCapacity = getBatchOutboundValidBuffer().getEventCount();
              }
            }
          }

          // Update the statistics
          endTime = System.currentTimeMillis();
          processingTime += (endTime - startTime);

          recordsProcessed += recordCount;
      }
      else
      {
                    getPipeLog().debug("No valid records found.");
      }
    }
    while (size > 0);
  }
  catch (IOException ioe)
  {
            getPipeLog().error("IOException writing records.", ioe);
      throw new ProcessingException(ioe,getSymbolicName());
  }
  }

  /**
   * Do any non-record level processing required to finish this
   * batch cycle.
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
   * Reset the adapter in to ensure that it's ready to process records again
   * after it has been exited. This method must be called after calling
   * MarkForClosedown() to reset the state.
   */
  @Override
  public void reset()
  {
    //log.debug("reset called on Output Adapter <" + getSymbolicName() + ">");
    this.shutdownFlag = false;
  }

  /**
   * Do any required processing prior to completing the batch
   * cycle. The flush() method is called by the strategy for
   * each execution cycle. This differs from the cleanup
   * method, which is called only once upon application
   * shutdown.
   * flush() is called by the run() method during each cycle.
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void flush()
    throws ProcessingException
  {
    // no op
  }

  /**
   * MarkForClosedown tells the adapter thread to close at the first chance,
   * usually as soon as an idle cycle is detected
   */
  @Override
  public void markForClosedown()
  {
    this.shutdownFlag = true;

    // notify any listeners that are waiting that we are flushing
    synchronized (this)
    {
      notifyAll();
    }
  }

 /**
  * Do anything necessary before shutting down the output adapter
  */
  @Override
  public void close() throws ProcessingException
  {
        getPipeLog().debug("close");
  }

 /**
  * Do any
  */
  @Override
  public void cleanup()
  {
        getPipeLog().debug("cleanup");
  }

 /**
  * Set the inbound buffer for valid records
  */
  @Override
  public void setBatchInboundValidBuffer(ISupplier ch)
  {
    this.inputValidBuffer = ch;
  }

 /**
  * Get the inbound buffer for valid records
  */
  @Override
  public ISupplier getBatchInboundValidBuffer()
  {
    return this.inputValidBuffer;
  }

 /**
  * Set the outbound buffer for valid records
  */
  @Override
  public void setBatchOutboundValidBuffer(IConsumer ch)
  {
    this.outputValidBuffer = ch;
  }

 /**
  * Get the outbound buffer for valid records
  */
  @Override
  public IConsumer getBatchOutboundValidBuffer()
  {
    return this.outputValidBuffer;
  }

 /**
  * Get the batch size for commits
  *
  * @return The current batch size
  */
  public int getBatchSize()
  {
    return this.batchSize;
  }

 /**
  * Prepare the valid record for outputting
  *
  * @param r The record to prepare
  * @return The prepared record
  * @throws java.io.IOException
  */
  public abstract IRecord prepValidRecord(IRecord r) throws IOException;

 /**
  * Prepare the error record for outputting
  *
  * @param r The record to prepare
  * @return The prepared record
  * @throws java.io.IOException
  */
  public abstract IRecord prepErrorRecord(IRecord r) throws IOException;

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting. This returns void, because we do
  * not manipulate stream headers, thus this is for information to the
  * implementing module only, and need not be hooked, as it is handled
  * internally by the child class
  *
  * @param r The header record to process
  * @return The processed record
  */
  public abstract IRecord procHeader(IRecord r);

 /**
  * This is called when a data record is encountered. You should do any normal
  * processing here. Note that the result is a collection for the case that we
  * have to re-expand after a record compression input adapter has done
  * compression on the input stream.
  *
  * @param r The valid record to process
  * @return The collection of processed records
  */
  public abstract Collection<IRecord> procValidRecord(IRecord r);

 /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  *
  * @param r The error record to process
  * @return The collection of processed records
  */
  public abstract Collection<IRecord> procErrorRecord(IRecord r);

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished. This returns void, because we do
  * not write stream headers, thus this is for information to the implementing
  * module only.
  *
  * @param r The trailer record to process
  * @return The processed record
  */
  public abstract IRecord procTrailer(IRecord r);

 /**
  * return the symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }

 /**
  * set the symbolic name
  */
  @Override
  public void setSymbolicName(String name)
  {
    symbolicName = name;
  }

// -----------------------------------------------------------------------------
// ----------------------- Start of IMonitor functions -------------------------
// -----------------------------------------------------------------------------

 /**
  * Simple implementation of Monitor interface based on Thread
  * wait/notify mechanism.
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
  * RegisterClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
   *
   * @throws InitializationException
  */
  public void RegisterClientManager() throws InitializationException
  {
    // Set the client reference and the base services first
    ClientManager.getClientManager().registerClient(getPipeName(), getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BATCHSIZE,  ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BUFFERSIZE, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_MAX_SLEEP,  ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATS,      ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATSRESET, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_OUTPUTNAME, ClientManager.PARAM_MANDATORY_DYNAMIC);
  }

 /**
  * ProcessControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result string of the operation
  */
  public String processControlEvent(String Command, boolean Init, String Parameter)
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
             Long.toString(bufferHits) + ":" +
             Long.toString(getOutboundRecordCount());
    }

    if (Command.equalsIgnoreCase(SERVICE_BATCHSIZE))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(batchSize);
      }
      else
      {
        try
        {
          batchSize = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
                    getPipeLog().error(
                "Invalid number for batch size. Passed value = <" +
                Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_BUFFERSIZE))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(bufferSize);
      }
      else
      {
        try
        {
          bufferSize = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
                    getPipeLog().error(
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
          outputName = Parameter;
          ResultCode = 0;
      }
      else
      {
        if (Parameter.equals(""))
        {
          return outputName;
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
                    getPipeLog().error(
                "Invalid number for sleep time. Passed value = <" +
                Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (ResultCode == 0)
    {
            getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return "Command Not Understood\n";
    }
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of initialisation functions ----------------------
  // -----------------------------------------------------------------------------

  /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetBatchSize()
                           throws InitializationException
  {
    String tmpFile;
    tmpFile = PropertyUtils.getPropertyUtils().getGroupPropertyValueDef(getSymbolicName(),
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
    tmpFile = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValueDef(getPipeName(), symbolicName,
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
    tmpFile = PropertyUtils.getPropertyUtils().getGroupPropertyValueDef(getSymbolicName(),
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
    tmpParam = PropertyUtils.getPropertyUtils().getGroupPropertyValueDef(getSymbolicName(),
                                                  SERVICE_OUTPUTNAME,"");

    if (tmpParam.equals(""))
    {
      throw new InitializationException ("Output Adapter Name <" +
                                         getSymbolicName() +
                                         ".OutputName> not set for <" +
                                         getSymbolicName() + ">",
                                         getSymbolicName());
    }

    return tmpParam;
  }

 /**
  * Set if we are a terminating output adapter or not
  */
  @Override
  public void setTerminator(boolean Terminator)
  {
    TerminatingAdaptor = Terminator;
  }

    /**
     * @return the pipeName
     */
    public String getPipeName() {
      return pipeline.getSymbolicName();
    }

    /**
     * @return the pipeline
     */
  @Override
    public IPipeline getPipeline() {
      return pipeline;
    }

 /**
  * Set the pipeline reference so the input adapter can control the scheduler
  *
  * @param pipeline the Pipeline to set
  */
  @Override
  public void setPipeline(IPipeline pipeline)
  {
    this.pipeline = pipeline;
  }

   /**
    * Return the pipeline logger.
    * 
    * @return The logger
    */
    protected ILogger getPipeLog() {
      return pipeline.getPipeLog();
    }

   /**
    * Return the exception handler.
    * 
    * @return The exception handler
    */
    protected ExceptionHandler getExceptionHandler() {
      return pipeline.getPipelineExceptionHandler();
    }
}
