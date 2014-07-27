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

package OpenRate.process;

import OpenRate.CommonConfig;
import OpenRate.OpenRate;
import OpenRate.IPipeline;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The AbstractPlugIn provides a partially implemented PlugIn allowing simpler
 * concrete PlugIn modules. It deals with thread safe exit logic, and idle cycle
 * detection as well as providing the required hooks for buffer allocation.
 * All concrete processing is deferred to a doWork() abstract method, which
 * must be overridden in the concrete implementation.
 */
public abstract class AbstractPlugIn
  implements IPlugIn,
             IMonitor,
             IEventInterface
{
  // module symbolic name: set during initialisation
  private String symbolicName = "Unknown";

  private ISupplier supplier;
  private IConsumer consumer;
  private IConsumer errors;

  // for reporting fatal errors in a thread.
  private ExceptionHandler handler;
  
  // the configured batch size
  private int     batchSize;
  
  private int     bufferSize;
  private int     numThreads = 1;

  // to make getting ad hoc configurations easier
  private HashMap<String,String> configurationParameters = new HashMap<>(10);

 /**
  * the shutdown flag needs to be volatile to ensure that each thread accesses
  * the correct value. The flag is used to trigger a thread exit.
  */
  private volatile boolean shutdownFlag = false;

   // This is the pipeline that we are in, used for logging  
  private IPipeline pipeline;

  // List of Services that this Client supports
  private final static String SERVICE_BATCHSIZE  = CommonConfig.BATCH_SIZE;
  private final static String SERVICE_BUFFERSIZE = CommonConfig.BUFFER_SIZE;
  private final static String SERVICE_NUMTHREAD  = CommonConfig.NUM_PROCESSING_THREADS;
  private final static String SERVICE_STATS      = CommonConfig.STATS;
  private final static String SERVICE_STATSRESET = CommonConfig.STATS_RESET;
  private final static String SERVICE_ACTIVE     = CommonConfig.ACTIVE;
  private final static String DEFAULT_BATCHSIZE  = CommonConfig.DEFAULT_BATCH_SIZE;
  private final static String DEFAULT_BUFFERSIZE = CommonConfig.DEFAULT_BUFFER_SIZE;
  private final static String DEFAULT_NUMTHREAD  = CommonConfig.NUM_PROCESSING_THREADS_DEFAULT;
  private final static String DEFAULT_ACTIVE     = CommonConfig.DEFAULT_ACTIVE;

  //performance counters
  private long processingTime = 0;
  private long batchRecordsProcessed = 0;
  private long streamsProcessed = 0;
  private int  outBufferCapacity = 0;
  private int  bufferHits = 0;

  // this is used to control the active status
  private boolean active = true;

  /**
   * This is used for managing exceptions. Defined here to keep the messages
   * as short and as in line as possible in the modules.
   */
  protected String message;

 /**
  * constructor
  */
  protected AbstractPlugIn()
  {
    super();
  }

 /**
  * Initialise the module. Called during pipeline creation.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    String ConfigHelper;

    // set our name
    setSymbolicName(ModuleName);

    // Get the pipeline reference
    setPipeline(OpenRate.getPipelineFromMap(PipelineName));
            
    // Get the batch size we should be working on
    ConfigHelper = initGetBatchSize();
    processControlEvent(SERVICE_BATCHSIZE, true, ConfigHelper);
    ConfigHelper = initGetBufferSize();
    processControlEvent(SERVICE_BUFFERSIZE, true, ConfigHelper);
    ConfigHelper = initGetNumThread();
    processControlEvent(SERVICE_NUMTHREAD, true, ConfigHelper);
    ConfigHelper = initGetActive();
    processControlEvent(SERVICE_ACTIVE, true, ConfigHelper);

    // register us with the client manager
    registerClientManager();
  }

  /**
   * Thread execution method. Inherited from Runnable. All this method does is
   * call process() and catch any processing exception. Any exceptions that
   * occur in the processing are intercepted and passed back via the exception
   * handler that we nominated during the pipeline creation
   */
  @Override
  public void run()
  {
    // monitor the inbound buffer
    getBatchInbound().registerMonitor(this);

    process();
  }

 /**
  * Process() gets the records from the upstream FIFO and iterates through all
  * of the records in the collection that is gets. For each of these records,
  * the appropriate method is called in the implementation class, and the
  * results are collected and are emptied out of the downstream FIFO.
  *
  * This method id triggered by the wait/notify mechanism and wakes up as soon
  * as a batch of records is pushed into the input buffer (either real time or
  * batch records).
  *
  * This method must be thread safe as it is the fundamental multi-processing
  * hook in the framework.
  *
  * This module implements batch/real time convergence. It can be triggered by
  * either batch or real time events, but in any case, real time events are
  * processed in precedence to the batch events. This is achieved by processing
  * all real time events through the pipeline between each batch event. This
  * creates a very high priority path through the pipe for real time events.
  */
  @Override
  public void process()
  {
    Iterator<IRecord> iter;
    long startTime;
    long endTime;
    long BatchTime;

    // processing list for batch events
    Collection<IRecord> in;

    // Print the thread startup message
    OpenRate.getOpenRateStatsLog().debug("PlugIn <" + Thread.currentThread().getName() +
                   "> started, pulling from buffer <" + getBatchInbound().toString() +
                   ">, pushing to buffer <" + getBatchOutbound().toString() + ">");

    // Check to see if we have the naughty batch size of 0. this is usually
    // because someone has overwritten the init() without calling the parent
    // init
    if (getBatchSize() == 0)
    {
      message = "Batch size is 0 in plugin <" + this.toString() + ">. " +
              "Please ensure that you have called the parent init().";
      getExceptionHandler().reportException(new ProcessingException(message,getSymbolicName()));
    }

    // main thread loop. This will not be exited until the thread is
    // ordered to shut down.
    while (true)
    {
      // get the timestamp of the start of the processing. This will happen on
      // each thread wake up
      startTime = System.currentTimeMillis();

      // get the batch records to process
      in = getBatchInbound().pull(getBatchSize());

      int ThisBatchRecordCount = in.size();

      if (ThisBatchRecordCount > 0)
      {
        // If the active flag is set, we do the processing for real
        // if it is not set, we only manage the transaction
        if (    isActive())
        {
          // Active loop
          iter = in.iterator();

          // Process each of the block of records and trigger the processing
          // functions for each type (header, trailer, valid and error)
          while (iter.hasNext())
          {
            try
            {
              // Get the formatted information from the record
              IRecord r = iter.next();

              // Trigger the correct user level functions according to the state of
              // the record
              if (r.isValid())
              {
                procValidRecord(r);
              }
              else
              {
                if (r.isErrored())
                {
                  procErrorRecord(r);
                }
                else
                {
                  if (r instanceof HeaderRecord)
                  {
                    r = procHeader(r);
                    streamsProcessed++;
                  }

                  if (r instanceof TrailerRecord)
                  {
                    procTrailer(r);
                  }
                }
              }
            } // try
            catch (ProcessingException pe)
            {
              getPipeLog().error("Processing exception caught in Plug In <" +
                          getSymbolicName() + ">. See Error Log for the Stack Trace.");

              getExceptionHandler().reportException(pe);
            }
            catch (ClassCastException cce)
            {
              getPipeLog().error("Record Class Cast exception caught in Plug In <" +
                          getSymbolicName() + ">. See Error Log for the Stack Trace.");

              getExceptionHandler().reportException(new ProcessingException(cce,getSymbolicName()));
            }
            catch (NullPointerException npe)
            {
              getPipeLog().error("Null pointer exception caught in Plug In <" +
                          getSymbolicName() + ">. See Error Log for the Stack Trace.");

              getExceptionHandler().reportException(new ProcessingException(npe,getSymbolicName()));
            }
            catch (ArrayIndexOutOfBoundsException aiob)
            {
              getPipeLog().error("Array Index Out of Bounds exception caught in Plug In <" +
                          getSymbolicName() + ">. See Error Log for the Stack Trace.");

              getExceptionHandler().reportException(new ProcessingException(aiob,getSymbolicName()));
            }
            catch (Exception ge)
            {
                getPipeLog().fatal("General exception caught in Plug In <" +
                          getSymbolicName() + ">. See Error Log for the Stack Trace.");

                getExceptionHandler().reportException(new ProcessingException(ge,getSymbolicName()));
            }
            catch (Throwable t)
            {
              getPipeLog().fatal("Unexpected exception caught in Plug In <" +
                        getSymbolicName() + ">. See Error Log for the Stack Trace.");

              getExceptionHandler().reportException(new ProcessingException(t,getSymbolicName()));
              }
            }
          }
          else
          {
            if (this instanceof AbstractTransactionalPlugIn)
            {
              // Inactive loop - we only need to do this for transactional modules
              // if the module is non transactional, we need do nothing
              iter = in.iterator();

              // Process each of the block of records and trigger the processing
              // functions for each type (header, trailer, valid and error)
              while (iter.hasNext())
              {
                try
                {
                  // Get the formatted information from the record
                  IRecord r = iter.next();

                  // Trigger the correct user level functions according to the state of
                  // the record
                  if (r.isValid())
                  {
                    // nothing
                  }
                  else
                  {
                    if (r.isErrored())
                    {
                      // nothing
                    }
                    else
                    {
                      if (r instanceof HeaderRecord)
                      {
                        r = procHeader(r);
                        streamsProcessed++;
                      }

                      if (r instanceof TrailerRecord)
                      {
                        procTrailer(r);
                      }
                    }
                  } // else
              } // try
              catch (ClassCastException cce)
              {
                getPipeLog().error("Record Class Cast exception caught in Plug In <" +
                            getSymbolicName() + ">. See Error Log for the Stack Trace.");

                getExceptionHandler().reportException(new ProcessingException(cce,getSymbolicName()));
              }
              catch (NullPointerException npe)
              {
                getPipeLog().error("Null pointer exception caught in Plug In <" +
                            getSymbolicName() + ">. See Error Log for the Stack Trace.");

                getExceptionHandler().reportException(new ProcessingException(npe,getSymbolicName()));
              }
              catch (ArrayIndexOutOfBoundsException aiob)
              {
                getPipeLog().error("Array Index Out of Bounds exception caught in Plug In <" +
                            getSymbolicName() + ">. See Error Log for the Stack Trace.");

                getExceptionHandler().reportException(new ProcessingException(aiob,getSymbolicName()));
              }
              catch (Throwable t)
              {
                getPipeLog().fatal("Unexpected exception caught in Plug In <" +
                          getSymbolicName() + ">. See Error Log for the Stack Trace.");

                getExceptionHandler().reportException(new ProcessingException(t,getSymbolicName()));
                }
              } // while
            }
          }

          getBatchOutbound().push(in);
          OpenRate.getOpenRateStatsLog().debug("PlugIn <" + Thread.currentThread().getName() + "> pushed <" + String.valueOf(ThisBatchRecordCount) + "> batch records to buffer <" + getBatchOutbound().toString() + ">");

          outBufferCapacity = getBatchOutbound().getEventCount();

          endTime = System.currentTimeMillis();
          BatchTime = (endTime - startTime);
                setProcessingTime(getProcessingTime() + BatchTime);

          while (outBufferCapacity > getBufferSize())
          {
                    setBufferHits(getBufferHits() + 1);
            OpenRate.getOpenRateStatsLog().debug("PlugIn <" + Thread.currentThread().getName() + "> buffer high water mark! Buffer max = <" + getBufferSize() + "> current count = <" + outBufferCapacity + ">");
            try
            {
              Thread.sleep(100);
            }
            catch (InterruptedException ex)
            {
              // Nothing
            }
            outBufferCapacity = getBatchOutbound().getEventCount();
          }

          OpenRate.getOpenRateStatsLog().info(
            "Plugin <" + Thread.currentThread().getName() + "> processed <" +
            String.valueOf(ThisBatchRecordCount) + "> events in <" + BatchTime + "> ms" );

          // Update the statistics
                setBatchRecordsProcessed(getBatchRecordsProcessed() + ThisBatchRecordCount);
        }
        else
        {
          OpenRate.getOpenRateStatsLog().debug("PlugIn <" + Thread.currentThread().getName() + "> going to sleep");

          // We want to shut down the processing
          if (shutdownFlag == true)
          {
            OpenRate.getOpenRateStatsLog().debug("PlugIn <" + Thread.currentThread().getName() + "> shut down. Exiting.");
            break;
          }

          // If not marked for shutdown, wait for notification from the
          // suppler that new records are available for processing.
          try
          {
            synchronized (this)
            {
              wait();
            }
          }
          catch (InterruptedException e)
          {
            // ignore interrupt exceptions
          }
        } // else
      } // while loop
  }

  /**
   * Shuts down the PlugIn. Use this to save any configuration or data before
   * the plug in closes
   */
  @Override
  public void shutdown()
  {
    // Not currently used
  }

  /**
   * Tell the plug in to shutdown after it has done all of the work that
   * is currently waiting. The pipeline assumes that this is the case when
   * no more records are found for the whole of a cycle.
   */
  @Override
  public void markForShutdown()
  {
    //log.debug("PlugIn <" + getSymbolicName() + "> marked for exit....");
    this.shutdownFlag = true;

    // notify any listeners that are waiting that we are flushing
    synchronized (this)
    {
      notifyAll();
    }
  }

  /**
   * Reset the plug in to ensure that it's ready to process records again
   * after it has been exited. This method must be called after calling
   * markForExit() to reset the state of the PlugIn.
   */
  @Override
  public void reset()
  {
    //log.debug("reset called on PlugIn <" + getSymbolicName() + ">");
    this.shutdownFlag = false;
  }

// -----------------------------------------------------------------------------
// -------------------- Start of local utility functions -----------------------
// -----------------------------------------------------------------------------

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetBatchSize() throws InitializationException
  {
    String tmpValue;

    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),symbolicName,SERVICE_BATCHSIZE, DEFAULT_BATCHSIZE);
    return tmpValue;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetBufferSize() throws InitializationException
  {
    String tmpValue;

    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),symbolicName,SERVICE_BUFFERSIZE, DEFAULT_BUFFERSIZE);
    return tmpValue;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetNumThread() throws InitializationException
  {
    String tmpValue;

    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),symbolicName,SERVICE_NUMTHREAD, DEFAULT_NUMTHREAD);
    return tmpValue;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetActive() throws InitializationException
  {
    String tmpValue;

    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),symbolicName,SERVICE_ACTIVE, DEFAULT_ACTIVE);

    return tmpValue;
  }

 /**
  * return the symbolic name
  *
  * @return The symbolic name for this plugin
  */
  @Override
  public String getSymbolicName()
  {
      return symbolicName;
  }

 /**
  * set the symbolic name
  *
  * @param Name The new symbolic name for this plugin
  */
  @Override
  public void setSymbolicName(String Name)
  {
      symbolicName=Name;
  }

 /**
  * Do any non-record level processing required to finish this batch cycle. This
  * is used for scheduling and flushing through the pipeline
  *
  * @return The number of events in the output FIFO buffer
  */
  @Override
  public int getOutboundRecordCount()
  {
    outBufferCapacity = getBatchOutbound().getEventCount();

    return outBufferCapacity;
  }

// -----------------------------------------------------------------------------
// ----------------------- Start of IMonitor functions -------------------------
// -----------------------------------------------------------------------------

 /**
  * Simple implementation of Monitor interface based on Thread
  * wait/notify mechanism.
  *
  * @param e The monitor event
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
// ----------------- Start of published hookable functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting. In this case we have to open a new
  * dump file each time a stream starts.
  *
  * @param r The record we are working on
  * @return The processed record
  */
  public abstract IRecord procHeader(IRecord r);

 /**
  * This is called when a data record is encountered. You should do any normal
  * processing here.
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  public abstract IRecord procValidRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  public abstract IRecord procErrorRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished. In this example, all we do is
  * pass the control back to the transactional layer.
  *
  * @param r The record we are working on
  * @return The processed record
  */
  public abstract IRecord procTrailer(IRecord r);

 /**
  * This is called when a RT data record is encountered. You should do any normal
  * processing here. For most purposes this is steered to the normal (batch)
  * processing, but this can be overwritten
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  @Override
  public IRecord procRTValidRecord(IRecord r) throws ProcessingException
  {
    // pass through to batch version
    return procValidRecord(r);
  }

 /**
  * This is called when a RT data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction! For most purposes this is steered
  * to the normal (batch) processing, but this can be overwritten
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  @Override
  public IRecord procRTErrorRecord(IRecord r) throws ProcessingException
  {
    return procErrorRecord(r);
  }

 /**
  * Gathers and caches property configurations from the properties file. Because
  * this may happen many times per file, we cache the value for speed. If you
  * don't want caching, using the non-runtime version.
  *
  * @param propertyName the property name we are looking for
  * @return the value we found, or null if none found
  */
  public String getPropertyValueRunTime(String propertyName)
  {
    if (configurationParameters.containsKey(propertyName))
    {
      return configurationParameters.get(propertyName);
    }
    else
    {
      // get it, store it, then return it
      String propertyValue = null;
      try
      {
        propertyValue = PropertyUtils.getPropertyUtils().getPluginPropertyValue(getPipeName(), getSymbolicName(), propertyName);
      }
      catch (InitializationException ex)
      {
        getPipeLog().error("Failure getting ad hoc property <" + propertyName + ">");
      }
      configurationParameters.put(propertyName, propertyValue);
      return propertyValue;
    }
  }

 /**
  * Gathers property configurations from the properties file. No caching or
  * storing is done, therefore this method is not meant for repeated use, for
  * example at run time.
  *
  * @param propertyName the property name we are looking for
  * @return the value we found, or null if none found
  * @throws InitializationException
  */
  public String getPropertyValue(String propertyName) throws InitializationException
  {
    // Helpful error handling
    if ((getPipeName() == null) || (getSymbolicName().equals("Unknown")))
    {
      message = "Module initialisation not done in module <" + getSymbolicName() + ">. Did you call super.init()?";
      throw new InitializationException(message,getSymbolicName());
    }

    // get it, store it, then return it
    String propertyValue = null;
    try
    {
      propertyValue = PropertyUtils.getPropertyUtils().getPluginPropertyValue(getPipeName(), getSymbolicName(), propertyName);
    }
    catch (InitializationException ex)
    {
      getPipeLog().error("Failure getting ad hoc property <" + propertyName + ">");
    }

    return propertyValue;
  }

 /**
  * Gathers property configurations from the properties file. No caching or
  * storing is done, therefore this method is not meant for repeated use, for
  * example at run time.
  *
  * @param propertyName the property name we are looking for
  * @param defaultValue the default value if none is available
  * @return the value we found, or null if none found
  * @throws InitializationException
  */
  public String getPropertyValueDef(String propertyName, String defaultValue) throws InitializationException
  {
    // Helpful error handling
    if ((getPipeName() == null) || (getSymbolicName().equals("Unknown")))
    {
      message = "Module initialisation not done in module <" + getSymbolicName() + ">. Did you call super.init()?";
      throw new InitializationException(message,getSymbolicName());
    }

    // get it, store it, then return it
    String propertyValue = defaultValue;
    try
    {
      propertyValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(), getSymbolicName(), propertyName, defaultValue);
    }
    catch (InitializationException ex)
    {
      getPipeLog().error("Failure getting ad hoc property <" + propertyName + ">");
    }

    return propertyValue;
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
    //Register this Client
    ClientManager.getClientManager().registerClient(getPipeName(),getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BATCHSIZE,  ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BUFFERSIZE, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_NUMTHREAD,  ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATS,      ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATSRESET, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_ACTIVE,     ClientManager.PARAM_DYNAMIC);
  }

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
  *
  * @param Command The command that we are to work on
  * @param Init True if the pipeline is currently being constructed
  * @param Parameter The parameter value for the command
  * @return The result message of the operation
  */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {

    int ResultCode = -1;
    double CDRsPerSec;

    // Set the batch size
    if (Command.equalsIgnoreCase(SERVICE_BATCHSIZE))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(getBatchSize());
      }
      else
      {
        try
        {
          batchSize = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          getPipeLog().error("Invalid number for batch size. Passed value = <" + Parameter + ">");
        }
        ResultCode = 0;
      }
    }

    // Set the batch size
    if (Command.equalsIgnoreCase(SERVICE_BUFFERSIZE))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(getBufferSize());
      }
      else
      {
        try
        {
                    setBufferSize(Integer.parseInt(Parameter));
        }
        catch (NumberFormatException nfe)
        {
          getPipeLog().error("Invalid number for buffer size. Passed value = <" + Parameter + ">");
        }
        ResultCode = 0;
      }
    }

    // Reset the Statistics
    if (Command.equalsIgnoreCase(SERVICE_STATSRESET))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        setProcessingTime(0);
                setBatchRecordsProcessed(0);
        streamsProcessed = 0;
        setBufferHits(0);
        ResultCode = 0;
      }
      else
      {
        return "false";
      }
    }

    // Return the Statistics
    if (Command.equalsIgnoreCase(SERVICE_STATS))
    {
      if (  getProcessingTime() == 0)
      {
        CDRsPerSec = 0;
      }
      else
      {
        CDRsPerSec = (double)((getBatchRecordsProcessed()*1000)/getProcessingTime());
      }

      return Long.toString(getBatchRecordsProcessed()) + ":" +
             Long.toString(getProcessingTime()) + ":" +
             Long.toString(streamsProcessed) + ":" +
             Double.toString(CDRsPerSec) + ":" +
             Long.toString(outBufferCapacity) + ":" +
             Long.toString(getBufferHits()) + ":" +
             Long.toString(getBatchInbound().getEventCount());
    }

    if (Command.equalsIgnoreCase(SERVICE_NUMTHREAD))
    {
      if (Parameter.equals(""))
      {
        return Integer.toString(numThreads);
      }
      else
      {
        try
        {
          numThreads = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          getPipeLog().error("Invalid number for number of threads. Passed value = <" + Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    // Reset the Statistics
    if (Command.equalsIgnoreCase(SERVICE_ACTIVE))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        active = true;
        ResultCode = 0;
      }
      else if (Parameter.equalsIgnoreCase("false"))
      {
        active = false;
        ResultCode = 0;
      }
      else
      {
        // return the current status
        if (    isActive())
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
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }

  // -----------------------------------------------------------------------------
  // -------------------- Standard getter/setter functions -----------------------
  // -----------------------------------------------------------------------------

    /**
     * @return the BatchSize
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return the Active
     */
    public boolean isActive() {
        return active;
    }
    
    /**
     * @return the streamsProcessed
     */
    public long getStreamsProcessed() {
        return streamsProcessed;
    }

   /**
    * Increments the number of streams processed
    */
    public void incStreamsProcessed()
    {
      streamsProcessed++;
    }

    /**
     * @return the bufferHits
     */
    public int getBufferHits() {
        return bufferHits;
    }

    /**
     * Increment the buffer hits
     */
    public void incBufferHits() {
        this.setBufferHits(this.getBufferHits() + 1);
    }

    /**
     * @return the processingTime
     */
    public long getProcessingTime() {
        return processingTime;
    }

    /**
     * @param processingTime the processingTime to set
     */
    public void setProcessingTime(long processingTime) {
        this.processingTime = processingTime;
    }

    /**
     * @param processingTimeUpdate the processingTime to set
     */
    public void updateProcessingTime(long processingTimeUpdate) {
        this.processingTime += processingTimeUpdate;
    }

    /**
     * @param bufferHits the bufferHits to set
     */
    public void setBufferHits(int bufferHits) {
        this.bufferHits = bufferHits;
    }

    /**
     * @return the bufferSize
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * @param bufferSize the bufferSize to set
     */
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * @return the batchRecordsProcessed
     */
    public long getBatchRecordsProcessed() {
        return batchRecordsProcessed;
    }

    /**
     * @param batchRecordsProcessed the batchRecordsProcessed to set
     */
    public void setBatchRecordsProcessed(long batchRecordsProcessed) {
        this.batchRecordsProcessed = batchRecordsProcessed;
    }
    
    /**
     * @param batchRecordsProcessedUpdate the batchRecordsProcessed to set
     */
    public void updateBatchRecordsProcessed(long batchRecordsProcessedUpdate) {
        this.batchRecordsProcessed += batchRecordsProcessedUpdate;
    }
    
  /**
   * Set the inbound delivery mechanism.
   *
   * @param s The supplier FIFO
   */
  @Override
  public void setInbound(ISupplier s)
  {
    this.supplier = s;
  }

  /**
   * supplier configured on the inbound side of the PlugIn.
   *
   * @return The supplier FIFO
   */
  public ISupplier getBatchInbound()
  {
    return this.supplier;
  }

  /**
   * Set the outbound delivery mechanism.
   *
   * @param c The consumer FIFO
   */
  @Override
  public void setOutbound(IConsumer c)
  {
    this.consumer = c;
  }

  /**
   * consumer configured on the outbound side of this PlugIn.
   *
   * @return The consumer FIFO
   */
  public IConsumer getBatchOutbound()
  {

    return this.consumer;
  }

  /**
   * Set the error delivery mechanism.
   *
   * @param err The error consumer FIFO
   */
  @Override
  public void setErrorBuffer(IConsumer err)
  {
    this.errors = err;
  }

  /**
   * return error buffer
   *
   * @return The error consumer FIFO
   */
  public IConsumer getErrorBuffer()
  {

    return this.errors;
  }

  /**
   * Set the exception handler mechanism.
   *
   * @param handler The exception handler to be used for this class
   */
  @Override
  public void setExceptionHandler(ExceptionHandler handler)
  {
    this.handler = handler;
  }

  /**
   * return exception handler
   *
   * @return The exception handler to be used for this class
   */
  public ExceptionHandler getExceptionHandler()
  {

    return this.handler;
  }

  /**
   * Method setNumThreads.
   *
   * @param val The number of threads to use for this plug-in
   */
  public void setNumThreads(int val)
  {
    numThreads = val;
  }

  /**
   * Return the suggested number of threads to launch for this PlugIn.
   * The number of threads is '1' by default.
   *
   * @return The number of threads to use for this plugin
   */
  @Override
  public int numThreads()
  {
    return this.numThreads;
  }

 /**
  * Return whether we have been asked to shutdown.
  *
  * @return true if a shutdown has been requested, otherwise false
  */
  public boolean getShutdownFlag()
  {
    return shutdownFlag;
  }

    /**
     * @return the pipeline
     */
    public IPipeline getPipeline() {
        return pipeline;
    }

    /**
     * @param pipeline the pipeline to set
     */
    public void setPipeline(IPipeline pipeline) {
        this.pipeline = pipeline;
    }
    
   /**
    * Get the name of the pipeline.
    * 
    * @return The pipeline name
    */
    public String getPipeName()
    {
      return pipeline.getSymbolicName();
    }
    
   /**
    * Get the logger for this pipeline.
    * 
    * @return The pipeline logger
    */
    public ILogger getPipeLog()
    {
      return pipeline.getPipeLog();
    }
}
