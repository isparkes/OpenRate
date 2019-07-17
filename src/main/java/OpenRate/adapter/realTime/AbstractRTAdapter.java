
package OpenRate.adapter.realTime;

import OpenRate.IPipeline;
import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.ILogger;
import OpenRate.process.IPlugIn;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class implements the real time (RT) adapter for the OpenRate framework.
 * This real time adapter is in charge of accepting and marshalling requests
 * for processing, and gathering the responses and returning them.
 *
 * The concrete implementation of this class deals with the transport of the
 * events and selects the appropriate communication mechanism.
 *
 * @author ian
 */
public abstract class AbstractRTAdapter implements IRTAdapter
{
  // The symbolic name is used in the management of the pipeline (control and
  // thread monitoring) and logging.
  private String SymbolicName;
  
  /**
   * Flag that marks adapters shutdown
   */
  private volatile boolean shutdown = false;

 /**
  * This is the pipeline that we are in, used for logging and property retrieval
  */
  protected IPipeline pipeline;

  // used to create the output batches
  int outputCounter = 0;

  /**
   * used to number records that we process
   */
  protected int currRecordNumber;

  // The list of plugins that have been configured in the pipeline
  private ArrayList<IPlugIn> PlugInList;

  /**
   * used to control if the performance sensitive process thread debugs or not
   */
  protected boolean debugging = false;

 /**
  * Get the ID of the thread that is currently being used in this context.
  *
  * @return The thread ID
  */
  public String getThreadID()
  {
    return "Real Time Adapter";
  }

  /**
   * Injects the pipeline plugin list into the adapter so we can process records
   * by running them down the chain one by one
   *
   * @param PlugInList
   */
  @Override
  public void setProcessingList(ArrayList<IPlugIn> PlugInList)
  {
    this.PlugInList = PlugInList;
  }

 /**
  * Initialise the module. Called during pipeline creation.
  *
  * @param pipelineName The name of the pipeline this module is in
  * @param moduleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String pipelineName, String moduleName) throws InitializationException
  {
    setSymbolicName(moduleName);

    // store the pipe we are in
    setPipeline(OpenRate.getPipelineFromMap(pipelineName));

    // Get the debug status
    String ConfigHelper = PropertyUtils.getPropertyUtils().getRTAdapterPropertyValueDef(pipelineName, moduleName, "Debug", "false");

    if (ConfigHelper.equals("true"))
    {
      debugging = true;
    }
  }

  /**
   * Perform any cleanup. Called by the OpenRateApplication during application
   * shutdown. This should do any final cleanup and closing of resources.
   * Note: It is not called during normal processing, so it's only useful for
   * true shutdown logic.
   */
  @Override
  public void cleanup()
  {
    // NOP
  }

 /**
  * Launch the daemon processing proper. This performs two main tasks:
  * 1) Create the thread in which the socket server runs and fork it. This
  *    means that the adapter can accept incoming connections and process the
  *    requests that arrive via them. Each connection will be launched in its
  *    own thread. In effect, this is the implementation of the INPUT side of
  *    the adapter.
  * 2) Create the thread that will receive the records from the end of the
  *    pipeline, and process them. This uses the normal notify/wait processing.
  *    This is the OUTPUT side of the adapter.
  */
  @Override
  public void run()
  {
    // Set up the input listenener
    initialiseInputListener();

    // the "write" method goes into an infinite wait/notify loop, accepting
    // the wake up notify events, and processin anything that has arrived
    try
    {
      // This is the main processing loop, which will not exit until the
      // thread is commanded to terminate.
      write();
    }
    catch (ProcessingException pe)
    {
      getPipeLog().error("Processing exception caught in Output Adapter <" +
                getSymbolicName() + ">", pe);
      getExceptionHandler().reportException(pe);
    }
    catch (Throwable t)
    {
      getPipeLog().fatal("Unexpected exception caught in Plug In <" +
                getSymbolicName() + ">", t);
      getExceptionHandler().reportException(new ProcessingException(t,getSymbolicName()));
    }
  }

  /**
   * The write method for the real time adapter is an empty loop, so that we
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void write() throws ProcessingException
  {
    while (!shutdown)
    {
      // If not marked for shutdown, wait for notification from the
      // suppler that new records are available for processing.
      try
      {
        synchronized (this)
        {
          //log.info("Output adapter <" + getSymbolicName() + "> waiting" );
          wait();
        }
      }
      catch (InterruptedException e)
      {
        // ignore
      }
    } // while
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
    // Do nothing
  }

  /**
   * MarkForClosedown tells the adapter thread to close at the first chance,
   * usually as soon as an idle cycle is detected
   */
  @Override
  public void markForClosedown()
  {
  	// Mark adapter for shutdown
  	shutdown = true;
	  
    // Shutdown the listener
    shutdownInputListener();

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
  }

 /**
  * RT streams are auto-managing and don't need to be opened or closed. If any
  * caller tried to do this, it would be wrong.
  */
  @Override
  public void closeStream()
  {
  }

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
   * @param name The symbolic name for this class stack
   */
  @Override
  public void setSymbolicName(String name)
  {
    SymbolicName = name;
  }

  /**
   * Processes a record through the chain, passing the result back. This
   * overloaded method allows us to automate the conversion process (because
   * we are getting a FlatRecord, it has to be converted)
   *
   * @param recordToProcess the record we are going to work on
   * @return the processed record
   * @throws ProcessingException
   */
  @Override
  public FlatRecord processRTRecord(FlatRecord recordToProcess)
          throws ProcessingException
  {
    Iterator<IPlugIn> pluginIter = PlugInList.iterator();
    IRecord tmpRecord;

    IPlugIn tmpPlugin;

    // perform the input mapping - we know to do this because our input
    // record is a FlatRecord
    tmpRecord = performInputMapping(recordToProcess);

    // drop suppressed records
    if (tmpRecord != null)
    {
      // Process through the chain
      while (pluginIter.hasNext())
      {
        tmpPlugin = pluginIter.next();

        if (tmpRecord.isErrored())
        {
          try
          {
            tmpPlugin.procRTErrorRecord(tmpRecord);
          }
          catch (ProcessingException ex)
          {
            getPipeLog().error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
          }
        }
        else
        {
          try
          {
            tmpPlugin.procRTValidRecord(tmpRecord);
          }
          catch (ProcessingException ex)
          {
            getPipeLog().error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
          }
        }
      }

      // perform the output mapping - we know to do this because our output
      // record is a FlatRecord
      if (tmpRecord.isErrored())
      {
        tmpRecord = performErrorOutputMapping(tmpRecord);
      }
      else
      {
        tmpRecord = performValidOutputMapping(tmpRecord);
      }
    }

    return (FlatRecord) tmpRecord;
  }

  /**
   * Processes a record through the chain, passing the result back. This
   *
   * @param recordToProcess the record we are going to work on
   * @return the processed record
   * @throws ProcessingException
   */
  @Override
  public IRecord processRTRecord(IRecord recordToProcess)
          throws ProcessingException
  {
    Iterator<IPlugIn> pluginIter = PlugInList.iterator();
    IRecord tmpRecord = recordToProcess;

    IPlugIn tmpPlugin;

    // Process through the chain
    while (pluginIter.hasNext())
    {
      tmpPlugin = pluginIter.next();

      if (tmpRecord.isErrored())
      {
        try
        {
          tmpPlugin.procRTErrorRecord(tmpRecord);
        }
        catch (ProcessingException ex)
        {
          getPipeLog().error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
        }
      }
      else
      {
        try
        {
          tmpPlugin.procRTValidRecord(tmpRecord);
        }
        catch (ProcessingException ex)
        {
          getPipeLog().error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
        }
      }
    }

    return tmpRecord;
  }

  // -----------------------------------------------------------------------------
  // ----------------- Start of published hookable functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * This method takes the incoming real time record, and prepares it for
  * submission into the processing pipeline.
  *
  * @param RTRecordToProcess The real time record to map
  * @return The mapped real time record
  * @throws ProcessingException
  */
  @Override
  public abstract IRecord performInputMapping(FlatRecord RTRecordToProcess) throws ProcessingException;

 /**
  * This method takes the outgoing real time record, and prepares it for
  * returning to the submitter.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  * @throws ProcessingException
  */
  @Override
  public abstract FlatRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException;

 /**
  * This method takes the outgoing real time record, and prepares it for
  * returning to the submitter.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  */
  @Override
  public abstract FlatRecord performErrorOutputMapping(IRecord RTRecordToProcess);

 /**
  * This is called when a data record is encountered. You should do any normal
  * processing here.
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  public abstract IRecord procInputValidRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  public abstract IRecord procInputErrorRecord(IRecord r) throws ProcessingException;

 /**
  * This is called when a data record is encountered. You should do any normal
  * processing here.
  *
  * @param r The record we are working on
  * @return The processed record
  */
  public abstract IRecord procOutputValidRecord(IRecord r);

 /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  *
  * @param r The record we are working on
  * @return The processed record
  */
  public abstract IRecord procOutputErrorRecord(IRecord r);

 /**
  * initialise the input listener
  */
  public void initialiseInputListener()
  {
    // Do nothing - this will be overridden in case it is needed
  }

 /**
  * shutdown the input listener
  */
  public void shutdownInputListener()
  {
    // Do nothing - this will be overridden in case it is needed
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
    ClientManager.getClientManager().registerClient(getPipeName(),getSymbolicName(), this);

    //Register services for this Client
    //ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BATCHSIZE, ClientManager.PARAM_MANDATORY);
  }

  /**
  * ProcessControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
   *
  * @param Command The command that we are to work on
  * @param Init True if the pipeline is currently being constructed
  * @param Parameter The parameter value for the command
  * @return The result message of the operation
   */
  public String ProcessControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;


    if (ResultCode == 0)
    {
      String logStr = "Command <" + Command + "> handled by plugin <" +
                       getSymbolicName() + "> in pipe <" + getPipeName() +">";
      getPipeLog().debug(logStr);

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
     * @return the pipeName
     */
    public String getPipeName() {
      return pipeline.getSymbolicName();
    }

    /**
     * @return the pipeline
     */
    public IPipeline getPipeline() {
      return pipeline;
    }

 /**
  * Set the pipeline reference so the input adapter can control the scheduler
  *
  * @param pipeline the Pipeline to set
  */
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
