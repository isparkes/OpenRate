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
package OpenRate.adapter.realTime;

import OpenRate.audit.AuditUtils;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
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
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractRTAdapter.java,v $, $Revision: 1.17 $, $Date: 2013-05-13 18:12:12 $";

  // The symbolic name is used in the management of the pipeline (control and
  // thread monitoring) and logging.
  private String SymbolicName;

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

 /**
  * This is the pipeline that we are in, used for logging and property retrieval
  */
  protected String pipeName;

  /**
   * The exception handler that we use for reporting runtime errors
   */
  protected ExceptionHandler handler;

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
  * Contructor
  */
  public void AbstractRTAdapter()
  {
    // Add the version map
    AuditUtils.getAuditUtils().buildHierarchyVersionMap(this.getClass());

  }

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
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName) throws InitializationException
  {
    setSymbolicName(ModuleName);

    // store the pipe we are in
    this.pipeName = PipelineName;

    // Get the pipe log
    PipeLog = LogUtil.getLogUtil().getLogger(PipelineName);
    // Get the debug status
    String ConfigHelper = PropertyUtils.getPropertyUtils().getRTAdapterPropertyValueDef(PipelineName, ModuleName, "Debug", "false");

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
  * Set the exception handler mechanism.
  *
  * @param handler The parent handler to set
  */
  @Override
  public void setExceptionHandler(ExceptionHandler handler)
  {
    this.handler = handler;
  }

 /**
  * return exception handler
  *
  * @return The parent handler that is currently in use
  */
  public ExceptionHandler getExceptionHandler()
  {
    return this.handler;
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
      PipeLog.error("Processing exception caught in Output Adapter <" +
                getSymbolicName() + ">", pe);
      getExceptionHandler().reportException(pe);
    }
    catch (Throwable t)
    {
      PipeLog.fatal("Unexpected exception caught in Plug In <" +
                getSymbolicName() + ">", t);
      getExceptionHandler().reportException(new ProcessingException(t));
    }
  }

  /**
   * The write method for the real time adapter is an empty loop, so that we
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void write() throws ProcessingException
  {
    while (true)
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
    throw new UnsupportedOperationException("Not supported yet.");
  }

 /**
  * RT streams are auto-managing and don't need to be opened or closed. If any
  * caller tried to do this, it would be wrong.
  */
  @Override
  public void closeStream()
  {
    throw new UnsupportedOperationException("Not supported yet.");
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
    IRecord tmpRecord = recordToProcess;

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
            PipeLog.error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
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
            PipeLog.error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
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
          PipeLog.error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
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
          PipeLog.error("Processing exception <"+ex.getMessage()+"> caught in adapter <"+tmpPlugin.getSymbolicName()+">");
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
    ClientManager.registerClient(pipeName,getSymbolicName(), this);

    //Register services for this Client
    //ClientManager.registerClientService(getSymbolicName(), SERVICE_BATCHSIZE, ClientManager.PARAM_MANDATORY);
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
                       getSymbolicName() + "> in pipe <" + pipeName +">";
      PipeLog.debug(logStr);

      return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }

}
