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
 ** 7) You agree to disclose any changes to this work to the copyright holder
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

package OpenRate;

import OpenRate.adapter.IInputAdapter;
import OpenRate.adapter.IOutputAdapter;
import OpenRate.adapter.realTime.IRTAdapter;
import OpenRate.buffer.IBuffer;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.process.IPlugIn;
import OpenRate.transaction.ISyncPoint;
import OpenRate.transaction.TransactionManager;
import OpenRate.transaction.TransactionManagerFactory;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * The Pipeline encapsulates the pipeline as defines as Input adapter,
 * some processing modules, Output adapter. This allows the easy configuration
 * of multiple pipelines within a single framework. Multiple pipes in a frame-
 * work are able to share resources and memory.
 *
 * The pipeline is constructed entirely in the ConfigurePipeline procedure,
 * which instantiates the adapters and creates the main pipeline thread, adding
 * the input, output and processing modules as threads in a dedicated thread
 * group, or in the main thread as necessary.
 *
 * This version now supports output adapter chaining, which means that a single
 * record can go to multiple outputs. Not defining an output means that all
 * outputs should be written to.
 */
public class Pipeline
  implements IPipeline,
             IEventInterface,
             ISyncPoint
{
  // Get the logs, for this and all child classes. The pipe log will be
  // intialised during the init, up until then, all logging will go to the
  // framework FWLog, or the default logger (console).

  /**
   * Framework logger. This logger is used for logging information from the
   * framework level, which include cache messages initialisation messages and
   * framework level events.
   */
  protected ILogger FWLog         = LogUtil.getLogUtil().getLogger("Framework");

  /**
   * Error AstractLogger. This logger is used for capturing stack traces and other
   * "verbose" errors, which is intended to leave the other logs free of
   * stack dumps, and to centralise the place in which critical errors are
   * reported.
   */
  protected ILogger ErrorLog      = LogUtil.getLogUtil().getLogger("ErrorLog");

  /**
   * Statistics AstractLogger. This logger gives you an overview of the processing
   * statistics during the run, and aids in locating processing bottlenecks
   * and monitoring.
   */
  protected ILogger StatsLog      = LogUtil.getLogUtil().getLogger("Statistics");

  /**
   * Pipeline Level AstractLogger. This logger is used for logging messages specific
   * to this pipeline. It is instantiated during pipeline startup.
   */
  protected ILogger PipeLog       = null;

  // The symbolic name is used in the management of the pipeline (control and
  // thread monitoring) and logging.
  private String symbolicName;

  // Tells the pipeline to stop when instructed
  private boolean stopRequested = false;
  private boolean stop = false;

  // true if an abort happened
  private boolean aborted = false;

  // Active tells the pipe if it can process records
  private boolean active;

  // Used to reset the active status after sync processing
  private boolean syncStatusActive;

  // To make sure that we do not leave transactions half finished,
  // The active flag is updated with the ActiveStateRequested when a transaction
  // finishes
  private boolean activeStateRequested;

  // The number of times that the pipeline should run, default to forever
  private int runCount = 0;

  // Concrete classes for the following member attributes are
  // loaded from a property file and instantiated via reflection.
  private ExceptionHandler handler = new ExceptionHandler();

  // And the batch input adapter for this pipe
  private IInputAdapter batchInputAdapter;

  // And the real time input adapter for this pipe
  private IRTAdapter RTAdapter;

  // tree of all threads that have been launched for processing plugins. Each
  // plug in has a sub-node containing a ThreadGroup for itself.
  private ThreadGroup pluginRoot;

  // tree of all threads that have been launched for output adapters.
  private ThreadGroup OutputAdapterRoot;

  // tree of all threads that have been launched for output adapters.
  private ThreadGroup RTAdapterRoot;

  // list of plug ins to be run by this pipeline
  private ArrayList<IPlugIn> plugInList = new ArrayList<>();

  // list of batch output adapters to be run by this pipeline
  private ArrayList<IOutputAdapter> batchOutputAdapterList = new ArrayList<>();

  // list of plug ins threads to be run by this pipeline (there can be more than
  // one thread per logical plug in)
  private ArrayList<ThreadGroup> thGrpsPlugIn = new ArrayList<>();

  // These are used for configuring the pipe
  private int  SleepTime;

  // List of Services that this Client supports
  private final String SERVICE_PIPELINE_ACTIVE = CommonConfig.ACTIVE;
  private final String SERVICE_PIPELINE_SLEEP  = "Sleep";
  private final String SERVICE_RUNCOUNT        = "RunCount";
  private final String SERVICE_HALT_ON_EXCP    = "HaltOnException";
  private final String SERVICE_BUFFER_STATUS   = "BufferStatus";

  // If we encounter an unhadled processing exception, this says if we stop
  private boolean HaltOnException = true;

  // Does this pipe need to perform an action that needs a sync
  private int localSyncStatus = ISyncPoint.SYNC_STATUS_NORMAL_RUN;

  // reference to the transaction manager for this pipe
  private TransactionManager TM;

  // The scheduler is responsible for managing the high frequency polling when
  // we appear to have some work to do. This holds the date that we are due to
  // go back to the idle schedule. Each time the pipe receives something to do,
  // we push the scheduler forward. This means we stay in the "active" schedule
  // long enough to roll from one file to the next at high speed.
  private long schedulerHighSpeed = 0;

  // Used to map the buffers in order that we can interrogate them
  ArrayList<IBuffer> bufferList = new ArrayList<>();

  // this tells us if this is a batch pipe - used for management
  private boolean batchPipeline;

 /**
  * Constructor
  */
  public Pipeline()
  {
    super();
  }

 /**
  * Init method will be called prior to run to allow the execution model to
  * initialize itself & acquire any necessary resources.
  *
  * @param Name - the name of the pipeline
  * @throws InitializationException
  */
  @Override
  public void init(String Name) throws InitializationException
  {
    // Used to manage the transaction manager MaxTransactions
    String maxTransactions;

    // the max transactions this pipe can use as an integer
    int    maxTransTM;

    // Set the name of this pipeline
    setSymbolicName(Name);

    // Create & configure the pipeline components
    ConfigurePipeline();

    // Register with the client manager
    registerClientManager();

    // Get the reference to the transaction manager for this pipe
    TM = TransactionManagerFactory.getTransactionManager(getSymbolicName());

    // set the default max transactions state of the pipeline
    maxTransactions = PropertyUtils.getPropertyUtils().getPropertyValueDef("PipelineList."+symbolicName+".MaxTransactions",
                                                         "1");
    try
    {
      maxTransTM = Integer.parseInt(maxTransactions);
    }
    catch (NumberFormatException nfe)
    {
      String Message = "MaxTransactions must be a numeric value, but we got <" + maxTransactions + "> in pipeline <" + symbolicName + ">. Aborting.";
      throw new InitializationException(Message);
    }

    // Set the max transactions
      TM.setMaxTransactions(maxTransTM);

    // set up the logger
    PipeLog = LogUtil.getLogUtil().getLogger(Name);
  }

  /**
   * Creates the actual pipeline implementation from the configuration. This
   * creates and instantiates the InputAdapter, the pipeline modules and the
   * output adapters, and links the modules together with the appropriate
   * buffers.
   *
   * @param props - the properties we have inherited
   * @throws InitializationException
   */
  private void ConfigurePipeline() throws InitializationException
  {
    // this controls the type (batch or realtime) we create
    String pipelineType;

    // used in setting up the pipe
    String strActiveState;
    String strHaltOnExcp;

    // Initialise the default polling sleep time
    SleepTime = 5000;

    try
    {
      // set the default active state of the pipeline
      strActiveState = PropertyUtils.getPropertyUtils().getPropertyValueDef("PipelineList."+symbolicName+"."+SERVICE_PIPELINE_ACTIVE,
                                                          "true");

      // set the default active state of the pipeline
      pipelineType = PropertyUtils.getPropertyUtils().getPropertyValueDef("PipelineList."+symbolicName+".PipelineType",
                                                          "Batch");

      // set the default active state of the pipeline
      strHaltOnExcp = PropertyUtils.getPropertyUtils().getPropertyValueDef("PipelineList."+symbolicName+"."+SERVICE_HALT_ON_EXCP,
                                                          "True");

      // Get the transaction controller configuration
      // Validate what we got for the pipe type
      if (pipelineType.equalsIgnoreCase("Batch"))
      {
        pipelineType = "Batch";
        batchPipeline = true;
      }
      else if (pipelineType.equalsIgnoreCase("RealTime"))
      {
        pipelineType = "RealTime";
        batchPipeline = false;
      }
      else
      {
        String Message = "Pipeline Type must be either Batch or RealTime, but we got <" + pipelineType + "> in pipeline <" + symbolicName + ">. Aborting.";
        throw new InitializationException(Message);
      }

      FWLog.info("*** Constructing " +pipelineType + " pipeline <" + symbolicName + "> ***");

      // Set the pipeline state with the value we have found from the
      // registry, otherwise it is true
      active = strActiveState.equalsIgnoreCase("true");
      activeStateRequested = active;

      if (!active)
      {
        FWLog.warning("Starting pipeline <" + symbolicName + "> in inactive state");
      }

      // set the halt on exception state
      HaltOnException = strHaltOnExcp.equalsIgnoreCase("true");

      // Construct the pipeline according to the batch model
      if (batchPipeline)
      {
        // Get the initialised batch input adapter
        batchInputAdapter = getBatchInputAdapter(handler);

        // create and initalise the processing body of the pipe
        plugInList = getProcessPlugins(handler);

        // create the batch output adapter list
        batchOutputAdapterList = getBatchOutputAdapterList(handler);

        // Hookup the buffers through the chain
        hookupBuffers(getBufferClass());
      }
      else
      // Construct the pipeline according to the real time model
      {
        // Get the real time input adapter
        RTAdapter = getRTAdapter(handler);

        // create and initalise the processing body of the pipe
        plugInList = getProcessPlugins(handler);

        // Set the exception handlers
        RTAdapter.setExceptionHandler(handler);

        // Set up the RT processing chain - this injects the plugin list into the
        // adapter so it can be used for processing
        RTAdapter.setProcessingList(plugInList);
      }
    }
    catch (InitializationException ie)
    {
      // this will already be handled as we want, just pass it up
      throw ie;
    }
    catch (Exception ex)
    {
      // Unexpected exception. Wrap it and pass it up, nesting the original message
      String Message = "Unexpected exception configuring pipeline <" + getSymbolicName() + ">, message <" + ex.getMessage() + ">";
      throw new InitializationException(Message, ex);
    }
    catch (Throwable ex)
    {
      // Unexpected exception. Wrap it and pass it up, nesting the original message
      String Message = "Unexpected exception configuring pipeline <" + getSymbolicName() + ">, message <" + ex.getMessage() + ">";
      throw new InitializationException(Message, ex);
    }
  }

 /**
  * Create the FIFO buffer class for batch pipelines
  *
  * @return The buffer class
  * @throws InitializationException
  */
  private Class<?> getBufferClass() throws InitializationException
  {
    // These variables are used for recovering the name of the buffer class
    // The buffer class we have here serves as a master for all of the rest of
    // the pipeline, and is cloned during pipeline setup
    String   defaultBuffer = null;
    Class<?> BufferClass = null;

    // ------------------- Set up the FIFO buffer class ------------------------
    try
    {
      // get the buffer class
      defaultBuffer = PropertyUtils.getPropertyUtils().getPipelinePropertyValueDef(symbolicName,
                                                                "Configuration",
                                                                CommonConfig.BUFFER_TYPE,
                                                                CommonConfig.DEFAULT_BUFFER_TYPE);
      BufferClass = Class.forName(defaultBuffer);
    }
    catch (ClassNotFoundException cnfe)
    {
      String Message = "Error finding buffer class <" + defaultBuffer + "> in pipeline <" + symbolicName + ">";
      throw new InitializationException(Message);
    }

    return BufferClass;
  }

 /**
  * Get and initialise the batch input adapter for this pipeline
  *
  * @return The initialised batch input adapter
  * @exception InitializationException
  */
  private IInputAdapter getBatchInputAdapter(ExceptionHandler Handler)
          throws InitializationException
  {
    Class<?> PluginClass = null;
    String   PluginClassName;
    String   PluginName;
    ArrayList<String> PluginNameList;

    // ---------------- Create the correct batch input adapter -----------------
    PluginNameList = PropertyUtils.getPropertyUtils().getGenericNameList(symbolicName+".InputAdapter");

    // Check that we have the right number of input adapters (1)
    if (PluginNameList.size() != 1)
    {
      String Message = "Expecting 1 Batch Input Adapter class for pipeline <" +
                        symbolicName + ">. Found <" + PluginNameList.size() + ">";
      throw new InitializationException(Message);
    }

    // Get the name
    PluginName = PluginNameList.get(0);
    if (PluginName == null)
    {
      String Message = "No Batch input adapter found";
      FWLog.error(Message);
      throw new InitializationException(Message);
    }
    else
    {
      FWLog.debug("Batch input adapter <" + PluginName + ">");

      // Get the class name
      PluginClassName = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(symbolicName, PluginName, "ClassName");

      // we found a batch input adapter - check it and instantiate it
      try
      {
        PluginClass = Class.forName(PluginClassName);
      }
      catch (ClassNotFoundException | NoClassDefFoundError ex)
      {
        String Message = "Input adapter class <" + PluginClassName +
                         "> not found for pipeline <" + symbolicName + ">. <"
                         + ex.getMessage() + ">";
        throw new InitializationException(Message);
      }

      try
      {
        batchInputAdapter = (IInputAdapter)PluginClass.newInstance();
      }
      catch (InstantiationException ex)
      {
        String Message = "Input adapter class  <" + PluginClassName +
                         "> instantiation error in pipeline <" + symbolicName +
                         ">. <" + ex.getMessage() + ">";
        throw new InitializationException(Message);
      }
      catch (IllegalAccessException ex)
      {
        String Message = "Input adapter class  <" + PluginClassName +
                         "> access error in pipeline <" + symbolicName + ">. <" +
                         ex.getMessage() + ">";
        throw new InitializationException(Message);
      }

      // Now that we have the input adapter, initialise it using the index 0 (we
      // have only one input adapter)
      batchInputAdapter.init(symbolicName, PluginName);

      // set the exception handler
      batchInputAdapter.setExceptionHandler(Handler);

      // link the batch input adaptor to us, so it can manage the scheduler
      batchInputAdapter.setPipeline(this);
    }

    return batchInputAdapter;
  }

 /**
  * Get and initialise the adapter for real time pipelines
  *
  * @return The initialised RT adapter
  * @throws InitializationException
  */
  private IRTAdapter getRTAdapter(ExceptionHandler Handler)
          throws InitializationException
  {
    String PluginName;
    ArrayList<String> PluginNameList;
    Class<?>          PluginClass = null;
    String            PluginClassName;

    // ----------------- Create the correct real time adapter ------------------
    PluginNameList = PropertyUtils.getPropertyUtils().getGenericNameList(symbolicName+".RTAdapter");

    // Check that we have the right number of input adapters (1)
    if (PluginNameList.size() != 1)
    {
      String Message = "Expecting 1 RT adapter class for pipeline <" +
                        symbolicName + ">. Found <" + PluginNameList.size() + ".";
      throw new InitializationException(Message);
    }

    PluginName = PluginNameList.get(0);
    if (PluginName == null)
    {
      String Message = "No Real Time input adapter found";
      FWLog.debug(Message);
      throw new InitializationException(Message);
    }
    else
    {
      FWLog.debug("Real time input adapter <" + PluginName + ">");

      // Get the class name
      PluginClassName = PropertyUtils.getPropertyUtils().getRTAdapterPropertyValue(symbolicName, PluginName, "ClassName");

      try
      {
        PluginClass = Class.forName(PluginClassName);
      }

      catch (ClassNotFoundException ex)
      {
        String Message = "Input adapter class <" + PluginClassName +
                         "> not found for pipeline <" + symbolicName + ">";
        throw new InitializationException(Message);
      }

      try
      {
        RTAdapter = (IRTAdapter) PluginClass.newInstance();
      }
      catch (InstantiationException ex)
      {
        String Message = "Input adapter class  <" + PluginClassName +
                         "> instantiation error in pipeline <" +
                         symbolicName + ">. <" + ex.getMessage() + ">";
        throw new InitializationException(Message);
      }
      catch (IllegalAccessException ex)
      {
        String Message = "Input adapter class  <" + PluginClassName +
                         "> access error in pipeline <" + symbolicName + ">. <" +
                         ex.getMessage() + ">";
        throw new InitializationException(Message);
      }

      // Now that we have the input adapter, initialise it using the index 0 (we
      // have only one input adapter)
      RTAdapter.init(symbolicName, PluginName);
      RTAdapter.setExceptionHandler(Handler);
    }

    return RTAdapter;
  }

 /**
  * Get and initialise the batch output adapters for this pipeline
  *
  * @param Handler The Exception handler we are going to link to
  * @return The output adapter list
  * @throws InitializationException
  */
  private ArrayList<IOutputAdapter> getBatchOutputAdapterList(ExceptionHandler Handler)
          throws InitializationException
  {
    String PluginName;
    ArrayList<String> PluginNameList;
    Iterator<String> PluginIter;
    int      Index;
    Class<?> PluginClass;
    String   PluginClassName = null;
    IOutputAdapter tmpBatchOutputAdapter;

    // ---------------- Create the batch output adapter chain ------------------
    PluginNameList = PropertyUtils.getPropertyUtils().getGenericNameList(symbolicName+".OutputAdapter");
    if (PluginNameList.isEmpty())
    {
      String Message = "No Output adapter found for pipeline <" + symbolicName + ">";
      throw new InitializationException(Message);
    }
    else
    {
      try
      {
        // Create the output adapters
        PluginIter = PluginNameList.iterator();

        Index = 0;
        while (PluginIter.hasNext())
        {
          PluginName = PluginIter.next();

          // Now create the output adapter chain - get the adapter class
          PluginClassName = PropertyUtils.getPropertyUtils().getBatchOutputAdapterPropertyValue(symbolicName, PluginName, "ClassName");

          FWLog.debug("OutputAdapter " + Index + " = <" + PluginClassName  + ">");

          PluginClass = Class.forName(PluginClassName);

          // Find what type of output adapter
          tmpBatchOutputAdapter = (IOutputAdapter)PluginClass.newInstance();

          // set the terminator tag on the last adapter, which has the effect of
          // logging records as errors if they have not been consumed by the
          // output adapter chain.
          if (Index == PluginNameList.size()-1)
          {
            tmpBatchOutputAdapter.setTerminator(true);
          }

          // Initialise the output adapter. Note that we do not link the output to
          // anything, which means that until we build the buffers, all adapters are
          // set to sink unconsumed errors
          tmpBatchOutputAdapter.init(symbolicName, PluginName);
          batchOutputAdapterList.add(tmpBatchOutputAdapter);
          tmpBatchOutputAdapter.setExceptionHandler(Handler);
          Index++;
        }
      }
      catch (ClassNotFoundException cnfe)
      {
        String Message = "Error finding plugin class <" + PluginClassName + ">";
        throw new InitializationException(Message);
      }
      catch (ClassCastException cce)
      {
        String Message = "Error creating plugin class (cast exception) <" + PluginClassName + ">";
        throw new InitializationException(Message);
      }
      catch (InstantiationException ie)
      {
        String Message = "Error instantiating plugin class <" + PluginClassName + ">";
        throw new InitializationException(Message);
      }
      catch (IllegalAccessException iae)
      {
        String Message = "Error accessing plugin class <" + PluginClassName + ">";
        throw new InitializationException(Message);
      }
    }

    return batchOutputAdapterList;
  }

 /**

  * Get and initialise the processing plug ins
  *
  * @param Handler The Exception handler we are going to link to
  * @return The processing plug in list
  * @throws InitializationException
  */
  private ArrayList<IPlugIn> getProcessPlugins(ExceptionHandler Handler)
          throws InitializationException
  {
    // This is the processing plug in we are adding
    IPlugIn Plugin;

    ArrayList<String> PluginNameList;
    Iterator<String> PluginIter;
    String   PluginName;
    Class<?> PluginClass;
    String   PluginClassName = null;
    int      Index;

    // ------------------------- Build the pipeline ----------------------------
    // Add the plugins to the pipeline
    // Processing plug ins will be added to the pipeline in numeric order,
    // until a number is missing. The numbering starts at 0.
    try
    {
      PluginNameList = PropertyUtils.getPropertyUtils().getGenericNameList(symbolicName + ".Process");
      // Now create the output adapter chain
      PluginIter = PluginNameList.iterator();
      Index = 0;

      while (PluginIter.hasNext())
      {
        PluginName = PluginIter.next();

        // Get the process class for the Plugin
        PluginClassName = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(symbolicName, PluginName, "ClassName", "None");

        FWLog.debug("Process "+ Index +" = " + PluginClassName);

        PluginClass = Class.forName(PluginClassName);
        Plugin = (IPlugIn)PluginClass.newInstance();

        // Create the new Plugin
        Plugin.init(symbolicName,PluginName);
        Plugin.setExceptionHandler(Handler);
        plugInList.add(Plugin);

        Index++;
      }
    }
    catch (ClassNotFoundException cnfe)
    {
      String Message = "Error finding plugin class <" + PluginClassName + ">";
      throw new InitializationException(Message);
    }
    catch (ClassCastException cce)
    {
      String Message = "Error creating plugin class (cast exception) <" + PluginClassName + ">";
      throw new InitializationException(Message,cce);
    }
    catch (InstantiationException ie)
    {
      String Message = "Error instantiating plugin class <" + PluginClassName + ">";
      throw new InitializationException(Message,ie);
    }
    catch (IllegalAccessException iae)
    {
      String Message = "Error accessing plugin class <" + PluginClassName + ">";
      throw new InitializationException(Message,iae);
    }

    return plugInList;
  }

 /**
  * Hook up the buffers between the plug ins for batch mode
  *
  * @param BufferClass The FIFO buffer class we are using for batch pipes
  * @throws InitializationException
  */
  private void hookupBuffers(Class<?> BufferClass) throws InitializationException
  {
    IBuffer tmpBuffer;
    IOutputAdapter tmpBatchOutputAdapter;
    IPlugIn tmpPlugIn;
    int Index;

    // ------------------------- Hookup buffers ----------------------------
    // Now that we have the input and the output, link them with a buffer
    // We will insert plugins, reconnecting the buffers as necessary later, but
    // this method means that even empty pipelines (not that you'll ever need
    // one) work
    try
    {
      // Hookup the input buffers - there can only be one input adapter of each
      // type (realtime / batch) at the moment, so we can create these
      // statically
      tmpBuffer = (IBuffer)BufferClass.newInstance();
      bufferList.add(tmpBuffer);

      batchInputAdapter.setBatchOutboundValidBuffer(tmpBuffer);
      tmpBuffer.setSupplier(batchInputAdapter.getSymbolicName());

      // Now we hookup the output to the appropriate place (output if the
      // pipe is empty, otherwise the first processing class
      if (plugInList.isEmpty() )
      {
        // hook straight up to the output adapter chain
        tmpBatchOutputAdapter = batchOutputAdapterList.get(0);
        tmpBatchOutputAdapter.setBatchInboundValidBuffer(tmpBuffer);
        tmpBuffer.setConsumer(tmpBatchOutputAdapter.getSymbolicName());
      }
      else
      {
        // Hookup to the first processing Plugin, and then build the rest of the
        // pipeline chain
        tmpPlugIn = plugInList.get(0);
        tmpPlugIn.setInbound(tmpBuffer);
        tmpBuffer.setConsumer(tmpPlugIn.getSymbolicName());

        for (Index = 1; Index < plugInList.size(); Index++)
        {
          // create a new buffer
          tmpBuffer = (IBuffer)BufferClass.newInstance();
          bufferList.add(tmpBuffer);

          // hook the buffer up to the next processing module for batch
          tmpPlugIn = plugInList.get(Index - 1);
          tmpPlugIn.setOutbound(tmpBuffer);
          tmpBuffer.setSupplier(tmpPlugIn.getSymbolicName());
          tmpPlugIn = plugInList.get(Index);
          tmpPlugIn.setInbound(tmpBuffer);
          tmpBuffer.setConsumer(tmpPlugIn.getSymbolicName());
        }

        // Last processing module, hook it up to the first output adapter
        tmpBuffer = (IBuffer)BufferClass.newInstance();
        bufferList.add(tmpBuffer);

        // Last processing module, hook it up to the first output adapter
        tmpBuffer = (IBuffer)BufferClass.newInstance();
        tmpPlugIn = plugInList.get(plugInList.size() - 1);
        tmpPlugIn.setOutbound(tmpBuffer);
        tmpBuffer.setSupplier(tmpPlugIn.getSymbolicName());

        // if we have an RT adapter, hook it up onto the beginning of the output chain
        if (RTAdapter != null)
        {
          // if we have a batch output adapter, create a new buffer and link it
          if (batchOutputAdapterList.size() > 0)
          {
            throw new InitializationException ("Output adapter defined in an RT pipe");
          }
        }

        // if we only have a RT adapter, skip the batch output adpater,
        // otherwise hook up the first output adapter in the list to the
        // batch output of the pipe
        if (batchOutputAdapterList.size() > 0)
        {
          tmpBatchOutputAdapter = batchOutputAdapterList.get(0);
          tmpBatchOutputAdapter.setBatchInboundValidBuffer(tmpBuffer);
          tmpBuffer.setConsumer(tmpBatchOutputAdapter.getSymbolicName());
        }
      }

      // Now create the output adapter chain, using the same logic - we already
      // know that there is at least one output adapter, we checked it, so just
      // do the rest of the chain.
      for (Index = 1; Index < batchOutputAdapterList.size(); Index++)
      {
        // create a new buffer for the valid and error records
        tmpBuffer = (IBuffer)BufferClass.newInstance();
        bufferList.add(tmpBuffer);

        // hook the valid and error buffers up to the next processing module
        tmpBatchOutputAdapter = batchOutputAdapterList.get(Index - 1);
        tmpBatchOutputAdapter.setBatchOutboundValidBuffer(tmpBuffer);
        tmpBuffer.setSupplier(tmpBatchOutputAdapter.getSymbolicName());

        tmpBatchOutputAdapter = batchOutputAdapterList.get(Index);
        tmpBatchOutputAdapter.setBatchInboundValidBuffer(tmpBuffer);
        tmpBuffer.setConsumer(tmpBatchOutputAdapter.getSymbolicName());
      }
    }
    catch (InstantiationException ie)
    {
      String Message = "Error instantiating buffer class in pipeline <" +
                       symbolicName + ">. <" + ie.getMessage() + ">";
      throw new InitializationException(Message);
    }
    catch (IllegalAccessException iae)
    {
      String Message = "Error accessing buffer class in pipeline <" +
                       symbolicName + ">. <" + iae.getMessage() + ">";
      throw new InitializationException(Message);
    }
  }

  /**
   * Run the Pipeline. This section performs the scheduling function of the
   * pipeline. In the case that there are records in processing, the sleep time
   * is ignored, and the processing loop runs at full speed. When there are no
   * more records to be processed, the more tranquil processing loop with the
   * additional sleep is performed.
   *
   * Note that to provide maximum performance, transactions are streamed
   * immediately one after the other, using the "streaming counter", which has
   * the effect of keeping the fast loop going for a defined number of cycles
   * even after no more records are found. The effect of holding the fast cycle
   * open for a few cycles means that we can close one transaction and open a
   * new one without ever returning to the slow cycles.
   *
   * Additionally, the active state of the pipeline is read and managed in this
   * section. A pipeline can only change state when we are not processing. To
   * enforce this, we manage the "Active" and "ActiveStateRequested" variables.
   */
  @Override
  public void run()
  {
    // The sleep time for this run
    long tmpSleepTime;

    // The number of records we processed in the input adapter
    long recordsReceived = 0;

    // The number of records in the pipe
    long recordsInPipe;

    try
    {
      startPipeline();

      // **** Manage the main processing loop ****
      while (!stop)
      {
        // perform the pipeline processing if the pipe is active
        if (active)
        {
          // retrieve input records, if there are any that need doing. We only
          // do this if there is a batch input adapter that is set
          if (batchInputAdapter != null)
          {
            recordsReceived = batchInputAdapter.push(batchInputAdapter.getBatchOutboundValidBuffer());
          }
        }

        // perform pipeline maintenance - check for transaction completion
        // buffer problems etc
        recordsInPipe = checkPipeline();

        // See if there are still records in the pipe
        if (getTransactionOpen((recordsReceived+recordsInPipe)>0) | (localSyncStatus > 0))
        {
          // This is the number of cycles we continue to use the fast scheduling
          // for, after there is no more real work to do.
          setSchedulerHigh();
        }

        // Get the sleep time for the pipe
        if (getSchedulerHigh())
        {
          tmpSleepTime = 100;
        }
        else
        {
          tmpSleepTime = SleepTime;
        }

        // **** Manage pipeline state changes ****
        if (TM == null)
        {
          // we are running in a non transactional pipe
          active = activeStateRequested;
          stop = stopRequested;
        }
        else
        {
          // transactional pipeline:
          // set the active state of the pipeline if there is a pending change
          if (activeStateRequested != active)
          {
            // we can set the state to false once the current transaction
            // has completed
            if ((activeStateRequested == false) & (TM.getActiveTransactionCount() == 0))
            {
              active = activeStateRequested;
              FWLog.info("Pipeline <" + symbolicName + "> inactive");
            }

            // We can set the active state to true if the transaction manager
            // tells us that we are allowed to start new transactions
            if ((activeStateRequested == true) & (TM.getNewTransactionAllowed()))
            {
              active = activeStateRequested;
              FWLog.info("Pipeline <" + symbolicName + "> active");
            }
          }

          // If a stop has been requested, action it at the end of the transaction
          if ((stopRequested) & (TM.getActiveTransactionCount() == 0))
          {
            stop = true;
          }
        }

        // This handles the sync processing - finishing the current transaction
        if (localSyncStatus == 2)
        {
          // if we have become inactive, move the sync status on
          if (!active)
          {
            localSyncStatus = 3;
          }
        }

        // This handles the sync processing - processing event backlog
        if (localSyncStatus == 4)
        {
          // currently just do it, in the future:
          // ToDo: buffer events that we cannot handle directly for sync
          // processing
          localSyncStatus = 5;
        }

        // **** Manage pipeline scheduling (loop timeouts) ****
        // This is the pipeline idle loop
        if (tmpSleepTime > 0)
        {
          try
          {
            FWLog.debug(
                  "Pipeline <" + symbolicName + "> will sleep for " +
                  tmpSleepTime + " ms.");
            Thread.sleep(tmpSleepTime);
          }
          catch (InterruptedException e)
          {
            // ignore the exception
          }
        }

        // Update the runcount. We use this to stop the pipeline after a
        // Certain number of runs
        if (runCount > 0)
        {
          // If we are idle, decrement the runcount
          if (recordsReceived == 0)
          {
            runCount--;
          }

          if (runCount == 0)
          {
            FWLog.info(
                  "RunCount reached, setting pipe <" + symbolicName +
                  "> inactive");
            active = false;
          }
        } // if Runcount
      } // while !Stop

      // shutdown the pipeline
      stopPipeline();
    }
    catch (ProcessingException pe)
    {
      FWLog.error("ProcessingException thrown.", pe);
    }
  }

 /**
  * Perform any cleanup required. This allows the IPipeline to keep resources
  * open after the run() method in case a multi-call model is used to
  * execute run() repeatedly.
  */
  @Override
  public void cleanup()
  {
    // nop
  }

  /**
   * stop the process. This can be called to safely shutdown a process that
   * is either long running or runs continuously. It's not intended to be a
   * drastic shutdown, but rather a "find a reasonable spot to stop & do so"
   * type message. A few minutes of processing before reaching such a point
   * is, while not recommended, not unreasonable in certain cases. The
   * process should make an effort to shutdown as quickly as is reasonable
   * without leaving the application in an invalid state.
   */
  @Override
  public void stop()
  {
    // Only notify once
    if (stopRequested == false)
    {
      FWLog.warning(
            "Pipeline <" + symbolicName +
            "> received Stop Command. Will exit after the current Transaction");

      // Stop new transactions being opened
      TM.setNewTransactionAllowed(false);

      // Ask the pipeline to shut down at the first reasonable opportunity
      stopRequested = true;
    }

    // set the scheduler to make sure we purge out anything in progress as quickly as possible
    setSchedulerHigh();
  }

 /**
  * This function is used to set the active state of the pipeline. An inactive
  * pipeline can be set active again immediately, but cannot processes any
  * records
  *
  * @param NewState The new state for the active flag
  */
  public synchronized void setActive(boolean NewState)
  {
    if (TM == null)
    {
      activeStateRequested = NewState;
      FWLog.info("Pipeline <" + symbolicName + "> active state changed");
    }
    else
    {
      if (NewState)
      {
        activeStateRequested = true;
        TM.setNewTransactionAllowed(true);
        FWLog.info("Pipeline <" + symbolicName + "> scheduled to become active");
      }
      else
      {
        activeStateRequested = false;
        TM.setNewTransactionAllowed(false);
        FWLog.info("Pipeline <" + symbolicName +
              "> scheduled to become inactive after transaction completion");
      }
    }
  }

  /**
   * startPipeline() launches the pipeline threads, and they wait for records
   * to arrive pushed by the input adapter.
   *
   * @throws ProcessingException
   */
  protected void startPipeline() throws ProcessingException
  {
    ListIterator<IPlugIn> pluginIterator;
    IPlugIn      tmpPlugIn;
    ThreadGroup  tmpGrpPlugIn;
    IOutputAdapter tmpOutputAdapter;

    FWLog.debug("Pipeline <" + getSymbolicName() + "> starting...");

    if (isBatchPipeline())
    {
      if (plugInList.isEmpty())
      {
        FWLog.debug("no plugins, pipeline will pass records through.");
      }

      pluginIterator = plugInList.listIterator();
      pluginRoot = new ThreadGroup("PlugIns");

      // Don't use ThreadGroup.enumerate( ThreadGroup[] ) because there is no
      // guarantee that it will enforce the ordering of the thread groups.
      // They MUST be in creation order for the pipe to work as we expect.
      thGrpsPlugIn = new ArrayList<>();

      // for each PlugIn, launch a set of threads. These are created in a
      // thread group for each plug in, hierachically subordinate to the
      // thread group for the processing elements of the pipeline
      while (pluginIterator.hasNext())
      {
        tmpPlugIn = pluginIterator.next();

        // reset IPlugIn before launching. clears shutdown flag.
        tmpPlugIn.reset();

        // thread group name = PlugIn name
        tmpGrpPlugIn = new ThreadGroup(pluginRoot, tmpPlugIn.getSymbolicName());
        thGrpsPlugIn.add(tmpGrpPlugIn);

        int thread_count = (tmpPlugIn.numThreads() > 0)
                             ? tmpPlugIn.numThreads() : 1;

        for (int i = 0; i < thread_count; ++i)
        {
          Thread PlugInTh = new Thread(tmpGrpPlugIn, tmpPlugIn,
                                       tmpPlugIn.getSymbolicName() +
                                       ".Inst-" + Integer.toString(i));

          // We could use this to unblock pipe bottlenecks, but at the
          // moment we don't seem to need it
          //PlugInTh.setPriority( Thread.NORM_PRIORITY );
          PlugInTh.setDaemon(true); // for fatal error handling.
          PlugInTh.start();
        }
      }

      // Create the root thread group for the output adapters
      OutputAdapterRoot = new ThreadGroup("Output");

      // Launch the batch output adapters
      for (int i = 0; i < batchOutputAdapterList.size(); ++i)
      {
        tmpOutputAdapter = batchOutputAdapterList.get(i);

        // reset Adapter before launching. clears shutdown flag.
        tmpOutputAdapter.reset();

        Thread ThrOutAdapter = new Thread(OutputAdapterRoot, tmpOutputAdapter,
                                       tmpOutputAdapter.getSymbolicName() +
                                       ".Inst-" + Integer.toString(i));

        // We could use this to unblock pipe bottlenecks, but at the
        // moment we don't seem to need it
        //ThrOutAdapter.setPriority( Thread.NORM_PRIORITY );
        ThrOutAdapter.setDaemon(true); // for fatal error handling.
        ThrOutAdapter.start();
      }
    }
    else
    {
      // Create the thread group for the real time adapter
      RTAdapterRoot = new ThreadGroup("RT");

      // Launch the real time output adapter
      if (RTAdapter != null)
      {
        // reset Adapter before launching. clears shutdown flag.
        RTAdapter.reset();

        Thread ThrRTAdapter = new Thread(RTAdapterRoot, RTAdapter,
                                       RTAdapter.getSymbolicName() +
                                       ".Inst-RT");
        // We could use this to unblock pipe bottlenecks, but at the
        // moment we don't seem to need it
        //ThrOutAdapter.setPriority( Thread.NORM_PRIORITY );
        ThrRTAdapter.setDaemon(true); // for fatal error handling.
        ThrRTAdapter.start();
      }
    }

    FWLog.debug("Pipeline <" + getSymbolicName() + "> started.");
  }

 /**
  * Stop the processing in the pipeline as soon as all the records have
  * purged out of it
  *
  * @throws ProcessingException
  */
  protected void stopPipeline() throws ProcessingException
  {
    // Shut down processing plug in threads
    stopPlugIns();

    // and the output adapter threads
    stopOutputAdapters();
  }

 /**
  * Perform regular maintenance on the pipeline elements. Also checks the pipe
  * for exceptions, and in the case one is found, report it and shut down the
  * pipe
  *
  * @return The approximate number of records still in the pipe
  */
  protected int checkPipeline()
  {
    IOutputAdapter tmpOutputAdapter;
    IPlugIn      tmpPlugIn;
    ListIterator<IPlugIn> pluginIterator;
    ListIterator<ThreadGroup> threadGroupIterator;
    int          RecordsInPipe = 0;

    pluginIterator = plugInList.listIterator();
    threadGroupIterator = thGrpsPlugIn.listIterator();

    while (pluginIterator.hasNext() && threadGroupIterator.hasNext())
    {
      tmpPlugIn = pluginIterator.next();
      RecordsInPipe += tmpPlugIn.getOutboundRecordCount();
    }

    for (int i = 0; i < batchOutputAdapterList.size(); ++i)
    {
      tmpOutputAdapter = batchOutputAdapterList.get(i);

      // anything that needs to be done 1x per batch cycle.
      RecordsInPipe += tmpOutputAdapter.getOutboundRecordCount();
    }

    // check if there have been any errors in the threads, and if there
    // have, pass the exception up
    if (handler.hasError())
    {
      // Failure occurred, propogate the error
      System.err.println("Exception thrown in pipeline <" + getSymbolicName() + ">, see Error Log.");
      FWLog.error("Exception thrown in pipeline <" + getSymbolicName() + ">, see Error Log.");

      // report the exceptions to the ErrorLog
      Iterator<Exception> excList = handler.getExceptionList().iterator();

      while (excList.hasNext())
      {
        Exception tmpException = (Exception) excList.next();
        ErrorLog.error("Processing Exception caught.", tmpException);
        PipeLog.error(tmpException.getMessage());
      }

      // Clear down the list
      handler.clearExceptions();

      // See if we should shutdown
      if(HaltOnException)
      {
        // stop the pipe
        System.err.println("Exception thrown in pipeline <" + getSymbolicName() + ">, see Error Log.");
        FWLog.error("Pipeline <" + getSymbolicName() + "> configured to shut down on exception. Shutting down.");
        aborted = true;
      }
    }

    return RecordsInPipe;
  }

 /**
  * This method orders the output adapters to close down and waits until they
  * respect the command.
  *
  * @throws ProcessingException
  */
  protected void stopOutputAdapters() throws ProcessingException
  {
    IOutputAdapter tmpOutputAdapter;

    // Shut down the real time adapter if it is defined
    if (RTAdapter != null)
    {
      RTAdapter.markForClosedown();
    }

    // Shut down the output adapters
    for (int i = 0; i < batchOutputAdapterList.size(); ++i)
    {
      tmpOutputAdapter = batchOutputAdapterList.get(i);

      // Close down the output adapters
      // run the output adapter so that we can clear out any records
      tmpOutputAdapter.markForClosedown();
    }

    // Wait until all the output adapters have finished
    if (OutputAdapterRoot != null)
    {
      while (OutputAdapterRoot.activeCount() > 0) //&& (Handler.hasError() == false))
      {
        FWLog.debug("Waiting for output thread groups to finish.");

        try
        {
          // There are two possibilities here that we could use:
          //   Yield() will just let others take over
          //   Sleep(x) makes the PlugIn stop and wait for x ms.
          // I'm not really sure which has the best advantages, but I
          // tend towards a simple yield at the moment because we want
          // to avoid dead time
          //Thread.yield();
          Thread.sleep(100);
        }
        catch (InterruptedException ie)
        {
          FWLog.debug("Interrupted!");
        }
      }

      // Clean up the output adapter thread groups
      OutputAdapterRoot.destroy();
      OutputAdapterRoot = null;
    }

    if (batchOutputAdapterList != null)
    {
      // Shut down the output adapters
      for (int i = 0; i < batchOutputAdapterList.size(); ++i)
      {
        tmpOutputAdapter = batchOutputAdapterList.get(i);

        // anything that needs to be done 1x per batch cycle.
        tmpOutputAdapter.getOutboundRecordCount();

        // after all the batches are processed, do any work
        // that must happen once & only once per interface run.
        tmpOutputAdapter.close();
      }
    }
  }

  /**
   * Close down the plug in threads that have been launched in the pipeline
   *
   * @throws ProcessingException
   */
  protected void stopPlugIns() throws ProcessingException
  {
    IPlugIn      tmpPlugIn;
    ListIterator<IPlugIn> pluginIterator;
    ListIterator<ThreadGroup> threadGroupIterator;
    ThreadGroup  tmpGrpPlugIn;

    pluginIterator = plugInList.listIterator();
    threadGroupIterator = thGrpsPlugIn.listIterator();

    while (pluginIterator.hasNext() && threadGroupIterator.hasNext())
    {
      tmpPlugIn = pluginIterator.next();
      tmpPlugIn.markForClosedown();
      tmpGrpPlugIn = threadGroupIterator.next();

      // wait for all Threads in this group to shutdown.
      while (tmpGrpPlugIn.activeCount() > 0) //&& (Handler.hasError() == false))
      {
        FWLog.debug(
                "Waiting for plugin thread group <" + tmpGrpPlugIn.getName() +
                "> to finish.");

        try
        {
            // There are two possibilities here that we could use:
            //   Yield() will just let others take over
            //   Sleep(x) makes the PlugIn stop and wait for x ms.
            // I'm not really sure which has the best advantages, but I
            // tend towards a simple yield at the moment because we want
            // to avoid dead time
            //Thread.yield();
            Thread.sleep(100);
        }
        catch (InterruptedException ie)
        {
          FWLog.debug("Interrupted!");
        }
      }

      FWLog.debug("ThreadGroup <" + tmpGrpPlugIn.getName() + "> dead, next... ");

      // Destroy the group
      if (tmpGrpPlugIn.isDestroyed() == false)
      {
        tmpGrpPlugIn.destroy();
      }
    }

    // Clean up the Plugin thread groups
    if (pluginRoot != null)
    {
      pluginRoot.destroy();
      pluginRoot = null;
    }
  }

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
   *
   * @param name The new symbolic name to use
   */
  protected void setSymbolicName(String name)
  {
    symbolicName = name;
  }

  /**
  * Perform Plugin level shutdown processing. This is called in order to close
  * any data or resources that should be closed before the pipe is destroyed
  */
  @Override
  public void Shutdown()
  {
    // Nothing at present
  }

 /**
  * Set the pipeline input scheduler to run at high speed for a period of time
  */
  @Override
  public void setSchedulerHigh()
  {
    // Put the scheduled high speed time for 10 seconds
    schedulerHighSpeed = ConversionUtils.getConversionUtilsObject().getCurrentUTCms() + 10000;
  }

 /**
  * Tell us if the scheduler is still in the high speed period.
  *
  * @return true if we are in the high speed schedule
  */
  @Override
  public boolean getSchedulerHigh()
  {
    return (schedulerHighSpeed > ConversionUtils.getConversionUtilsObject().getCurrentUTCms());
  }

 /**
  * Abstration function for the possibility to run pipelines without a
  * Transaction Manager.
  *
  * @param PipeState The expected state
  * @return The actual state. Will equal the expected state if we are running
  *         without a transaction manager
  */
  public boolean getTransactionOpen(boolean PipeState)
  {
    if (TM == null)
    {
      return PipeState;
    }
    else
    {
      return (TM.getActiveTransactionCount() > 0);
    }
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /* processControlEvent is the method that will be called when an event
  * is received for a module that has registered itself as a client of the
  * External Control Interface
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int    ResultCode = -1;
    String logStr;

    if (Command.equalsIgnoreCase(SERVICE_PIPELINE_ACTIVE))
    {
      if (Parameter.equalsIgnoreCase("false"))
      {
        // Suspend pipeline processing
        setActive(false);
        ResultCode = 0;
      }

      if (Parameter.equalsIgnoreCase("true"))
      {
        // Start pipeline processing
        setActive(true);
        ResultCode = 0;
      }

      if (Parameter.equalsIgnoreCase(""))
      {
        // Get the current status
        String currentActiveState;

        if (active != activeStateRequested)
        {
          currentActiveState = Boolean.toString(active) + "*";
        }
        else
        {
          currentActiveState = Boolean.toString(active);
        }

        return currentActiveState;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_PIPELINE_SLEEP))
    {
      if (Parameter.equals(""))
      {
        // Get the current status
        return Integer.toString(SleepTime);
      }
      else
      {
        try
        {
          SleepTime = Integer.parseInt(Parameter);
        }
        catch (NumberFormatException nfe)
        {
          logStr = "Sleep paramter was not numeric. Passed value = <" +
                   Parameter + ">";
          FWLog.error(logStr);

          return logStr;
        }

        FWLog.info(
              "Sleep time set to <" + SleepTime + "> for pipeline <" +
              symbolicName + ">");
        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_RUNCOUNT))
    {
      if (Parameter.equals(""))
      {
        // Get the current status
        return Integer.toString(runCount);
      }
      else
      {
        try
        {
          runCount = Integer.parseInt(Parameter);

          // Also set the pipe to active if it is inactive
          if (!active)
          {
            setActive(true);
          }
        }
        catch (NumberFormatException nfe)
        {
          logStr = "RunCount paramter was not numeric. Passed value = <" +
                   Parameter + ">";
          FWLog.error(logStr);

          return logStr;
        }
      }

      ResultCode = 0;
    }

    if (Command.equalsIgnoreCase(SERVICE_HALT_ON_EXCP))
    {
      if (Parameter.equalsIgnoreCase("false"))
      {
        // Suspend pipeline processing
        HaltOnException = false;
        ResultCode = 0;
      }

      if (Parameter.equalsIgnoreCase("true"))
      {
        // Start pipeline processing
        HaltOnException = true;
        ResultCode = 0;
      }

      if (Parameter.equalsIgnoreCase(""))
      {
        // Get the current status
        return Boolean.toString(HaltOnException);
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_BUFFER_STATUS))
    {
      Iterator<IBuffer> bufferIter = bufferList.iterator();
      String responseString = "";
      IBuffer tmpBuffer;

      if (Parameter.equals(""))
      {
        while (bufferIter.hasNext())
        {
          tmpBuffer = bufferIter.next();
          responseString = responseString + tmpBuffer.getConsumer() + "=" + tmpBuffer.getEventCount() + ", ";
        }

        // Get the current status
        return responseString;
      }
    }

    logStr = "Command " + Command + " handled by OpenRateApplication";
    FWLog.debug(logStr);

    if (ResultCode == 0)
    {
      return "OK";
    }
    else
    {
      return "Error: Command not understood.";
    }
  }

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    //Register this Client to this pipeline
    ClientManager.registerClient(getSymbolicName(),getSymbolicName(), this);

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PIPELINE_ACTIVE, ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PIPELINE_SLEEP,  ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_RUNCOUNT,        ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_HALT_ON_EXCP,    ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_BUFFER_STATUS,   ClientManager.PARAM_DYNAMIC);
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of inherited ISyncPoint functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  */
  @Override
  public int getSyncStatus()
  {
    return localSyncStatus;
  }

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  */
  @Override
  public void setSyncStatus(int newStatus)
  {
    if ((newStatus == ISyncPoint.SYNC_STATUS_SYNC_REQUESTED) & (localSyncStatus != ISyncPoint.SYNC_STATUS_SYNC_REQUESTED))
    {
      // command the pipe to not accept any new transactions
      syncStatusActive = active;
      setActive(false);
    }

    if ((newStatus == ISyncPoint.SYNC_STATUS_NORMAL_RUN) & (localSyncStatus != ISyncPoint.SYNC_STATUS_NORMAL_RUN))
    {
      // command the pipe to accept new transactions
      // Ticket #568446 - reset active to previous status after sync
      setActive(syncStatusActive);
    }

    // Update the status
    localSyncStatus = newStatus;
  }

 /**
  * Returns true if the pipe is a batch pipeline
  *
  * @return true if the pipeline is a batch pipeline, false if it is real time
  */
  @Override
  public boolean isBatchPipeline()
  {
    return batchPipeline;
  }

 /**
  * Returns true if the pipeline aborted - used to cascade the abort to
  * stop the system in frameworks with multiple pipes
  *
  * @return true if the pipe aborted and the framework should stop
  */
  @Override
  public boolean isAborted()
  {
    return aborted;
  }
}
