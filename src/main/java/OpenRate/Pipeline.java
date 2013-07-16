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
   * Pipeline Level AstractLogger. This logger is used for logging messages specific
   * to this pipeline. It is instantiated during pipeline startup.
   */
  private ILogger pipeLog       = null;

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
  private ExceptionHandler pipeExceptionHandler = new ExceptionHandler();

  // And the batch input adapter for this pipe
  private IInputAdapter batchInputAdapter;

  // And the real time input adapter for this pipe
  private IRTAdapter rtAdapter;

  // tree of all threads that have been launched for processing plugins. Each
  // plug in has a sub-node containing a ThreadGroup for itself.
  private ThreadGroup pluginRoot;

  // tree of all threads that have been launched for output adapters.
  private ThreadGroup outputAdapterRoot;

  // tree of all threads that have been launched for output adapters.
  private ThreadGroup rtAdapterRoot;

  // list of plug ins to be run by this pipeline
  private ArrayList<IPlugIn> plugInList = new ArrayList<>();

  // list of batch output adapters to be run by this pipeline
  private ArrayList<IOutputAdapter> batchOutputAdapterList = new ArrayList<>();

  // list of plug ins threads to be run by this pipeline (there can be more than
  // one thread per logical plug in)
  private ArrayList<ThreadGroup> thGrpsPlugIn = new ArrayList<>();

  // These are used for configuring the pipe
  private int  sleepTime;

  // List of Services that this Client supports
  private final String SERVICE_PIPELINE_ACTIVE = CommonConfig.ACTIVE;
  private final String SERVICE_PIPELINE_SLEEP  = "Sleep";
  private final String SERVICE_RUNCOUNT        = "RunCount";
  private final String SERVICE_HALT_ON_EXCP    = "HaltOnException";
  private final String SERVICE_BUFFER_STATUS   = "BufferStatus";

  // If we encounter an unhadled processing exception, this says if we stop
  private boolean haltOnException = true;

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
  
  // Used to simplify logging and exception handling
  private String message;

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
      message = "MaxTransactions must be a numeric value, but we got <" + maxTransactions + "> in pipeline <" + symbolicName + ">. Aborting.";
      throw new InitializationException(message,getSymbolicName());
    }

    // Set the max transactions
      TM.setMaxTransactions(maxTransTM);

    // set up the logger
    setPipeLog(LogUtil.getLogUtil().getLogger(Name));
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
    sleepTime = 5000;
    
    // Get our logger
    setPipeLog(LogUtil.getLogUtil().getLogger(symbolicName));

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
        message = "Pipeline Type must be either Batch or RealTime, but we got <" + pipelineType + "> in pipeline <" + symbolicName + ">. Aborting.";
        throw new InitializationException(message,getSymbolicName());
      }

      OpenRate.getOpenRateFrameworkLog().info("*** Constructing " +pipelineType + " pipeline <" + symbolicName + "> ***");

      // Set the pipeline state with the value we have found from the
      // registry, otherwise it is true
      active = strActiveState.equalsIgnoreCase("true");
      activeStateRequested = active;

      if (!active)
      {
        OpenRate.getOpenRateFrameworkLog().warning("Starting pipeline <" + symbolicName + "> in inactive state");
      }

      // set the halt on exception state
      haltOnException = strHaltOnExcp.equalsIgnoreCase("true");

      // Construct the pipeline according to the batch model
      if (batchPipeline)
      {
        // Get the initialised batch input adapter
        batchInputAdapter = getBatchInputAdapter(pipeExceptionHandler);

        // create and initalise the processing body of the pipe
        plugInList = getProcessPlugins(pipeExceptionHandler);

        // create the batch output adapter list
        batchOutputAdapterList = getBatchOutputAdapterList(pipeExceptionHandler);

        // Hookup the buffers through the chain
        hookupBuffers(getBufferClass());
      }
      else
      // Construct the pipeline according to the real time model
      {
        // Get the real time input adapter
        rtAdapter = getRTAdapter(pipeExceptionHandler);

        // create and initalise the processing body of the pipe
        plugInList = getProcessPlugins(pipeExceptionHandler);

        // Set up the RT processing chain - this injects the plugin list into the
        // adapter so it can be used for processing
        rtAdapter.setProcessingList(plugInList);
      }
    }
    catch (InitializationException ex)
    {
      // this will already be handled as we want, just pass it up
      throw ex;
    }
    catch (Exception ex)
    {
      // Unexpected exception. Wrap it and pass it up, nesting the original message
      message = "Unexpected exception configuring pipeline <" + getSymbolicName() + ">, message <" + ex.getMessage() + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }
    catch (Throwable ex)
    {
      // Unexpected exception. Wrap it and pass it up, nesting the original message
      message = "Unexpected exception configuring pipeline <" + getSymbolicName() + ">, message <" + ex.getMessage() + ">";
      throw new InitializationException(message,ex,getSymbolicName());
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
    catch (ClassNotFoundException ex)
    {
      message = "Error finding buffer class <" + defaultBuffer + "> in pipeline <" + symbolicName + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }

    return BufferClass;
  }

 /**
  * Get and initialise the batch input adapter for this pipeline
  *
  * @return The initialised batch input adapter
  * @exception InitializationException
  */
  private IInputAdapter getBatchInputAdapter(ExceptionHandler pipeExceptionHandler)
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
      message = "Expecting 1 Batch Input Adapter class for pipeline <" +
                        symbolicName + ">. Found <" + PluginNameList.size() + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Get the name
    PluginName = PluginNameList.get(0);
    if (PluginName == null)
    {
      message = "No Batch input adapter found";
      OpenRate.getOpenRateFrameworkLog().error(message);
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      OpenRate.getOpenRateFrameworkLog().debug("Batch input adapter <" + PluginName + ">");

      // Get the class name
      PluginClassName = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValue(symbolicName, PluginName, "ClassName");

      // we found a batch input adapter - check it and instantiate it
      try
      {
        PluginClass = Class.forName(PluginClassName);
      }
      catch (ClassNotFoundException | NoClassDefFoundError ex)
      {
        message = "Input adapter class <" + PluginClassName +
                         "> not found for pipeline <" + symbolicName + ">. <"
                         + ex.getMessage() + ">";
        throw new InitializationException(message,getSymbolicName());
      }

      try
      {
        batchInputAdapter = (IInputAdapter)PluginClass.newInstance();
      }
      catch (InstantiationException ex)
      {
        message = "Input adapter class  <" + PluginClassName +
                         "> instantiation error in pipeline <" + symbolicName +
                         ">. <" + ex.getMessage() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      catch (IllegalAccessException ex)
      {
        message = "Input adapter class  <" + PluginClassName +
                         "> access error in pipeline <" + symbolicName + ">. <" +
                         ex.getMessage() + ">";
        throw new InitializationException(message,getSymbolicName());
      }

      // Now that we have the input adapter, initialise it using the index 0 (we
      // have only one input adapter)
      batchInputAdapter.init(symbolicName, PluginName);

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
  private IRTAdapter getRTAdapter(ExceptionHandler pipeExceptionHandler)
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
      message = "Expecting 1 RT adapter class for pipeline <" +
                        symbolicName + ">. Found <" + PluginNameList.size() + ".";
      throw new InitializationException(message,getSymbolicName());
    }

    PluginName = PluginNameList.get(0);
    if (PluginName == null)
    {
      message = "No Real Time input adapter found";
      OpenRate.getOpenRateFrameworkLog().debug(message);
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
     OpenRate.getOpenRateFrameworkLog().debug("Real time input adapter <" + PluginName + ">");

      // Get the class name
      PluginClassName = PropertyUtils.getPropertyUtils().getRTAdapterPropertyValue(symbolicName, PluginName, "ClassName");

      try
      {
        PluginClass = Class.forName(PluginClassName);
      }

      catch (ClassNotFoundException ex)
      {
        message = "Input adapter class <" + PluginClassName +
                         "> not found for pipeline <" + symbolicName + ">";
        throw new InitializationException(message,getSymbolicName());
      }

      try
      {
        rtAdapter = (IRTAdapter) PluginClass.newInstance();
      }
      catch (InstantiationException ex)
      {
        message = "Input adapter class  <" + PluginClassName +
                         "> instantiation error in pipeline <" +
                         symbolicName + ">. <" + ex.getMessage() + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      catch (IllegalAccessException ex)
      {
        message = "Input adapter class  <" + PluginClassName +
                         "> access error in pipeline <" + symbolicName + ">. <" +
                         ex.getMessage() + ">";
        throw new InitializationException(message,getSymbolicName());
      }

      // Now that we have the input adapter, initialise it using the index 0 (we
      // have only one input adapter)
      rtAdapter.init(symbolicName, PluginName);
    }

    return rtAdapter;
  }

 /**
  * Get and initialise the batch output adapters for this pipeline
  *
  * @param pipeExceptionHandler The Exception handler we are going to link to
  * @return The output adapter list
  * @throws InitializationException
  */
  private ArrayList<IOutputAdapter> getBatchOutputAdapterList(ExceptionHandler pipeExceptionHandler)
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
      message = "No Output adapter found for pipeline <" + symbolicName + ">";
      throw new InitializationException(message,getSymbolicName());
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

          OpenRate.getOpenRateFrameworkLog().debug("OutputAdapter " + Index + " = <" + PluginClassName  + ">");

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
          Index++;
        }
      }
      catch (ClassNotFoundException ex)
      {
        message = "Error finding plugin class <" + PluginClassName + "> in module <"+getSymbolicName()+">";
        throw new InitializationException(message,ex,getSymbolicName());
      }
      catch (ClassCastException ex)
      {
        message = "Error creating plugin class (cast exception) <" + PluginClassName + ">";
        throw new InitializationException(message,ex,getSymbolicName());
      }
      catch (InstantiationException ex)
      {
        message = "Error instantiating plugin class <" + PluginClassName + ">";
        throw new InitializationException(message,ex,getSymbolicName());
      }
      catch (IllegalAccessException ex)
      {
        message = "Error accessing plugin class <" + PluginClassName + ">";
        throw new InitializationException(message,ex,getSymbolicName());
      }
    }

    return batchOutputAdapterList;
  }

 /**

  * Get and initialise the processing plug ins
  *
  * @param pipeExceptionHandler The Exception handler we are going to link to
  * @return The processing plug in list
  * @throws InitializationException
  */
  private ArrayList<IPlugIn> getProcessPlugins(ExceptionHandler pipeExceptionHandler)
          throws InitializationException
  {
    // This is the processing plug in we are adding
    IPlugIn Plugin;

    ArrayList<String> PluginNameList;
    Iterator<String> PluginIter;
    String   PluginName;
    Class<?> PluginClass;
    String   pluginClassName = null;
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
        pluginClassName = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(symbolicName, PluginName, "ClassName", "None");

        if (pluginClassName.equals("None"))
        {
          message = "Could not find the ClassName definition for module <"+PluginName+"> in pipe <"+symbolicName+">";
          throw new InitializationException(message,getSymbolicName());
        }
        
        OpenRate.getOpenRateFrameworkLog().debug("Process "+ Index +" = " + pluginClassName);

        PluginClass = Class.forName(pluginClassName);
        Plugin = (IPlugIn)PluginClass.newInstance();

        // Create the new Plugin
        Plugin.init(symbolicName,PluginName);
        Plugin.setExceptionHandler(pipeExceptionHandler);
        plugInList.add(Plugin);

        Index++;
      }
    }
    catch (ClassNotFoundException ex)
    {
      message = "Error finding plugin class <" + pluginClassName + "> in module <"+getSymbolicName()+">";
      throw new InitializationException(message,ex,getSymbolicName());
    }
    catch (ClassCastException ex)
    {
      message = "Error creating plugin class (cast exception) <" + pluginClassName + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }
    catch (InstantiationException ex)
    {
      message = "Error instantiating plugin class <" + pluginClassName + ">";
      throw new InitializationException(message,ex,getSymbolicName());
    }
    catch (IllegalAccessException ex)
    {
      message = "Error accessing plugin class <" + pluginClassName + ">";
      throw new InitializationException(message,ex,getSymbolicName());
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
        if (rtAdapter != null)
        {
          // if we have a batch output adapter, create a new buffer and link it
          if (batchOutputAdapterList.size() > 0)
          {
            message = "Output adapter defined in an RT pipe";
            throw new InitializationException (message,getSymbolicName());
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
      message = "Error instantiating buffer class in pipeline <" +
                       symbolicName + ">. <" + ie.getMessage() + ">";
      throw new InitializationException(message,getSymbolicName());
    }
    catch (IllegalAccessException iae)
    {
      message = "Error accessing buffer class in pipeline <" +
                       symbolicName + ">. <" + iae.getMessage() + ">";
      throw new InitializationException(message,getSymbolicName());
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
          tmpSleepTime = sleepTime;
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
              OpenRate.getOpenRateFrameworkLog().info("Pipeline <" + symbolicName + "> inactive");
            }

            // We can set the active state to true if the transaction manager
            // tells us that we are allowed to start new transactions
            if ((activeStateRequested == true) & (TM.getNewTransactionAllowed()))
            {
              active = activeStateRequested;
              OpenRate.getOpenRateFrameworkLog().info("Pipeline <" + symbolicName + "> active");
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
            OpenRate.getOpenRateFrameworkLog().debug(
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
            OpenRate.getOpenRateFrameworkLog().info(
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
      OpenRate.getOpenRateFrameworkLog().error("ProcessingException thrown.", pe);
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
      OpenRate.getOpenRateFrameworkLog().warning(
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
      OpenRate.getOpenRateFrameworkLog().info("Pipeline <" + symbolicName + "> active state changed");
    }
    else
    {
      if (NewState)
      {
        activeStateRequested = true;
        TM.setNewTransactionAllowed(true);
        OpenRate.getOpenRateFrameworkLog().info("Pipeline <" + symbolicName + "> scheduled to become active");
      }
      else
      {
        activeStateRequested = false;
        TM.setNewTransactionAllowed(false);
        OpenRate.getOpenRateFrameworkLog().info("Pipeline <" + symbolicName +
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

    OpenRate.getOpenRateFrameworkLog().debug("Pipeline <" + getSymbolicName() + "> starting...");

    if (isBatchPipeline())
    {
      if (plugInList.isEmpty())
      {
        OpenRate.getOpenRateFrameworkLog().debug("no plugins, pipeline will pass records through.");
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
      outputAdapterRoot = new ThreadGroup("Output");

      // Launch the batch output adapters
      for (int i = 0; i < batchOutputAdapterList.size(); ++i)
      {
        tmpOutputAdapter = batchOutputAdapterList.get(i);

        // reset Adapter before launching. clears shutdown flag.
        tmpOutputAdapter.reset();

        Thread ThrOutAdapter = new Thread(outputAdapterRoot, tmpOutputAdapter,
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
      rtAdapterRoot = new ThreadGroup("RT");

      // Launch the real time output adapter
      if (rtAdapter != null)
      {
        // reset Adapter before launching. clears shutdown flag.
        rtAdapter.reset();

        Thread ThrRTAdapter = new Thread(rtAdapterRoot, rtAdapter,
                                       rtAdapter.getSymbolicName() +
                                       ".Inst-RT");
        // We could use this to unblock pipe bottlenecks, but at the
        // moment we don't seem to need it
        //ThrOutAdapter.setPriority( Thread.NORM_PRIORITY );
        ThrRTAdapter.setDaemon(true); // for fatal error handling.
        ThrRTAdapter.start();
      }
    }

    OpenRate.getOpenRateFrameworkLog().debug("Pipeline <" + getSymbolicName() + "> started.");
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
    if (pipeExceptionHandler.hasError())
    {
      // Failure occurred, propogate the error
      System.err.println("Exception thrown in pipeline <" + getSymbolicName() + ">, see Error Log.");
      OpenRate.getOpenRateFrameworkLog().error("Exception thrown in pipeline <" + getSymbolicName() + ">, see Error Log.");

      // report the exceptions to the ErrorLog
      Iterator<Exception> excList = pipeExceptionHandler.getExceptionList().iterator();

      // for each of the exceptions we have collected
      while (excList.hasNext())
      {
        Exception tmpException = (Exception) excList.next();
        OpenRate.getOpenRateErrorLog().error("Processing Exception caught.", tmpException);
        getPipeLog().error(tmpException.getMessage());
      }

      // Clear down the list
      pipeExceptionHandler.clearExceptions();

      // See if we should shutdown
      if(haltOnException)
      {
        // stop the pipe
        System.err.println("Exception thrown in pipeline <" + getSymbolicName() + ">, see Error Log.");
        OpenRate.getOpenRateFrameworkLog().error("Pipeline <" + getSymbolicName() + "> configured to shut down on exception. Shutting down.");
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
    if (rtAdapter != null)
    {
      rtAdapter.markForClosedown();
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
    if (outputAdapterRoot != null)
    {
      while (outputAdapterRoot.activeCount() > 0) //&& (Handler.hasError() == false))
      {
        OpenRate.getOpenRateFrameworkLog().debug("Waiting for output thread groups to finish.");

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
          OpenRate.getOpenRateFrameworkLog().debug("Interrupted!");
        }
      }

      // Clean up the output adapter thread groups
      outputAdapterRoot.destroy();
      outputAdapterRoot = null;
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
        OpenRate.getOpenRateFrameworkLog().debug(
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
          OpenRate.getOpenRateFrameworkLog().debug("Interrupted!");
        }
      }

      OpenRate.getOpenRateFrameworkLog().debug("ThreadGroup <" + tmpGrpPlugIn.getName() + "> dead, next... ");

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
          logStr = "Sleep paramter was not numeric. Passed value = <" +
                   Parameter + ">";
          OpenRate.getOpenRateFrameworkLog().error(logStr);

          return logStr;
        }

        OpenRate.getOpenRateFrameworkLog().info(
              "Sleep time set to <" + sleepTime + "> for pipeline <" +
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
          OpenRate.getOpenRateFrameworkLog().error(logStr);

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
        haltOnException = false;
        ResultCode = 0;
      }

      if (Parameter.equalsIgnoreCase("true"))
      {
        // Start pipeline processing
        haltOnException = true;
        ResultCode = 0;
      }

      if (Parameter.equalsIgnoreCase(""))
      {
        // Get the current status
        return Boolean.toString(haltOnException);
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
    OpenRate.getOpenRateFrameworkLog().debug(logStr);

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
    ClientManager.getClientManager().registerClient(getSymbolicName(),getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PIPELINE_ACTIVE, ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PIPELINE_SLEEP,  ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_RUNCOUNT,        ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_HALT_ON_EXCP,    ClientManager.PARAM_DYNAMIC);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BUFFER_STATUS,   ClientManager.PARAM_DYNAMIC);
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

  /**
   * @return the pipeLog
   */
  @Override
    public ILogger getPipeLog() {
        return pipeLog;
    }

    /**
     * @param pipeLog the pipeLog to set
     */
    public void setPipeLog(ILogger pipeLog) {
        this.pipeLog = pipeLog;
    }

 /**
  * Returns the pipeline exception handler.
  * 
  * @return The exception handler for the pipeline
  */
    @Override
    public ExceptionHandler getPipelineExceptionHandler() {
        return pipeExceptionHandler;
    }
}
