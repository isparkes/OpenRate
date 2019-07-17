package OpenRate.adapter;

import OpenRate.CommonConfig;
import OpenRate.IPipeline;
import OpenRate.OpenRate;
import OpenRate.buffer.IConsumer;
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

/**
 * The IInputAdapter is responsible for creating the work set that the pipeline
 * will execute on. Common implementations will load records from either a file
 * or from the database.
 */
public abstract class AbstractInputAdapter
        implements IInputAdapter,
        IEventInterface {

  // The symbolic name is used in the management of the pipeline (control and
  // thread monitoring) and logging.

  private String symbolicName;

  /**
   * This is the local variable that we use to determine the batch size. This
   * determines the number of records which is pushed into the output FIFO.
   */
  protected int batchSize;

  /**
   * This is the local variable that we use to determine the buffer high water
   * mark.
   */
  private int bufferSize;

  // This is the buffer we will be writing to
  private IConsumer consumer;

  // This is the pipeline that we are in, used for logging and property retrieval
  private IPipeline pipeline;

  // List of Services that this Client supports
  private final static String SERVICE_BATCHSIZE = CommonConfig.BATCH_SIZE;
  private final static String SERVICE_BUFFERSIZE = CommonConfig.BUFFER_SIZE;
  private final static String DEFAULT_BATCHSIZE = CommonConfig.DEFAULT_BATCH_SIZE;
  private final static String DEFAULT_BUFFERSIZE = CommonConfig.DEFAULT_BUFFER_SIZE;
  private final static String SERVICE_STATS = CommonConfig.STATS;
  private final static String SERVICE_STATSRESET = CommonConfig.STATS_RESET;

  //performance counters
  private long processingTime = 0;
  private long recordsProcessed = 0;
  private long streamsProcessed = 0;
  private int outBufferCapacity = 0;
  private int bufferHits = 0;

  // used to simplify logging and exception handling
  public String message;

  /**
   * Default constructor
   */
  public AbstractInputAdapter() {
  }

  /**
   * Get the batch size for the input adapter. This determines the maximum
   * number of records that will be read from the input stream before a batch is
   * pushed into the processing stream.
   *
   * @param PipelineName The name of the pipeline that is using this adapter
   * @param ModuleName The module name of this adapter
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
    String ConfigHelper;
    setSymbolicName(ModuleName);

    // store the pipe we are in
    setPipeline(OpenRate.getPipelineFromMap(PipelineName));

    // Register the events that we can process with the event manager
    registerClientManager();

    // Get the batch size we should be working on
    ConfigHelper = initGetBatchSize();
    processControlEvent(SERVICE_BATCHSIZE, true, ConfigHelper);
    ConfigHelper = initGetBufferSize();
    processControlEvent(SERVICE_BUFFERSIZE, true, ConfigHelper);
  }

  /**
   * No-op cleanup method. Meant to be overridden if necessary.
   */
  @Override
  public void cleanup() {
    // no op
  }

  /**
   * Push a set of records into the pipeline.
   *
   * @param validBuffer The buffer that will receive the good records
   * @return The number of records pushed
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  public int push(IConsumer validBuffer) throws ProcessingException {
    long startTime;
    long endTime;
    long BatchTime = 0;
    int size = 0;
    Collection<IRecord> validRecords;
    Collection<IRecord> all;

    // load records
    startTime = System.currentTimeMillis();

    try {
      // Get the batch of records from the implementation class
      validRecords = loadBatch();

      // Create a new batch
      all = new ArrayList<>();

      // Add all the records to the new batch
      all.addAll(validRecords);

      // see how many records we got
      size = all.size();
      if (size > 0) {
        // push the records into the buffer if we had any
        validBuffer.push(validRecords);
      }

      endTime = System.currentTimeMillis();
      BatchTime = (endTime - startTime);
      processingTime += BatchTime;
      recordsProcessed += size;
      outBufferCapacity = validBuffer.getEventCount();

      while (outBufferCapacity > bufferSize) {
        bufferHits++;
        OpenRate.getOpenRateStatsLog().debug("Input  <" + getSymbolicName() + "> buffer high water mark! Buffer max = <" + bufferSize + "> current count = <" + outBufferCapacity + ">");
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          //
        }

        // refresh
        outBufferCapacity = validBuffer.getEventCount();
      }
    } catch (ProcessingException pe) {
      getPipeLog().error("Processing exception caught in Input Adapter <"
              + getSymbolicName() + ">", pe);
      getExceptionHandler().reportException(pe);
    } catch (NullPointerException npe) {
      getPipeLog().error("Null Pointer exception caught in Input Adapter <"
              + getSymbolicName() + ">", npe);
      getExceptionHandler().reportException(new ProcessingException(npe, getSymbolicName()));
    } catch (Throwable t) {
      // ToDo: Force only allowed exception types up
      getPipeLog().fatal("Unexpected exception caught in Input Adapter <"
              + getSymbolicName() + ">", t);
      getExceptionHandler().reportException(new ProcessingException(t, getSymbolicName()));
    }

    OpenRate.getOpenRateStatsLog().debug(
            "Input  <" + getSymbolicName() + "> pushed <"
            + size + "> events into the valid buffer <"
            + validBuffer.toString() + "> in <" + BatchTime + "> ms");

    return size;
  }

  /**
   * Retrieve a batch of records from the adapter.
   *
   * @return The collection of records that was loaded
   * @throws OpenRate.exception.ProcessingException
   */
  protected abstract Collection<IRecord> loadBatch() throws ProcessingException;

  /**
   * Set the buffer that we will be writing to
   *
   * @param ch The buffer for valid records
   */
  @Override
  public void setBatchOutboundValidBuffer(IConsumer ch) {
    this.consumer = ch;
  }

  /**
   * Get the buffer that we will be writing to
   *
   * @return The consumer buffer
   */
  @Override
  public IConsumer getBatchOutboundValidBuffer() {
    return this.consumer;
  }

  /**
   * return the symbolic name
   *
   * @return The symbolic name for this class stack
   */
  @Override
  public String getSymbolicName() {
    return symbolicName;
  }

  /**
   * set the symbolic name
   *
   * @param name The symbolic name for this class stack
   */
  @Override
  public void setSymbolicName(String name) {
    symbolicName = name;
  }

  /**
   * Increment the streams processed counter
   */
  public void incrementStreamCount() {
    streamsProcessed++;
  }

  // -----------------------------------------------------------------------------
  // ----------------- Start of published hookable functions ---------------------
  // -----------------------------------------------------------------------------
  /**
   * This is called when the synthetic Header record is encountered, and has the
   * meaning that the stream is starting. In this case we have to open a new
   * dump file each time a stream starts. *
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public abstract HeaderRecord procHeader(HeaderRecord r) throws ProcessingException;

  /**
   * This is called when the synthetic trailer record is encountered, and has
   * the meaning that the stream is now finished. In this example, all we do is
   * pass the control back to the transactional layer.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public abstract TrailerRecord procTrailer(TrailerRecord r) throws ProcessingException;

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------
  /**
   * registerClientManager registers this class as a client of the ECI listener
   * and publishes the commands that the plug in understands. The listener is
   * responsible for delivering only these commands to the plug in.
   *
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void registerClientManager() throws InitializationException {
    // Set the client reference and the base services first
    ClientManager.getClientManager().registerClient(getPipeName(), getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BATCHSIZE, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_BUFFERSIZE, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATS, ClientManager.PARAM_NONE);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_STATSRESET, ClientManager.PARAM_DYNAMIC);
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
          String Parameter) {
    int ResultCode = -1;
    double CDRsPerSec;

    // Reset the Statistics
    if (Command.equalsIgnoreCase(SERVICE_STATSRESET)) {
      if (Parameter.equalsIgnoreCase("true")) {
        processingTime = 0;
        recordsProcessed = 0;
        streamsProcessed = 0;
        bufferHits = 0;
      }
      ResultCode = 0;
    }

    // Return the Statistics
    if (Command.equalsIgnoreCase(SERVICE_STATS)) {
      if (processingTime == 0) {
        CDRsPerSec = 0;
      } else {
        CDRsPerSec = (double) ((recordsProcessed * 1000) / processingTime);
      }

      return Long.toString(recordsProcessed) + ":"
              + Long.toString(processingTime) + ":"
              + Long.toString(streamsProcessed) + ":"
              + Double.toString(CDRsPerSec) + ":"
              + Long.toString(outBufferCapacity) + ":"
              + Long.toString(bufferHits) + ":"
              + Long.toString(getBatchOutboundValidBuffer().getEventCount());
    }

    if (Command.equalsIgnoreCase(SERVICE_BATCHSIZE)) {
      if (Parameter.equals("")) {
        return Integer.toString(batchSize);
      } else {
        try {
          batchSize = Integer.parseInt(Parameter);
        } catch (NumberFormatException nfe) {
          getPipeLog().error("Invalid number for batch size. Passed value = <"
                  + Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (Command.equalsIgnoreCase(SERVICE_BUFFERSIZE)) {
      if (Parameter.equals("")) {
        return Integer.toString(bufferSize);
      } else {
        try {
          bufferSize = Integer.parseInt(Parameter);
        } catch (NumberFormatException nfe) {
          getPipeLog().error(
                  "Invalid number for buffer size. Passed value = <"
                  + Parameter + ">");
        }

        ResultCode = 0;
      }
    }

    if (ResultCode == 0) {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    } else {
      return "Command Not Understood";
    }
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of local utility functions -----------------------
  // -----------------------------------------------------------------------------
  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetBatchSize()
          throws InitializationException {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_BATCHSIZE, DEFAULT_BATCHSIZE);

    return tmpValue;
  }

  /**
   * Temporary function to gather the information from the properties file. Will
   * be removed with the introduction of the new configuration model.
   */
  private String initGetBufferSize()
          throws InitializationException {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getBatchInputAdapterPropertyValueDef(getPipeName(), getSymbolicName(),
            SERVICE_BUFFERSIZE, DEFAULT_BUFFERSIZE);

    return tmpValue;
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
  public void setPipeline(IPipeline pipeline) {
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
