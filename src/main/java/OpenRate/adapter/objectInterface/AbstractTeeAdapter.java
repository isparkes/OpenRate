

package OpenRate.adapter.objectInterface;

import OpenRate.adapter.realTime.TeeBatchConverter;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.process.AbstractPlugIn;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import OpenRate.utils.PropertyUtils;
import java.util.Collection;

/**
 * This module tees into a Real Time pipeline and takes a feed of the events
 * for putting into a batch pipeline. This is usually used for persistence
 * of RT events in a batch mode, however, it can also be used for balance
 * updates in a batch pipeline.
 *
 * Buffer Input Adapter
 * --------------------
 * The output of the buffer tee adapter allows you to "sniff" events out of
 * a pipeline (real time or batch) and put them into another pipeline (batch)
 * for persistence or further processing. This converts synchronous events into
 * asynchronous events. The coupling between the two pipelines is a normal FIFO
 * buffer. The buffer is accessed using a "LinkedBufferCache" which allows the
 * FIFO to be set up and accessed by name.
 *
 * Input >->->- Pipeline 1 ->->->- Buffer Tee Adapter ->->-> Output
 *                                     |
 *   +------------- Buffer ------------+
 *   |
 *   +-> Buffer Input Adapter >->- Pipeline 2 ->->->-> Output
 *
 *
 * Socket Input Adapter
 * --------------------
 * The output of the socket tee adapter allows you to "sniff" events out of
 * a pipeline (real time or batch) and put them into another pipeline (batch)
 * for further processing.
 *
 * Input >->->- Pipeline 1 ->->->- Socket Tee Adapter ->->-> Output
 *                                     |
 *   +------------- TCPIP -------------+
 *   |
 *   +-> Socket Input Adapter >->- Pipeline 2 ->->->-> Output
 *
 */
public abstract class AbstractTeeAdapter
        extends AbstractPlugIn
        implements ITeeAdapter
{
  private final static String SERVICE_CONVBATCH  = "ConversionBatchSize";
  private final static String DEFAULT_CONVBATCH  = "1000";
  private final static String SERVICE_PURGETIME  = "PurgeTime";
  private final static String DEFAULT_PURGETIME  = "5000";

  /**
   * This is the local variable that we use to determine the batch size
   */
  protected int BatchSize;

  /**
   * the batch converter thread
   */
  protected TeeBatchConverter batchConv;

 /**
  * This is the local variable that we use to determine the buffer high water
  * mark
  */
  protected int purgeTime;

  @Override
  public void init(String PipelineName, String ModuleName) throws InitializationException
  {
    String ConfigHelper;

    super.init(PipelineName, ModuleName);

    // Create the batch converter if we need it
    batchConv = new TeeBatchConverter();

    // Get the batch size we should be working on
    ConfigHelper = initGetConvBatchSize();
    processControlEvent(SERVICE_CONVBATCH, true, ConfigHelper);
    ConfigHelper = initGetPurgeTime();
    processControlEvent(SERVICE_PURGETIME, true, ConfigHelper);

    // launch it in it's own thread
    Thread batchConvThread = new Thread(batchConv, getPipeName() + "-BatchConverter");

    batchConv.setParentAdapter(this);

    // Start the thread
    batchConvThread.start();
  }

  @Override
  public IRecord procHeader(IRecord r)
  {
    return r;
  }

  @Override
  public IRecord procValidRecord(IRecord r) throws ProcessingException
  {
    FlatRecord tmpRecord = (FlatRecord) performValidOutputMapping(r);

    if (tmpRecord != null)
    {
      addRecordToOutputBatch(tmpRecord);
    }

    return r;
  }

  @Override
  public IRecord procErrorRecord(IRecord r) throws ProcessingException
  {
    FlatRecord tmpRecord = (FlatRecord) performErrorOutputMapping(r);

    if (tmpRecord != null)
    {
      addRecordToOutputBatch(tmpRecord);
    }

    return r;
  }

  @Override
  public IRecord procTrailer(IRecord r)
  {
    return r;
  }

  /**
   * Places a record into the output buffer for processing by a batch adapter
   * using socket communication.
   *
   * @param tmpRecord The record to write
   */
  public void addRecordToOutputBatch(IRecord tmpRecord)
  {
    batchConv.addRecordToOutputBatch(tmpRecord, false);
  }

 /**
  * Push the collected batch of records into the transport layer
  *
  * @param batchToPush The batch we are pushing
  */
  @Override
  public abstract void pushTeeBatch(Collection<IRecord> batchToPush);

  // ----------------------------------------------------------------------------
  // ----------------- Start of published hookable functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * This method takes the outgoing real time record, and prepares it for
  * returning to the submitter.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  * @throws ProcessingException
  */
  public abstract IRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException;

 /**
  * This method takes the outgoing real time record, and prepares it for
  * returning to the submitter.
  *
  * @param RTRecordToProcess The real time record to process
  * @return The processed real time record
  */
  public abstract IRecord performErrorOutputMapping(IRecord RTRecordToProcess);

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
    ClientManager.getClientManager().registerClient(getPipeName(),getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_CONVBATCH, ClientManager.PARAM_MANDATORY);
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_PURGETIME, ClientManager.PARAM_MANDATORY);
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

    if (ResultCode == 0)
    {
      getPipeLog().debug(LogUtil.LogECIPipeCommand(getSymbolicName(), getPipeName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return super.processControlEvent(Command, Init, Parameter);
    }
  }
  // -----------------------------------------------------------------------------
  // -------------------- Start of local utility functions -----------------------
  // -----------------------------------------------------------------------------

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetConvBatchSize() throws InitializationException
  {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),getSymbolicName(),
                                                   SERVICE_CONVBATCH, DEFAULT_CONVBATCH);

    return tmpValue;
  }

 /**
  * Temporary function to gather the information from the properties file. Will
  * be removed with the introduction of the new configuration model.
  */
  private String initGetPurgeTime() throws InitializationException
  {
    String tmpValue;
    tmpValue = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(getPipeName(),getSymbolicName(),
                                                   SERVICE_PURGETIME, DEFAULT_PURGETIME);

    return tmpValue;
  }
}
