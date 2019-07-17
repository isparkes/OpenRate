
package OpenRate.adapter.realTime;

import OpenRate.CommonConfig;
import OpenRate.adapter.IAdapter;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.process.IPlugIn;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import java.util.ArrayList;

/**
 * The IOutputAdapter is responsible for taking a completed
 * work set and storing it.
 */
public interface IRTAdapter
  extends IAdapter,
          Runnable
{
 /**
  * The tag that lets us get the batch size setting.
  */
  public static final String BATCH_SIZE = CommonConfig.BATCH_SIZE;

 /**
  * The default value of the batcgh size.
  */
  public static final String DEFAULT_BATCH_SIZE = CommonConfig.DEFAULT_BATCH_SIZE;

// ************************ Initialisation Stuff *******************************

 /**
  * Initialise the module. Called during pipeline creation.
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The module symbolic name of this module
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException;

// ************************ Input Adapter Stuff ********************************

 /**
  * This method is used to serialise the calls from various listener threads,
  * putting them in order, and requesting that the user level implementation
  * layer parse the records. This method is implemented as a synchronised
  * method, enforcing an ordering on the records.
  *
  * @param RTRecordToProcess The Real Time record to process
  * @return The processed real time record
  * @throws ProcessingException
  */
  public IRecord performInputMapping(FlatRecord RTRecordToProcess) throws ProcessingException;

// ************************ Output Adapter Stuff *******************************

  /**
   * Processing method for the output adapter. Opens the possibility
   * of processing completed records asynchronously while the pipeline
   * is running. It can also be run directly by the exec strategy from
   * within the main thread. Both modes are supported and reasonable.
   */
  @Override
  public void run();

  /**
   * reset the plug in to ensure that it's ready to process records again after
   * it has been exited by calling markForExit().
   */
  public void reset();

  /**
   * Mark output adapter complete so that it exits at the next possibility
   */
  public void markForClosedown();

  /**
   * Close is called outside of the strategy to allow the output
   * adapter to commit any work that should only be done once per
   * interface run. NOT once per batch cycle like flush()
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void close()
    throws ProcessingException;

  /**
   * Perform any cleanup. Called by the OpenRateApplication during application
   * shutdown. This should do any final cleanup and closing of resources.
   * Note: It is not called during normal processing, so it's only useful for
   * true shutdown logic.
   *
   * If you need something to happen during normal pipeline execution, use
   * <code>completeBatch()</code>.
   */
  @Override
  public void cleanup();

 /**
  * process a record using Real Time mode
  *
  * @param recordToProcess The record we are to process
  * @return The processed record
  * @throws ProcessingException
  */
  public IRecord processRTRecord(IRecord recordToProcess) throws ProcessingException;

 /**
  * process a record using Real Time mode
  *
  * @param recordToProcess The record we are to process
  * @return The processed record
  * @throws ProcessingException
  */
  public FlatRecord processRTRecord(FlatRecord recordToProcess) throws ProcessingException;

  /**
   * Performs any final shutdown activities for this adapter.
   */
  public void closeStream();

 /**
  * return the symbolic name
  *
  * @return The current module symbolic name
  */
  public String getSymbolicName();

 /**
  * set the symbolic name
  *
  * @param name The symbolic name to set
  */
  public void setSymbolicName(String name);

  /**
   * Set the list of plugins for this piplines, in order that we may process
   * a record down the chain at will
   *
   * @param PlugInList The list of plugins in the pipe
   */
  public void setProcessingList(ArrayList<IPlugIn> PlugInList);

  /**
   * Perform the output processing on a valid record
   *
   * @param RTRecordToProcess The record to perform the mapping on
   * @return The mapped flat record
   * @throws ProcessingException
   */
  public FlatRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException;

  /**
   * Perform the output processing on an error record
   *
   * @param RTRecordToProcess The record to perform the mapping on
   * @return The mapped flat record
   * @throws ProcessingException
   */
  public FlatRecord performErrorOutputMapping(IRecord RTRecordToProcess) throws ProcessingException;
}
