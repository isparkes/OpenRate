

package OpenRate.process;

import OpenRate.buffer.IConsumer;
import OpenRate.buffer.ISupplier;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;

/**
 * IPlugIn Interface. This defines the basic elements needed for processing
 * records in a chained manner. The implementations of this will deal with the
 * actual task of moving the data around.
 */
public interface IPlugIn extends Runnable
{
 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  public void init(String PipelineName, String ModuleName) throws InitializationException;

 /**
  * do the actual processing for the plug in
  *
  * @throws OpenRate.exception.ProcessingException
  */
  public void process() throws ProcessingException;

 /**
  * Count the number of records waiting at the output of this plugin
  *
  * @return The number of records in this batch
  */
  public int getOutboundRecordCount();

  /**
  * Shuts down the PlugIn. Use this to save any configuration or data before
  * the plug in closes
  */
  public void shutdown();

  /**
   * Set the inbound delivery mechanism.
   *
   * @param c The supplier
   */
  public void setInbound(ISupplier c);

 /**
  * Set the outbound delivery mechanism.
  *
  * @param c The consumer
  */
  public void setOutbound(IConsumer c);

 /**
  * Set the holding bin used for errored records.
  *
  * @param err The consumer for error records
  */
  public void setErrorBuffer(IConsumer err);

 /**
  * Set the handler used for reporting fatal errors during IPlugIn
  * processing. Since each IPlugIn is run within a thread, the call stack is
  * not an appropriate mechanism for reporting critical processing errors.
  * Therefore, a Handler object is provided for the Plug In to use for
  * logging unrecoverable processing errors.
  * Note: This handler must assume that any error is fatal. Non-fatal errors
  * should be handled by adding errors to the record(s) being processed.
  *
  * @param h The parent assigned handler
  */
  public void setExceptionHandler(ExceptionHandler h);

 /**
  * Tag this plug in to shutdown. Implementation may vary. For example, a
  * plug in may be marked for exit, but not actually shutdown until it detects
  * a certain number of empty cycles.
  */
  public void markForShutdown();

 /**
  * reset the plug in to ensure that it's ready to process records again after
  * it has been exited by calling markForExit().
  */
  public void reset();

 /**
  * Return the suggested number of threads to launch for this
  * IPlugIn. Obviously this requires that the application uses
  * a Pipeline that implements multi-threading.
  *
  * @return The number of threads in use
  */
  public int numThreads();

 /**
  * return the symbolic name
  *
  * @return The symbolic name
  */
  public String getSymbolicName();

 /**
  * set the symbolic name
  * @param name The symbolic name
  */
  public void setSymbolicName(String name);

 /**
  * This is called when a RT data record is encountered. You should do any normal
  * processing here. For most purposes this is steered to the normal (batch)
  * processing, but this can be overwritten
  *
  * @param r The record we are working on
  * @return The processed record
  * @throws ProcessingException
  */
  public IRecord procRTValidRecord(IRecord r) throws ProcessingException;

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
  public IRecord procRTErrorRecord(IRecord r) throws ProcessingException;
}
