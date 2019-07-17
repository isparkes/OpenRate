
package OpenRate.adapter.realTime;

import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;

/**
 * This class implements a socket listener based on the the real time (RT)
 * adapter for the OpenRate framework. This adapter handles real time events
 * coming from socket sources.
 */
public abstract class AbstractRTMethodAdapter extends AbstractRTAdapter
{
  // Holds the reference to the raw processing method
  private static IRTAdapter processingAdapter;

  /**
   * Used to link directly into the raw processing pipe
   *
   * @return the reference to the processing adapter
   */
  public static IRTAdapter getProcessingAdapter()
  {
    return processingAdapter;
  }

 /**
  * Constructor
  */
  public AbstractRTMethodAdapter()
  {
    super();

    processingAdapter = this;
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
    // Perform parent processing first
    super.init(PipelineName, ModuleName);
  }

 /**
  * Create the listener object, which will be used to allow the communication
  * with the client tasks.
  */
  @Override
  public void initialiseInputListener()
  {
    // Create the communication objects
  }

  @Override
  public void shutdownInputListener()
  {
    // Nothing
  }

 /**
  * Stubbed out methods for mapping. Override if you want to use them. It is
  * unlikely, because the method based processing can create records of the
  * correct format directly, whereas socket processing receives the information
  * in a string format therefore forcing the conversion from flat data to
  * record data.
  *
  * @param RTRecordToProcess Nothing
  * @return Nothing
  * @throws ProcessingException
  */
  @Override
  public FlatRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException {
    throw new UnsupportedOperationException("Not supported.");
  }

 /**
  * Stubbed out methods for mapping. Override if you want to use them. It is
  * unlikely, because the method based processing can create records of the
  * correct format directly, whereas socket processing receives the information
  * in a string format therefore forcing the conversion from flat data to
  * record data.
  *
  * @param RTRecordToProcess Nothing
  * @return Nothing
  */
  @Override
  public FlatRecord performErrorOutputMapping(IRecord RTRecordToProcess) {
    throw new UnsupportedOperationException("Not supported.");
  }
}
