
package OpenRate;

import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.transaction.ISyncPoint;

/**
 * The IPipeline Abstraction is the container or implementor of the pipeline
 * application logic. It's principle reason for being is to allow multiple
 * pipelines to run within the same framework.
 *
 * Pipeline life cycle:
 *
 * init() ready for run run() running told to stop markForShutdown()
 * shutdownPipeline() cleanupPipeline()
 */
public interface IPipeline
        extends Runnable, ISyncPoint {

  /**
   * Init method will be called prior to run so that we set up the pipeline and
   * instantiate and initialise all the modules.
   *
   * @param PipelineName The name of this pipeline
   * @throws OpenRate.exception.InitializationException
   */
  public void init(String PipelineName)
          throws InitializationException;

  /**
   * Perform pipeline plugin level close down processing.
   */
  public void shutdownPipeline();

  /**
   * Perform any process level cleanup required. This should take care of
   * de-referencing and closing process level things
   */
  public void cleanupPipeline();

  /**
   * stop the process. This can be called to safely shutdown a process that is
   * either long running or runs continuously. It's not intended to be a drastic
   * shutdown, but rather a "find a reasonable spot to stop & do so" type
   * message. A few minutes of processing before reaching such a point is, while
   * not recommended, not unreasonable in certain cases. The process should make
   * an effort to shutdown as quickly as is reasonable without leaving the
   * application in an invalid state.
   */
  public void markForShutdown();

  /**
   * Return the symbolic name of the pipe
   */
  @Override
  public String getSymbolicName();

  /**
   * Returns true if the pipe is a batch pipeline
   *
   * @return true if the pipeline is a batch pipeline, false if it is real time
   */
  public boolean isBatchPipeline();

  /**
   * Used for processing schedule management - set the pipe to the fast schedule
   */
  public void setSchedulerHigh();

  /**
   * Used for processing schedule management - see if the pipe is in the fast
   * schedule
   *
   * @return true if we are in the fast schedule
   */
  public boolean getSchedulerHigh();

  /**
   * Returns true if the pipe aborted
   *
   * @return true if the pipeline aborted
   */
  public boolean isAborted();

  /**
   * Returns the pipeline logger.
   *
   * @return The logger for the pipeline
   */
  public ILogger getPipeLog();

  /**
   * Returns the pipeline exception handler.
   *
   * @return The exception handler for the pipeline
   */
  public ExceptionHandler getPipelineExceptionHandler();
}
