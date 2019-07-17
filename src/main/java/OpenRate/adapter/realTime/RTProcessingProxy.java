
package OpenRate.adapter.realTime;

import OpenRate.buffer.IConsumer;
import OpenRate.logging.AstractLogger;

/**
 *
 * @author ian
 */
public class RTProcessingProxy
{
  private IConsumer Consumer;

  // This is the reference to the RT adapter that will handle requests
  private IRTAdapter ParentRTAdapter;

  // this is the name of the pipeline log that we will write to
  private String pipeName;

  /**
   * The PipeLog is the logger which should be used for all pipeline level
   * messages. This is instantiated during pipe startup, because at this
   * point we don't know the name of the pipe and therefore the logger to use.
   */
  protected AstractLogger PipeLog = null;

  void setRTConsumer(IConsumer PipeInputBuffer)
  {
    this.Consumer = PipeInputBuffer;
  }

  void setParentAdapter(IRTAdapter ParentRTAdapter)
  {
    this.ParentRTAdapter = ParentRTAdapter;
  }

  void setPipelineLog(AstractLogger PipeLog)
  {
    this.PipeLog = PipeLog;
  }
}
