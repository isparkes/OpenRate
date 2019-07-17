package OpenRate.adapter;

import OpenRate.IPipeline;
import OpenRate.buffer.IConsumer;
import OpenRate.exception.ProcessingException;

/**
 * The IInputAdapter is the interface responsible for selecting a set of work
 * from an external data source for processing and pushing it into the IBuffer
 * for processing.
 */
public interface IInputAdapter
        extends IAdapter {

  /**
   * Send a set of records requiring processing to the pipeline. The collection
   * should contain objects of type IRecord.
   *
   * @param validBuffer The buffer that will receive the pushed records
   * @return The number of records pushed
   * @throws OpenRate.exception.ProcessingException
   */
  public int push(IConsumer validBuffer)
          throws ProcessingException;

  /**
   * Sets the buffer that will receive the pushed batch records
   *
   * @param ch The receiving buffer
   */
  public void setBatchOutboundValidBuffer(IConsumer ch);

  /**
   * Gets the buffer that will receive the pushed batch records
   *
   * @return The consumer output FIFO
   */
  public IConsumer getBatchOutboundValidBuffer();

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
   * Set the pipeline reference so the input adapter can control the scheduler
   *
   * @param pipeline the Pipeline to set
   */
  public void setPipeline(IPipeline pipeline);

  /**
   * @return the pipeline
   */
  public IPipeline getPipeline();
}
