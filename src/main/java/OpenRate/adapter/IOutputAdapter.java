package OpenRate.adapter;

import OpenRate.IPipeline;
import OpenRate.buffer.IConsumer;
import OpenRate.buffer.ISupplier;
import OpenRate.exception.ProcessingException;

/**
 * The IOutputAdapter is responsible for taking a completed work set and storing
 * it to the external persistence providers.
 */
public interface IOutputAdapter
        extends IAdapter,
        Runnable {

  /**
   * Do any non-record level processing required to finish this batch cycle.
   *
   * @return The number of records in the outbound buffer
   */
  public int getOutboundRecordCount();

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
   * Close is called outside of the strategy to allow the output adapter to
   * commit any work that should only be done once per interface run. NOT once
   * per batch cycle like flush()
   *
   * @throws OpenRate.exception.ProcessingException
   */
  public void close()
          throws ProcessingException;

  /**
   * Perform any cleanup. Called by the OpenRateApplication during application
   * shutdown. This should do any final cleanup and closing of resources. Note:
   * It is not called during normal processing, so it's only useful for true
   * shutdown logic.
   *
   * If you need something to happen during normal pipeline execution, use
   * <code>getOutboundRecordCount()</code>.
   */
  @Override
  public void cleanup();

  /**
   * Set the inbound valid record buffer
   *
   * @param ch The supplier buffer to set
   */
  public void setBatchInboundValidBuffer(ISupplier ch);

  /**
   * Return the inbound valid record buffer
   *
   * @return The current supplier buffer
   */
  public ISupplier getBatchInboundValidBuffer();

  /**
   * Set the outbound valid record buffer
   *
   * @param ch The consumer buffer to set
   */
  public void setBatchOutboundValidBuffer(IConsumer ch);

  /**
   * Return the outbound valid record buffer
   *
   * @return The current consumer buffer
   */
  public IConsumer getBatchOutboundValidBuffer();

  /**
   * Do anything that we need to do to close down the stream
   *
   * @param TransactionNumber
   */
  public void closeStream(int TransactionNumber);

  /**
   * Set if we are a terminating output adapter or not
   *
   * @param Terminator True if we are the terminating adapter
   */
  public void setTerminator(boolean Terminator);

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
