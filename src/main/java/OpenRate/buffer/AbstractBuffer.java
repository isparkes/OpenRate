

package OpenRate.buffer;

import java.util.HashSet;
import java.util.Iterator;


/**
 * Abstract FIFO Buffer class.
 *
 * This is the heart of the record passing scheme, and defines the record
 * transport strategy, which is used to transport records through the pipeline
 * between one plug-in and the next. Each plug-in reads (pulls) records from the
 * input buffer, works on them, and then writes (pushes) them to the output
 * buffer. Exceptions to this are of course input adapters, which read from some
 * external source, and push records into the pipeline, and output adapters,
 * which read records and (may) destroy them.
 *
 * This abstract class must be extended with a storage class, which is able to
 * contain records, as this abstract class only deals with the monitor
 * management.
 */
public abstract class AbstractBuffer
  implements IBuffer
{
  // this hash set contains all of the monitors to this buffer. These will
  // be notified when new records are ready.
  private final HashSet<IMonitor> monitors = new HashSet<>();

  private String Supplier;
  private String Consumer;

 /**
  * Constructor for AbstractBuffer
  */
  public AbstractBuffer()
  {
  }

 /**
  * notifyMonitors notifies all monitors to this buffer, triggering each of
  * them in turn.
  */
  protected void notifyMonitors()
  {
    synchronized (monitors)
    {
      Iterator<IMonitor> iter = monitors.iterator();

      while (iter.hasNext())
      {
        IMonitor m = iter.next();
        m.notify(BufferEvent.NEW_RECORDS);
      }
    }
  }

 /**
  * registerMonitor adds a new monitor to the internal list of monitors to
  * this buffer.
  *
  * @param m The monitor object to be added
  */
  @Override
  public void registerMonitor(IMonitor m)
  {
    synchronized (monitors)
    {
      monitors.add(m);
    }
  }

 /**
  * Get the buffer supplier name
  *
  * @return the name of the assigned buffer supplier
  */
  @Override
  public String getSupplier()
  {
    return Supplier;
  }

 /**
  * Set the buffer supplier name
  *
  * @param newSupplier the name of the assigned buffer supplier
  */
  @Override
  public void setSupplier(String newSupplier)
  {
    Supplier = newSupplier;
  }

 /**
  * Get the buffer consumer name
  *
  * @return the name of the assigned buffer consumer
  */
  @Override
  public String getConsumer()
  {
    return Consumer;
  }

 /**
  * Set the buffer consumer name
  *
  * @param newComsumer the name of the assigned buffer consumer
  */
  @Override
  public void setConsumer(String newComsumer)
  {
    Consumer = newComsumer;
  }
}
