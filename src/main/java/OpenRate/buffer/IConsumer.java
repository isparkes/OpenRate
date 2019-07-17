

package OpenRate.buffer;

import OpenRate.record.IRecord;
import java.util.Collection;

/**
 * Consumer interface providing the method to pass records to a downstream
 * consumer. This is used as a sink for records that have been processed.
 */
public interface IConsumer
{
 /**
  * Place a collection of batch records into the FIFO buffer, possibly waiting
  * indefinitely until it can be accepted. Blocks until object is
  * completely stored, thus implementations should be performance aware.
  *
  * @param c The collection of records to push
  */
  public void push(Collection<IRecord> c);

 /**
  * Return the number of events in the buffer. To be implemented by the
  * concrete implementation.
  *
  * @return The number of events in the buffer
  */
  public int getEventCount();
}
