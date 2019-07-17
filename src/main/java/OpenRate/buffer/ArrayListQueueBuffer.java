

package OpenRate.buffer;

import OpenRate.record.IRecord;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Builds on the Abstract Buffer class to add record storage capability to the
 * buffer.
 *
 * This buffer class holds separate buffers for batch and real-time events. The
 * events are held in separate buffers, but if there are real time events to
 * pass, these are dealt with in precedence to the batch events.
 */
public class ArrayListQueueBuffer
  extends AbstractBuffer
{
  // implementation all deferred to this class.
  private List<IRecord>   queueHelperBatch;
  private final Object lock;

 /**
  * Default constructor.
  */
  public ArrayListQueueBuffer()
  {
    super();

    //queueHelper = Collections.synchronizedList(new ArrayList());
    queueHelperBatch = new ArrayList<>();

    // the locking object so that we make sure that the multi-threadedness of
    // the object is guaranteed
    lock = new Object();
  }

 /**
  * Push an entire collection of batch records into the buffer. This is the
  * main event for the addition of records into a buffer.
  *
  * @param collection The collection of records to push
  */
  @Override
  public void push(Collection<IRecord> collection)
  {
    synchronized (lock)
    {
      // blocks waiting for queue availability.
      queueHelperBatch.addAll(collection);
    }

    // tell the downstream modules that there is stuff to do
    notifyMonitors();
  }

 /**
  * Retrieve a number of batch records from the buffer. The number of records
  * that is returned is either all of the records available in the case that the
  * number of records is less than the specified maximum, or the specified
  * maximum. Note that the primary reason for limiting the number of records
  * that can be pulled at a time is to:
  *  - make sure that a particularly slow module does not block the processing
  *  - to allow multiple instances of the same plug in to work on records
  *    in parallel, thus removing bottlenecks
  *
  * Note that the records are retrieved from the head of the list, despite the
  * fact that retrieveing from the tail could provide a higher performance. In
  * the case that you really do not care about ordering, you could tweak this.
  */
  @Override
  public Collection<IRecord> pull(int max)
  {
    ArrayList<IRecord> list = new ArrayList<>();

    synchronized (lock)
    {
      int size = queueHelperBatch.size();

      for (int i = 0; (i < max) && (size > 0); ++i)
      {
        // It would be faster to pull from the end of the array instead
        // of the beginning, but if order matters (and mostly it will) we have
        // to work from the head of the queue.
        // In cases where order really does not matter, we could use:
        // Object o = queueHelper.remove(--size);
        IRecord o = queueHelperBatch.remove(0);
        --size;
        list.add(o);
      }
    }

    return list;
  }

 /**
  * Return the number of events in the buffers
  *
  * @return The number of events in the buffers
  */
  @Override
  public int getEventCount()
  {
    return queueHelperBatch.size();
  }
}
