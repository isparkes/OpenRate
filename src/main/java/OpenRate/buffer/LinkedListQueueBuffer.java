

package OpenRate.buffer;

import OpenRate.record.IRecord;
import java.util.Collection;
import java.util.LinkedList;


/**
 * Buffer implementation using an LinkedListQueue as the buffering element.
 * As this is one of the performance sensitive parts of the system, you might
 * want to define different buffering structures, depending on the limitations
 * of the application that you are working on. However the ArrayListQueue will
 * fulfill the requirements for a large majority of applications, as it:
 *  - Preserves record ordering
 *  - Allows easy addition and extraction
 *  - Is reasonably fast
 */
public class LinkedListQueueBuffer
  extends AbstractBuffer
{
  // The main storage object of the buffer
  private LinkedList<IRecord> queueHelperBatch;

  // the locking object so that we make sure that the multi-threadedness of
  // the object is guaranteed
  private final Object lock;

  /**
   * Default constructor.
   */
  public LinkedListQueueBuffer()
  {
    super();

    //queueHelper = Collections.synchronizedList(new LinkedList());
    queueHelperBatch = new LinkedList<>();
    lock = new Object();
  }

 /**
  * Push an entire collection of batch records into the buffer.
  * This is the main event for the addition of records into a buffer.
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
  * fact that retrieving from the tail could provide a higher performance. In
  * the case that you really do not care about ordering, you could tweak this.
  */
  @Override
  public Collection<IRecord> pull(int max)
  {
    LinkedList<IRecord> list = new LinkedList<>();

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
        IRecord o = queueHelperBatch.remove();
        list.add(o);
      }
    }

    return list;
  }

 /**
  * Return the number of events in the buffer
  */
  @Override
  public int getEventCount()
  {
    return queueHelperBatch.size();
  }
}
