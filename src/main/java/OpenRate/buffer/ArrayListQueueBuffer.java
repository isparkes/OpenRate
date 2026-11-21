/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package OpenRate.buffer;

import OpenRate.record.IRecord;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;


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
  //
  // ArrayDeque and not ArrayList: pull() takes records from the head, and
  // ArrayList.remove(0) copies the whole backing array down a place each
  // time, so draining a deep buffer was quadratic on the hottest path in the
  // framework. ArrayDeque.removeFirst() is constant time and keeps the same
  // FIFO ordering.
  private final Deque<IRecord> queueHelperBatch;
  private final Object lock;

 /**
  * Default constructor.
  */
  public ArrayListQueueBuffer()
  {
    super();

    queueHelperBatch = new ArrayDeque<>();

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
    ArrayList<IRecord> list;

    synchronized (lock)
    {
      int count = Math.min(max, queueHelperBatch.size());

      if (count < 0)
      {
        count = 0;
      }

      list = new ArrayList<>(count);

      for (int i = 0; i < count; i++)
      {
        // Records come off the head, because order matters almost everywhere.
        list.add(queueHelperBatch.removeFirst());
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
    // Under the lock, like every other access to the queue. Reading the size
    // outside it was a data race on a structure that is not thread safe, with
    // no happens-before edge to the pushes, and the pipeline scheduler uses
    // this value to decide whether the pipe is idle and should sleep.
    synchronized (lock)
    {
      return queueHelperBatch.size();
    }
  }
}
