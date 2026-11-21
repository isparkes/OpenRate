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
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
  // The main storage object of the buffer.
  //
  // LinkedBlockingQueue and not ConcurrentLinkedDeque: size() on a
  // ConcurrentLinkedDeque walks the whole queue, and the pipeline scheduler
  // asks every buffer in the pipe for its count on every cycle, so a deep
  // buffer made the scheduler itself the bottleneck. LinkedBlockingQueue
  // keeps a count, so size() is constant time, and drainTo() takes a batch
  // off in one go.
  //
  // Unlike ArrayListQueueBuffer this does not hold a lock across push(), so
  // a consumer can see part of a batch before the rest of it arrives. Order
  // within what each producer pushes is still kept.
  private final BlockingQueue<IRecord> queueHelperBatch;


  public LinkedListQueueBuffer()
  {
    queueHelperBatch = new LinkedBlockingQueue<>();
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
    queueHelperBatch.addAll(collection);

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
    ArrayList<IRecord> list = new ArrayList<>();

    if (max > 0)
    {
      queueHelperBatch.drainTo(list, max);
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
