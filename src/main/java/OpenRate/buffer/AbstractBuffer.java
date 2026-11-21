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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;


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
  // this set contains all of the monitors to this buffer. These will
  // be notified when new records are ready.
  //
  // Copy on write, so that notifyMonitors() does not have to hold a lock
  // while it calls into the monitors. Each monitor takes its own lock to do
  // its notifyAll(), so holding ours across that call meant every push in
  // the pipeline queued behind one monitor, and it set up a lock ordering
  // (buffer then monitor) that nothing else may ever take the other way
  // round. Monitors register during initialisation and never after, so the
  // copy on write costs nothing at run time.
  private final Set<IMonitor> monitors = new CopyOnWriteArraySet<>();

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
    for (IMonitor m : monitors)
    {
      m.notify(BufferEvent.NEW_RECORDS);
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
    monitors.add(m);
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
