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
import java.util.Collection;

/**
 * Supplier interface is used to source records. The buffer and adapter
 * classes are the most important uses of this interface. This is used as a
 * source for records to be processed.
 */
public interface ISupplier
{
 /**
  * Get a collection of batch records from the FIFO buffer. This method will
  * wait until a group of records can be returned, thus implementations should
  * be performance aware.
  *
  * @param max The maximum number of records to pull
  * @return The records pulled
  */
  public Collection<IRecord> pull(int max);

  /**
   * This forms the chief link for the asynchronous processing infrastructure,
   * meaning that when records arrive, the downstream module is notified that
   * there is work to do. This means that dead time in the processing is
   * minimised.
   *
   * @param m The monitor to be notified
   */
  public void registerMonitor(IMonitor m);

 /**
  * Return the number of events in the buffer. To be implemented by the
  * concrete implementation.
  *
  * @return The number of events in the buffer
  */
  public int getEventCount();
}
