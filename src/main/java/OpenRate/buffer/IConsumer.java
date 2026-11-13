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
