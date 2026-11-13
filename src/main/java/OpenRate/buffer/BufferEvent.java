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

/**
 * BufferEvent is the class that is used to notify monitors that something
 * has happened that they should be aware of. At the moment, the only event
 * that they should be aware of is the arrival of new records to be processed.
 */
public class BufferEvent
  implements IEvent
{
  // This holds the type of event that we are representing
  private String            type = null;

 /**
  * Event corresponding to new channel records.
  */
  public static BufferEvent NEW_RECORDS = new BufferEvent("NEW_RECORDS");

 /**
  * Default Constructor for BufferEvent
  */
  private BufferEvent()
  {
    type = "NONE";
  }

 /**
  * Constructor for BufferEvent, with type given
  *
  * @param type The type of event that is being created
  */
  private BufferEvent(String type)
  {
    this.type = type;
  }

 /**
  * Provides readable representation of the object
  *
  * @return the readable Buffer Event Type
  */
  @Override
  public String toString()
  {
    return "BufferEvent: type = " + type;
  }
}
