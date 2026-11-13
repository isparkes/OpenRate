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
 * Definition of the monitor infrastructure, which triggers a notification once
 * records have been accepted by a buffer for passing on. This means that
 * the downstream module will accept the records at the earliest moment
 * possible.
 */
public interface IMonitor
{
 /**
  * Perform the notification.
  *
  * @param e The event to notify
  */
  public void notify(IEvent e);
}
