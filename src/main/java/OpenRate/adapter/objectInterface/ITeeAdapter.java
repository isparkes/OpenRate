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


package OpenRate.adapter.objectInterface;

import OpenRate.record.IRecord;
import java.util.Collection;

/**
 * The ITeeAdapter interface is used to pass a batch of records to the
 * transport layer. The transport can be anything (e.g. socket, buffer), so this
 * forms the interface from the management layer to the transport layer.
 *
 * @author ian
 */
public interface ITeeAdapter
{
 /**
  * Push the collected batch of records into the transport layer
  *
  * @param batchToPush The batch we are pushing
  */
  public void pushTeeBatch(Collection<IRecord> batchToPush);
}
