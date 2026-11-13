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


package OpenRate.resource;

import OpenRate.exception.InitializationException;

/**
 * This interface can be implemented by objects that maintain
 * static resources requiring some "finalization" type
 * behavior. Since the JVM won't commit to calling finalize()
 * :-) I'm providing a hook where the object can be registered
 * w/ the application framework. All registered Resources will
 * have cleanup() called as part of application shutdown.
 *
 */
public interface IResource
{
  /**
   * Perform whatever initialization is required of the resource.
   *
   * @param ResourceName The name of the resource in the properties
   * @throws OpenRate.exception.InitializationException
   */
  public void init(String ResourceName) throws InitializationException;

 /**
  * return the symbolic name
  *
  * @return The module symbolic name
  */
  public String getSymbolicName();

 /**
  * Perform whatever cleanup is required of the underlying object..
  */
  public void close();
}

