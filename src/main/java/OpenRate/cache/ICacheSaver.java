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


package OpenRate.cache;

import OpenRate.exception.ProcessingException;

/**
 * Interface that declares the behaviour of cache loaders.
 * A cacheable object implementing this interface means, that
 * the object is cacheable and loads its cache at the
 * Initialization time.
 *
 */
public interface ICacheSaver
{
  /**
  * This method has to save all the data for the cacheable object
  * into from the cache back to persistent storage, using the persisting target
  * that we set in setCachePersistDestination
  */
  public void saveCache() throws ProcessingException;
}
