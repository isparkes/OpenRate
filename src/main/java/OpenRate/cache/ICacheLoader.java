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

import OpenRate.exception.InitializationException;


/**
 * Interface that declares the behaviour of cache loaders.
 * A cacheable object implementing this interface means, that
 * the object is cacheable and loads its cache at the
 * Initilization time.
 *
 */
public interface ICacheLoader
{
 /**
  * This method has to load all the data for the cacheable object
  * into the cache.
  *
  * @param ResourceName The resource name we are loading for
  * @param CacheName The cache name we are loading for
  * @throws OpenRate.exception.InitializationException
  */
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException;
}
