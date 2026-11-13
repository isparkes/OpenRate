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
 * Interface for managing the cache.  This interface will
 * declare the get/set of cacheable objects into the cache.
 *
 */
public interface ICacheManager
{
  /**
   * The gives abstraction to the put method in all
   * cache managers, to add the cacheable objects to
   * different types of cache.
   *
   * @param cacheManagerKey The key to store the cache manager with
   * @param cachedObject The cache manager to store
   * @throws InitializationException
   */
  public void put(String cacheManagerKey, ICacheable cachedObject)
           throws InitializationException;

  /**
   * The gives abstraction to the get method in all
   * cache managers, to obtain the cacheable objects from
   * different types of cache.
   *
   * @param cacheManagerKey The key of the cache manager to recover
   * @return The cache manager
   */
  public ICacheable get(String cacheManagerKey);
}
