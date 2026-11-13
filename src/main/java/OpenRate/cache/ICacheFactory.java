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

/**
 * Interface that needs to be implemented by all Factory classes
 * for creation/retrieval of specific cache manager and
 * initializing  cache.
 *
 */
public interface ICacheFactory
{
  /**
   * If a cache manager is currently registered for the
   * provided type, return it. If not, create a new
   * CacheManager instance keyed off the type & return it.
   *
   * @param type The type of the cache manager to get
   * @return The cache manager
   */
  public ICacheManager getManager(String type);
}
