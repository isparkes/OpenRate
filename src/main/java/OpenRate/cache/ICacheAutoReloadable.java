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
 * Interface used by the caching system for ICacheAutoReloadable
 * objects.
 */
public interface ICacheAutoReloadable
{
  /**
   * Get the period (seconds) of the cache auto reloading
   *
   * @return The configured period
   */
  long getAutoReloadPeriod();

  /**
   * Set the period (seconds) of the cache auto reloading
   *
   * @param autoReloadPeriod The new period in seconds
   */
  void setAutoReloadPeriod(long autoReloadPeriod);

  /**
   * Get the last time that this cache was reloaded
   *
   * @return The last reload time
   */
  long getLastReloadUTC();

 /**
  * Set the time that this cache was last reloaded
  *
  * @param lastReloadUTC The reload time
  */
  void setLastReloadUTC(long lastReloadUTC);

  /**
   * See if this cache has been excluded from auto reloading
   *
   * @return true if excluded, otherwise false
   */
  boolean getExcludeFromAutoReload();
}
