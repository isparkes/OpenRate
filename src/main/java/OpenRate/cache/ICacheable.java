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

import OpenRate.exception.ExceptionHandler;


/**
 * Interface used by the caching system for cacheable
 * objects.
 */
public interface ICacheable
{
  /**
   * Use this delimiter to delimit key for a cacheable object
   * if it is formed with multiple attributes.
   */
  public static final String MULTIPLE_KEY_SEPARATOR = "_";

 /**
  * Set the exception handler for this cache class.
  *
  * @param handler The exception handler to set
  */
  public void setHandler(ExceptionHandler handler);
}
