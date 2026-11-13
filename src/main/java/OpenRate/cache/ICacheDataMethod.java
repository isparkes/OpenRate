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

import java.util.ArrayList;
import java.util.List;

/**
 * Interface used by the caching system for cacheable objects, which are read
 * by the "Method" cache interface. This interface defines the basic function
 * which each Data Layer object must adhere to in order that it can interact
 * with the cache objects.
 */
public interface ICacheDataMethod
{
 /**
  * Use this delimiter to delimit key for a cacheable object
  * if it is formed with multiple attributes.
  *
  * @param CacheName The cache name we are loading for
  * @param MethodName The name of the method to use
  * @return The recovered data from the method
  */
  public List<ArrayList<String>> getCacheDataFromMethod(String CacheName, String MethodName);
}
