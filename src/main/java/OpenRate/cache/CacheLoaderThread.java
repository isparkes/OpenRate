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

import OpenRate.OpenRate;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.utils.ConversionUtils;

/**
 * Runnable container for threaded resource loading.
 *
 * @author tgdspia1
 */
public class CacheLoaderThread extends Thread
{
  private String           cacheName;
  private ICacheable       cacheableObject;
  private String           resourceName;
  private long             loadStartTime;
  private long             loadEndTime;
  private long             loadTime;

  /**
   * Constructor for creating the loader thread.
   *
   * @param tmpGrpResource The thread group we assign to.
   * @param tmpResourceName The resource we are creating for.
   */
  public CacheLoaderThread(ThreadGroup tmpGrpResource, String tmpResourceName)
  {
    super(tmpGrpResource,tmpResourceName);
  }

  /**
   * Setter for the name of the cache. Used to get configuration properties.
   *
   * @param cacheName The name of the cache
   */
  public void setCacheName(String cacheName)
  {
    this.cacheName = cacheName;
  }

 /**
  * Runs the loading
  */
  @Override
  public void run()
  {
    ICacheable   tmpCacheableIntf;
    ICacheLoader tmpCacheLoaderIntf;

    // set the handler
    tmpCacheableIntf = (ICacheable)cacheableObject;

    // Try to load the cache
    try
    {
      tmpCacheLoaderIntf = (ICacheLoader)cacheableObject;
      tmpCacheLoaderIntf.loadCache(resourceName, cacheName);
    }
    catch (InitializationException ie)
    {
      // report the exception up
      OpenRate.getFrameworkExceptionHandler().reportException(ie);
    }

    // Get the load end time
    loadEndTime = ConversionUtils.getConversionUtilsObject().getCurrentUTCms();

    // Calculate the load time
    loadTime = loadEndTime - loadStartTime;

    // display it
    OpenRate.getOpenRateFrameworkLog().info("Loaded  Cacheable Class <" + cacheName + "> in <" + loadTime + "ms>...");
    System.out.println("    Loaded  Cacheable Class <" + cacheName + "> in <" + loadTime + "ms>...");
  }

  /**
   * Setter for the cacheable object.
   *
   * @param cacheableObject
   */
  public void setCacheObject(ICacheable cacheableObject)
  {
    this.cacheableObject = cacheableObject;
  }

  /**
   * Setter for the name of the resource we are managing. Needed to access the
   * properties configuration.
   *
   * @param resourceName The resource name
   */
  public void setResourceName(String resourceName)
  {
    this.resourceName = resourceName;
  }

  /**
   * Setter for the load start time.
   *
   * @param loadStartTime The loading start time
   */
  public void setLoadStartTime(long loadStartTime)
  {
    this.loadStartTime = loadStartTime;
  }
}