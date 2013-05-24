/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * Tiger Shore Management or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */

package OpenRate.cache;

import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.utils.ConversionUtils;

/**
 * Runnable container for threaded resource loading.
 *
 * @author tgdspia1
 */
public class CacheLoaderThread extends Thread
{
  private String           cacheName;
  private ExceptionHandler handler;
  private ICacheable       cacheableObject;
  private String           resourceName;
  private ILogger          FWLog;
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
   * Setter for the exception handler. Used to pass up exceptions for
   * correct management.
   *
   * @param handler The exception handler.
   */
  public void setExceptionHandler(ExceptionHandler handler)
  {
    this.handler = handler;
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
    tmpCacheableIntf.setHandler(handler);

    // Try to load the cache
    try
    {
      tmpCacheLoaderIntf = (ICacheLoader)cacheableObject;
      tmpCacheLoaderIntf.loadCache(resourceName, cacheName);
    }
    catch (InitializationException ie)
    {
      // report the exception up
      handler.reportException(ie);
    }

    // Get the load end time
    loadEndTime = ConversionUtils.getConversionUtilsObject().getCurrentUTCms();

    // Calculate the load time
    loadTime = loadEndTime - loadStartTime;

    // display it
    FWLog.info("Loaded  Cacheable Class <" + cacheName + "> in <" + loadTime + "ms>...");
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
   * Setter for the Framework Log.
   *
   * @param FWLog The framework log
   */
  public void setLog(ILogger FWLog)
  {
    this.FWLog = FWLog;
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