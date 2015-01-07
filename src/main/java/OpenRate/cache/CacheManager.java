/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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

import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import java.util.HashMap;

/**
 * This is simple cache manager that handles the cacheable
 * objects by storing and retrieving from HashMap java class.
 * Other complex cache managers can store in different java
 * classes or third party caching mechanism like JCS etc.
 *
 */
public class CacheManager
  implements ICacheManager
{
  // Get access to the log
  private ILogger log = LogUtil.getLogUtil().getLogger("Framework");

  /**
   * This stores all the cacheable objects.
   */
  protected HashMap<String, ICacheable> cacheableClassMap;

  /**
   * Constructor. Creates the cache manager module, which is an index of the
   * caches which have been instantiated in this framework. This means we are
   * able to reference the cache managers at a later date using only the
   * name of the class, as opposed to the actual class instance.
   */
  public CacheManager()
  {
    // Initialise the manager store
    cacheableClassMap = new HashMap<>();
  }

  /**
   * This method retrieves the cacheable object from
   * cache using the key and returns the it.
   *
   * @param pkKey The search key for the cacheable object
   * @return The cacheable object
   */
  @Override
  public ICacheable get(String pkKey)
  {
    ICacheable lookUpResult;
    lookUpResult = cacheableClassMap.get(pkKey);

    return lookUpResult;
  }

  /**
   * This method adds the cacheable object to the
   * cache with its key.
   *
   * @param key The search key to store the object under
   * @param cachedObject The object to store
   * @throws InitializationException
   *
   */
  @Override
  public void put(String key, ICacheable cachedObject)
           throws InitializationException
  {
    try
    {
      cacheableClassMap.put(key, cachedObject);
    }
    catch (Exception cacheEx)
    {
      String message = "Error while putting object <"+key+"> into CacheManager";
      log.error(message);
      throw new InitializationException(message,
                                        cacheEx,
                                        "CacheManager");
    }
  }
}
