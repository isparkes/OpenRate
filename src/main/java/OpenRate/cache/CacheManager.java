

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
