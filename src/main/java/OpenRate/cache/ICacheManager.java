

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
