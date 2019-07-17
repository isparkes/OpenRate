
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
