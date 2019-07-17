

package OpenRate.cache;

import OpenRate.exception.InitializationException;


/**
 * Interface that declares the behaviour of cache loaders.
 * A cacheable object implementing this interface means, that
 * the object is cacheable and loads its cache at the
 * Initilization time.
 *
 */
public interface ICacheLoader
{
 /**
  * This method has to load all the data for the cacheable object
  * into the cache.
  *
  * @param ResourceName The resource name we are loading for
  * @param CacheName The cache name we are loading for
  * @throws OpenRate.exception.InitializationException
  */
  public void loadCache(String ResourceName, String CacheName)
                 throws InitializationException;
}
