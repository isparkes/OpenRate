

package OpenRate.cache;

import OpenRate.exception.ProcessingException;

/**
 * Interface that declares the behaviour of cache loaders.
 * A cacheable object implementing this interface means, that
 * the object is cacheable and loads its cache at the
 * Initialization time.
 *
 */
public interface ICacheSaver
{
  /**
  * This method has to save all the data for the cacheable object
  * into from the cache back to persistent storage, using the persisting target
  * that we set in setCachePersistDestination
  */
  public void saveCache() throws ProcessingException;
}
