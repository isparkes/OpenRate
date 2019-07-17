

package OpenRate.cache;

import OpenRate.exception.InitializationException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A shared object which can be created as a resource.  This object can then be
 * retrieved from the ResourceContext and used to share data between threads.
 *
 * Because of this, we define the hash as a concurrent hash.
 */
public class TransientHash extends AbstractCache implements ICacheLoader
{
  /**
   * Hash map of data that is to be shared between modules.
   */
  protected ConcurrentHashMap<String,String> sharedHash = null;

 /**
  * Initalization of the transient object store
  *
  * @param ResourceName The resource name we are loading for
  * @param CacheName The cache name we are loading for
  * @throws InitializationException
  */
  @Override
  public void loadCache(String ResourceName, String CacheName)
          throws InitializationException
  {
    // Set the symbolic name
    setSymbolicName(CacheName);

    // initialise the hash object
    sharedHash = new ConcurrentHashMap<>();
  }

  /**
   * Set the shared data to null for cleanup.
   */
  public void close()
  {
    sharedHash.clear();
  }

  /**
   * Returns the sharedData.
   *
   * @return The shared data
   */
  public synchronized ConcurrentHashMap<String, String> getSharedData()
  {
    return sharedHash;
  }

  /**
   * Sets the sharedData.
   *
   * @param sharedData The shared data object
   */
  public synchronized void setSharedData(ConcurrentHashMap<String,String> sharedData)
  {
    this.sharedHash = sharedData;
  }

 /**
  * Puts a string into the hash
  *
  * @param Key The key to use
  * @param Value The value to store
  */
  public void putString(String Key, String Value)
  {
    sharedHash.put(Key, Value);
  }

 /**
  * Sees if the hash contains a key
  *
  * @param Key The key to search for
  * @return true if the hash contains the key, otherwise false
  */
  public boolean containsKey(String Key)
  {
    return sharedHash.containsKey(Key);
  }

 /**
  * Gets a value from the hash
  *
  * @param Key The key to retrieve the value for
  * @return The retrieved value
  */
  public String getString(String Key)
  {
    return sharedHash.get(Key);
  }

 /**
  * Clears the hash
  */
  public void clear()
  {
    sharedHash.clear();
  }
}
