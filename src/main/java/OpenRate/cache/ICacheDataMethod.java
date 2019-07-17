

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
