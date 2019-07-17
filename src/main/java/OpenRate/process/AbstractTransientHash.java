

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.TransientHash;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;

/**
 * This module provides access to a shared transient hash object.
 *
 */
public abstract class AbstractTransientHash
  extends AbstractPlugIn
{
  // get the Cache manager for the zone map
  // We assume that there is one cache manager for
  // the zone, time and service maps, just to simplify
  // the configuration a bit

  // This is the object will be using the find the cache manager
  private ICacheManager CMCH = null;

  // The zone model object
  private TransientHash CH;

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    // Variable for holding the cache object name
    String CacheObjectName;

    super.init(PipelineName,ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
                                                           ModuleName,
                                                           "DataCache");

    CMCH = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMCH == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + "> in module <" + getSymbolicName() + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays
    CH = (TransientHash)CMCH.get(CacheObjectName);

    if (CH == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {

    return r;
  }

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished.
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {

    return r;
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Return whether the hash map contains the key
  *
  * @param Key The key to look for
  * @return true if the hash contains the key, otherwise false
  */
  public boolean containsKey(String Key)
  {
    return CH.containsKey(Key);
  }

 /**
  * Puts a string into the hash
  *
  * @param Key The key to use
  * @param Value The value to store
  */
  public void putString(String Key, String Value)
  {
    CH.putString(Key, Value);
  }

 /**
  * Gets a value from the hash
  *
  * @param Key The key to retrieve the value for
  * @return The retrieved value
  */
  public String getString(String Key)
  {
    return CH.getString(Key);
  }

 /**
  * Clears the hash
  */
  public void clearHash()
  {
    CH.clear();
  }
}
