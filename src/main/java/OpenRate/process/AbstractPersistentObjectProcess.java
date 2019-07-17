

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.PersistentIndexedObject;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.Set;

/**
 * This class is a bit of a fat Filter, doing all of the Pollux logic evaluation
 * and selection in one go. It uses information from three cache objects
 * to evaluate the logic candidate to use for the rating
 */
public abstract class AbstractPersistentObjectProcess extends AbstractStubPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMP = null;

  // The zone model object
  private PersistentIndexedObject ObjectDB;

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
    String CacheObjectName;

    // Do the inherited work, e.g. setting the symbolic name etc
    super.init(PipelineName,ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
                                                           ModuleName,
                                                           "DataCache");

    CMP = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMP == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays
    ObjectDB = (PersistentIndexedObject)CMP.get(CacheObjectName);

    if (ObjectDB == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

  /**
  * This is called when a data record is encountered. You should do any normal
  * processing here.
  */
  @Override
  public abstract IRecord procValidRecord(IRecord r);

  /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  */
  @Override
  public abstract IRecord procErrorRecord(IRecord r);

  // -----------------------------------------------------------------------------
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Get an object from the cache
  *
  * @param ObjectKey The index of the object
  * @return The object
  */
  public Object getObject(String ObjectKey)
  {
    return ObjectDB.getObject(ObjectKey);
  }

 /**
  * Delete and object from the cache
  *
  * @param ObjectKey The index of the object
  */
  public void deleteObject(String ObjectKey)
  {
    ObjectDB.deleteObject(ObjectKey);
  }

 /**
  * Put an object into the cache
  *
  * @param ObjectKey The index of the object
  * @param objectToPut The object to put
  */
  public void putObject(String ObjectKey, Object objectToPut)
  {
    ObjectDB.putObject(ObjectKey, objectToPut);
  }

 /**
  * See if an key exists in the cache
  *
  * @param ObjectKey The object key to find
  * @return true if the object exists otherwise false
  */
  public boolean containsObjectKey(String ObjectKey)
  {
    return ObjectDB.containsObjectKey(ObjectKey);
  }

   /**
  * Get the key set for the cache, used for iterating over it
  *
  * @return the object key set
  */
  public Set<String> getObjectKeySet()
  {
    return ObjectDB.getObjectKeySet();
  }

}
