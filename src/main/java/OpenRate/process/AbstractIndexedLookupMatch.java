/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.IndexedLookupCache;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;

/**
 * Now that we have the candidate that we want to work on, we now want to
 * perform the calculation on the counters.
 *
 * We do this using the balance group information from the account and the
 * logic definition.
 *
 */
public abstract class AbstractIndexedLookupMatch
  extends AbstractPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CM = null;

  /**
   * The indexed lookup cache object
   */
  protected IndexedLookupCache ILC;

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

    super.init(PipelineName, ModuleName);

   // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
                                                           ModuleName,
                                                           "DataCache");
    CM = CacheFactory.getGlobalManager(CacheObjectName);

    if (CM == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays
    ILC = (IndexedLookupCache)CM.get(CacheObjectName);

    if (ILC == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
   }

  /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting. In this case we have to open a new
  * dump file each time a stream starts.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    return r;
  }

  /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished. In this example, all we do is
  * pass the control back to the transactional layer.
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    return r;
  }

// -----------------------------------------------------------------------------
// ---------------------- Start of exposed functions ---------------------------
// -----------------------------------------------------------------------------

 /**
  * Get an entry from the cache using the appropriate index and value
  *
  * @param IndexNumber The idnex to use
  * @param Value The value to sear for in the index
  * @return The entry
  * @throws ProcessingException
  */
  public String[] getEntry(int IndexNumber, String Value) throws ProcessingException
  {
    return (String[]) ILC.getEntry(IndexNumber, Value);
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidIndexedLookupMatchResult(String[] resultToCheck)
  {
    if ( resultToCheck == null )
    {
      return false;
    }

    if ( resultToCheck[0].equals(IndexedLookupCache.NO_INDEXED_MATCH))
    {
      return false;
    }

    return true;
  }
}
