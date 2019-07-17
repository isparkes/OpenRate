

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.NumberRangeCache;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * This class looks up a number out of a series of ranges (from - to). The
 * ranges may not overlap.
 */
public abstract class AbstractNumberRangeMatch extends AbstractPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMNR = null;

  /**
   * The number range cache object
   */
  protected NumberRangeCache NR;

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

    CMNR = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMNR == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping array
    NR = (NumberRangeCache)CMNR.get(CacheObjectName);

    if (NR == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * Loop through for the header ...
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
    return r;
  }

 /**
  * ... and trailer
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
  * This returns the validity segment match
  *
  * @param Group The regular expression group to search
  * @param rangeSearchValue The value to search for in the ranges
  * @param EventTime the UTC event date to match for
  * @return The returned value, or NOMATCH if none was found
  */
  public String getNumberRangeMatch(String Group, long rangeSearchValue, long EventTime)
  {
    return NR.getEntry(Group, rangeSearchValue, EventTime);
  }

 /**
  * This returns the regular expression match
  *
  * @param Group The regular expression group to search
  * @param rangeSearchValue The value to search for in the ranges
  * @param EventTime The UTC event time to search at
  * @return The returned value, or NOMATCH if none was found
  */
  public ArrayList<String> getNumberRangeMatchWithChildData(String Group, long rangeSearchValue, long EventTime)
  {
    return NR.getEntryWithChildData(Group, rangeSearchValue, EventTime);
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidNumberRangeMatchResult(ArrayList<String> resultToCheck)
  {
    if ( resultToCheck == null || resultToCheck.isEmpty())
    {
      return false;
    }

    if ( resultToCheck.get(0).equals(NumberRangeCache.NO_RANGE_MATCH))
    {
      return false;
    }

    return true;
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidNumberRangeMatchResult(String resultToCheck)
  {
    if ( resultToCheck == null)
    {
      return false;
    }

    if (resultToCheck.equalsIgnoreCase(NumberRangeCache.NO_RANGE_MATCH))
    {
      return false;
    }

    return true;
  }
}
