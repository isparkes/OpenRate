

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.ValiditySegmentCache;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * This class looks up a match from a series of validity segments (segments
 * with a from date and a to date). The segments may not overlap.
 */
public abstract class AbstractValiditySegmentMatch extends AbstractPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMV = null;

  /**
   * The validity segment cache
   */
  protected ValiditySegmentCache VS;

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

    CMV = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMV == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping array
    VS = (ValiditySegmentCache)CMV.get(CacheObjectName);

    if (VS == null)
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
  * @param Group The regualar expression group to search
  * @param ResourceID The resource id to get the match for
  * @param EventTime the UTC event date to match for
  * @return The returned value, or NOMATCH if none was found
  */
  public String getValiditySegmentMatch(String Group, String ResourceID, long EventTime)
  {
    return VS.getValiditySegmentMatch(Group, ResourceID, EventTime);
  }

 /**
  * This returns the regular expression match
  *
  * @param Group The regualar expression group to search
  * @param ResourceID The resource ID to search for
  * @param EventTime The UTC event time to search at
  * @return The returned value, or NOMATCH if none was found
  */
  public ArrayList<String> getValiditySegmentMatchWithChildData(String Group, String ResourceID, long EventTime)
  {
    return VS.getValiditySegmentMatchWithChildData(Group, ResourceID, EventTime);
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidValiditySegmentMatchResult(ArrayList<String> resultToCheck)
  {
    if ( resultToCheck == null || resultToCheck.isEmpty())
    {
      return false;
    }

    if ( resultToCheck.get(0).equals(ValiditySegmentCache.NO_VALIDITY_MATCH))
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
  public boolean isValidValiditySegmentMatchResult(String resultToCheck)
  {
    if ( resultToCheck == null)
    {
      return false;
    }

    if (resultToCheck.equalsIgnoreCase(ValiditySegmentCache.NO_VALIDITY_MATCH))
    {
      return false;
    }

    return true;
  }
}
