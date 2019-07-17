

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.MultipleValidityCache;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.List;

/**
 * This class looks up which periods of validity cover the given key at a
 * given date and time. It is possible to locate the first match, or all
 * matches.
 *
 */
public abstract class AbstractMultipleValidityMatch extends AbstractPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMV = null;

  /**
   * The validity segment cache
   */
  protected MultipleValidityCache MV;

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
    MV = (MultipleValidityCache)CMV.get(CacheObjectName);

    if (MV == null)
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
  * @param group The regular expression group to search
  * @param resourceID The resource id to get the match for
  * @param eventTime the UTC event date to match for
  * @return The returned value, or NOMATCH if none was found
  */
  public String getFirstValidityMatch(String group, String resourceID, long eventTime)
  {
    return MV.getFirstValidityMatch(group, resourceID, eventTime);
  }

 /**
  * This returns the regular expression match
  *
  * @param group The regular expression group to search
  * @param resourceID The resource ID to search for
  * @param EventTime The UTC event time to search at
  * @return The returned value, or NOMATCH if none was found
  */
  public ArrayList<String> getFirstValidityMatchWithChildData(String group, String resourceID, long eventTime)
  {
    return MV.getFirstValidityMatchWithChildData(group, resourceID, eventTime);
  }

 /**
  * This returns the validity segment match
  *
  * @param group The regular expression group to search
  * @param resourceID The resource id to get the match for
  * @param eventTime the UTC event date to match for
  * @return The returned value, or NOMATCH if none was found
  */
  public ArrayList<String> getAllValidityMatches(String group, String resourceID, long eventTime)
  {
    return MV.getAllValidityMatches(group, resourceID, eventTime);
  }

 /**
  * This returns the regular expression match
  *
  * @param group The regular expression group to search
  * @param resourceID The resource ID to search for
  * @param eventTime The UTC event time to search at
  * @return The returned value, or NOMATCH if none was found
  */
  public ArrayList<ArrayList<String>> getAllValidityMatchesWithChildData(String group, String resourceID, long eventTime)
  {
    return MV.getAllValidityMatchesWithChildData(group, resourceID, eventTime);
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidMultipleValidityMatchResult(List<?> resultToCheck)
  {
    // check the outer container
    if (resultToCheck == null || resultToCheck.isEmpty())
    {
      return false;
    }

    // if there is an inner container, check it
    if (resultToCheck.get(0) instanceof List)
    {
      List tmpResult = (List) resultToCheck.get(0);

      if ( tmpResult == null || tmpResult.isEmpty())
      {
        return false;
      }

      if ( tmpResult.get(0).equals(MultipleValidityCache.NO_VALIDITY_MATCH))
      {
        return false;
      }
    }
    else
    {
      // No inner container - just check the value we have
      if ( resultToCheck.isEmpty())
      {
        return false;
      }

      if ( resultToCheck.get(0).equals(MultipleValidityCache.NO_VALIDITY_MATCH))
      {
        return false;
      }
    }

    return true;
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidMultipleValidityMatchResult(String resultToCheck)
  {
    if ( resultToCheck == null)
    {
      return false;
    }

    if (resultToCheck.equalsIgnoreCase(MultipleValidityCache.NO_VALIDITY_MATCH))
    {
      return false;
    }

    return true;
  }
}
