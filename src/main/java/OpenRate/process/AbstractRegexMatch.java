

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.RegexMatchCache;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Regex_Match'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * Now that we have the candidate that we want to work on, we now want to
 * perform the calculation on the counters.
 *
 * We do this using the balance group information from the acount and the
 * logic definition.
 *
 */
public abstract class AbstractRegexMatch
  extends AbstractPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CM = null;

  // The zone model object
  private RegexMatchCache RMC;

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
    RMC = (RegexMatchCache)CM.get(CacheObjectName);

    if (RMC == null)
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
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * This returns the regular expression match
  *
  * @param Group The regular expression group to search
  * @param searchParameters The array of search parameters
  * @return The returned value, or NOMATCH if none was found
  */
  public String getRegexMatch(String Group, String[] searchParameters)
  {
    return RMC.getMatch(Group,searchParameters);
  }

 /**
  * This returns the regular expression match
  *
  * @param Group The regular expression group to search
  * @param searchParameters The array of search parameters
  * @return The returned value, or NOMATCH if none was found
  */
  public ArrayList<String> getRegexMatchWithChildData(String Group, String[] searchParameters)
  {
    return RMC.getMatchWithChildData(Group,searchParameters);
  }

 /**
  * Evaluate an input against the search group. This is the generalised from
  * which you may want to create specialised versions for a defined number of
  * parameters, for reasons of performance.
  *
  * This function returns all of the entries that are matched, in priority
  * order. This is useful for aggregation processing etc.
  *
  * @param Group The Regex group to search
  * @param searchParameters The list of fields to search
  * @return List of all matches
  */
  public ArrayList<String> getAllEntries(String Group, String[] searchParameters)
  {
    return RMC.getAllEntries(Group, searchParameters);
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidRegexMatchResult(ArrayList<String> resultToCheck)
  {
    if ( resultToCheck == null || resultToCheck.isEmpty())
    {
      return false;
    }

    if ( resultToCheck.get(0).equals(RegexMatchCache.NO_REGEX_MATCH))
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
  public boolean isValidRegexMatchResult(String resultToCheck)
  {
    if ( resultToCheck == null)
    {
      return false;
    }

    if (resultToCheck.equalsIgnoreCase(RegexMatchCache.NO_REGEX_MATCH))
    {
      return false;
    }

    return true;

  }
}
