

package OpenRate.process;

import OpenRate.cache.BestMatchCache;
import OpenRate.cache.ICacheManager;
import OpenRate.exception.InitializationException;
import OpenRate.lang.DigitTree;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;

/**
 * Please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Best_Match'>click here</a> to go to wiki page.
 * <br>
 * <p>
 * This class is an example of a plug in that does only a lookup, and thus
 * does not need to be registered as transaction bound. Recall that we will
 * only need to be transaction aware when we need some specific information
 * from the transaction management (e.g. the base file name) or when we
 * require to have the possibility to undo transaction work in the case of
 * some failure.
 *
 * In this case we do not need it, as the input and output adapters will roll
 * the information back for us (by removing the output stream) in the case of
 * an error.
 */
public abstract class AbstractBestMatch
  extends AbstractPlugIn
{
  // get the Cache manager for the zone map
  // We assume that there is one cache manager for
  // the zone, time and service maps, just to simplify
  // the configuration a bit

  // This is the object will be using the find the cache manager
  private ICacheManager CMB = null;

  // The zone model object
  private BestMatchCache BM;

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

    CMB = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMB == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    // Load up the mapping arrays
    BM = (BestMatchCache)CMB.get(CacheObjectName);

    if (BM == null)
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
  * Return the time zone for a date passed as a string.
  *
  * @param mapGroup The ID of the service for this match
  * @param prefix The destination for this match
  * @return The best match zone result
  */
  public String getBestMatch(String mapGroup, String prefix)
  {
    return BM.getMatch(mapGroup, prefix);
  }

 /**
  * Return the time zone for a date passed as a string.
  *
  * @param mapGroup The ID of the service for this match
  * @param prefix The destination for this match
  * @return The best match zone result as an ArrayList
  */
  public ArrayList<String> getBestMatchWithChildData(String mapGroup, String prefix)
  {
    return  BM.getMatchWithChildData(mapGroup, prefix);
  }

 /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidBestMatchResult(ArrayList<String> resultToCheck)
  {
    if ( resultToCheck == null || resultToCheck.isEmpty())
    {
      return false;
    }

    if ( resultToCheck.get(0).equals(DigitTree.NO_DIGIT_TREE_MATCH))
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
  public boolean isValidBestMatchResult(String resultToCheck)
  {
    if ( resultToCheck == null)
    {
      return false;
    }

    if (resultToCheck.equalsIgnoreCase(DigitTree.NO_DIGIT_TREE_MATCH))
    {
      return false;
    }

    return true;
  }
}
