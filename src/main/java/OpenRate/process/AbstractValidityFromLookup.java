package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;
import OpenRate.cache.ValidityFromCache;
import java.util.ArrayList;

/**
 * Number Portability (NP) lookup based on a series of validity segments, each 
 * valid from a start date until the next segment starts, or if none is defined,
 * valid indefinitely.
 * 
 * @author Ian
 */
public abstract class AbstractValidityFromLookup
        extends AbstractPlugIn {

  // This is the object will be using the find the cache manager

  private ICacheManager CM = null;

  // The zone model object
  private ValidityFromCache NPC;

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------
  /**
   * Initialise the module. Called during pipeline creation to initialise: -
   * Configuration properties that are defined in the properties file. - The
   * references to any cache objects that are used in the processing - The
   * symbolic name of the module
   *
   * @param PipelineName The name of the pipeline this module is in
   * @param ModuleName The name of this module in the pipeline
   * @throws OpenRate.exception.InitializationException
   */
  @Override
  public void init(String PipelineName, String ModuleName)
          throws InitializationException {
    // Variable for holding the cache object name
    String CacheObjectName;

    super.init(PipelineName, ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
            ModuleName,
            "DataCache");
    CM = CacheFactory.getGlobalManager(CacheObjectName);

    if (CM == null) {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // Load up the mapping arrays
    NPC = (ValidityFromCache) CM.get(CacheObjectName);

    if (NPC == null) {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message, getSymbolicName());
    }
  }

  /**
   * This is called when the synthetic Header record is encountered, and has the
   * meaning that the stream is starting. In this case we have to open a new
   * dump file each time a stream starts.
   *
   * @return
   */
  @Override
  public IRecord procHeader(IRecord r) {
    return r;
  }

  /**
   * This is called when the synthetic trailer record is encountered, and has
   * the meaning that the stream is now finished. In this example, all we do is
   * pass the control back to the transactional layer.
   *
   * @return
   */
  @Override
  public IRecord procTrailer(IRecord r) {
    return r;
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------
  /**
   * This returns the regular expression match
   *
   * @param Group The regular expression group to search
   * @param resourceId
   * @param time
   * @return The returned value, or NOMATCH if none was found
   */
  public String getValiditySegmentMatch(String Group, String resourceId, long time) {
    return NPC.getValiditySegmentMatch(Group, resourceId, time);
  }

  /**
   * This returns the regular expression match
   *
   * @param Group The regular expression group to search
   * @param resourceId
   * @param time
   * @return The returned value, or NOMATCH if none was found
   */
  public ArrayList<String> getValiditySegmentMatchWithChildData(String Group, String resourceId, long time) {
    return NPC.getValiditySegmentMatchWithChildData(Group, resourceId, time);
  }

  /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidValidityMatchResult(ArrayList<String> resultToCheck) {
    if (resultToCheck == null || resultToCheck.isEmpty()) {
      return false;
    }

    if (resultToCheck.get(0).equals(ValidityFromCache.NO_VALIDITY_MATCH)) {
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
  public boolean isValidValidityMatchResult(String resultToCheck) {
    if (resultToCheck == null) {
      return false;
    }

    if (resultToCheck.equalsIgnoreCase(ValidityFromCache.NO_VALIDITY_MATCH)) {
      return false;
    }

    return true;

  }
}
