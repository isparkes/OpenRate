

package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.MinFeeCache;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.IRecord;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;

/**
 *
 * This class provides the abstract base for the Min Fee Calculation.
 *
 */
public abstract class AbstractMinFeeCalc extends AbstractPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMRR = null;

  // The Min Fee object
  private MinFeeCache MFC;

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
       ModuleName,"DataCache");

    CMRR = CacheFactory.getGlobalManager(CacheObjectName);

    if (CMRR == null)
    {
      message = "Could not find cache for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    MFC = (MinFeeCache)CMRR.get(CacheObjectName);
    if (MFC == null)
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
// ----------------------- Start of utility functions --------------------------
// -----------------------------------------------------------------------------

 /**
  * applyMinFee performs the Min Fee Value Check
  *
  * @param minFeeName The name of the min fee
  * @param ratedValue The current rated value
  * @return The updated value
  * @throws ProcessingException
  */
  protected double applyMinFee(String minFeeName, double ratedValue) throws ProcessingException
  {
    String tmpMinFeeString;
    double tmpMinFeeValue;
    double returnValue = ratedValue;

    // get the min fee value for this rate plan
    tmpMinFeeString = MFC.getEntry(minFeeName);

    if(tmpMinFeeString == null)
    {
      return ratedValue;
    }

    tmpMinFeeValue = Double.valueOf(tmpMinFeeString);

    if(ratedValue < tmpMinFeeValue)
    {
      return returnValue;
    }
    else
    {
      return ratedValue;
    }
  }
}
