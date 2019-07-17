

package OpenRate.process;

import OpenRate.cache.CallAssemblyCache;
import OpenRate.cache.ICacheManager;
import OpenRate.exception.InitializationException;
import OpenRate.lang.AssemblyCtx;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.PropertyUtils;

/**
 * This class provides the infrastructure for performing call assembly, which is
 * the process of collecting and aggregating partial records of long calls or
 * contexts.
 */
public abstract class AbstractCallAssembly extends AbstractStubPlugIn
{
  // This is the object will be using the find the cache manager
  private ICacheManager CMP = null;

  // The assembly cache
  private CallAssemblyCache AssemblyDB;

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
    AssemblyDB = (CallAssemblyCache) CMP.get(CacheObjectName);

    if (AssemblyDB == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // ------------------- Start of custom Plug In functions -----------------------
  // -----------------------------------------------------------------------------

 /**
  * Start the call assembly of an object by opening the Context and setting
  * the state to the initialised state.
  *
  * @param CallID The unique record identifier
  * @param Duration The duration of this partial
  * @param Volume The volume of this partial
  * @param uplink The uplink volume of this partial
  * @param downlink The downlink volume of this partial
  * @param startDate The UTC start date of this partial
  * @return true if ok, otherwise false
  */
  protected boolean startAssembly(String CallID, double Duration, double Volume, double uplink, double downlink, long startDate)
  {
    AssemblyCtx newCtx;

    // see if we already have the context
    newCtx = (AssemblyCtx) AssemblyDB.getObject(CallID);

    if (newCtx == null)
    {
      newCtx = new AssemblyCtx();
      newCtx.totalDuration = Duration;
      newCtx.totalData = Volume;
      newCtx.uplink = uplink;
      newCtx.downlink = downlink;
      newCtx.StartDate = startDate;
      newCtx.state = 1;

      // store
      AssemblyDB.putObject(CallID, newCtx);

      return true;
    }
    else
    {
      return false;
    }
  }

 /**
  * Continue the call assembly of an object by updating the Context and setting
  * the state to the intermediate state.
  *
  * @param CallID The unique record identifier
  * @param Duration The duration of this partial
  * @param Volume The volume of this partial
  * @param uplink The uplink volume of this partial
  * @param downlink The downlink volume of this partial
  * @param startDate The UTC start date of this partial
  * @return true if ok, otherwise false
  */
  protected boolean continueAssembly(String CallID, double Duration, double Volume, double uplink, double downlink, long startDate)
  {
    AssemblyCtx newCtx;

    // get the existing context
    newCtx = (AssemblyCtx) AssemblyDB.getObject(CallID);

    // Update the existing context
    if (newCtx == null)
    {
      return false;
    }
    else
    {
      // see if the state is right
      if (newCtx.state == 3)
      {
        //already closed
        return false;
      }
      else
      {
        newCtx.totalDuration += Duration;
        newCtx.totalData += Volume;
        newCtx.uplink += uplink;
        newCtx.downlink += downlink;
        newCtx.state = 2;

        // update the date
        if (startDate < newCtx.StartDate)
        {
          newCtx.StartDate = startDate;
        }
      }
    }

    // store
    AssemblyDB.putObject(CallID, newCtx);

    return true;
  }

 /**
  * Finish the call assembly of an object by updating the Context and setting
  * the state to the closed state.
  *
  * @param CallID The unique record identifier
  * @param Duration The duration of this partial
  * @param Volume The volume of this partial
  * @param uplink The uplink volume of this partial
  * @param downlink The downlink volume of this partial
  * @param startDate The UTC start date of this partial
  * @return true if ok, otherwise false
  */
  protected boolean endAssembly(String CallID, double Duration, double Volume, double uplink, double downlink, long startDate)
  {
    AssemblyCtx newCtx;

    // get the existing context
    newCtx = (AssemblyCtx) AssemblyDB.getObject(CallID);

    // Update the existing context
    if (newCtx == null)
    {
      return false;
    }
    else
    {
      // see if the state is right
      if (newCtx.state == 3)
      {
        //already closed
        return false;
      }
      else
      {
        newCtx.totalDuration += Duration;
        newCtx.totalData += Volume;
        newCtx.uplink += uplink;
        newCtx.downlink += downlink;
        newCtx.state = 3;
        newCtx.ClosedDate = startDate;
      }
    }

    // store
    AssemblyDB.putObject(CallID, newCtx);

    return true;
  }

  /**
   * Get the cumulative duration so for for the call
   *
   * @param CallID The call to get
   * @return The cumulaticve duration
   */
  protected double getCumulativeDuration(String CallID)
  {
    AssemblyCtx newCtx;

    // get the existing context
    newCtx = (AssemblyCtx) AssemblyDB.getObject(CallID);

    // Update the existing context
    if (newCtx == null)
    {
      return -1;
    }
    else
    {
      return newCtx.totalDuration;
    }
  }

 /**
  * Gets the state of an assebly context:
  * 0 - not started
  * 1 - started
  * 2 - in process
  * 3 - ended
  *
  * @param CallID The Call Reference to locate the information for
  * @return The state
  */
  protected int getState(String CallID)
  {
    AssemblyCtx newCtx;

    // get the existing context
    newCtx = (AssemblyCtx) AssemblyDB.getObject(CallID);

    // Update the existing context
    if (newCtx == null)
    {
      return 0;
    }
    else
    {
      return newCtx.state;
    }
  }

 /**
  * Get the start date of the first partial
  *
  * @param CallID
  * @return The first partial start date
  */
  protected long getOrigStartDate(String CallID)
  {
    AssemblyCtx newCtx;

    // get the existing context
    newCtx = (AssemblyCtx) AssemblyDB.getObject(CallID);

    // Update the existing context
    if (newCtx == null)
    {
      return -1;
    }
    else
    {
      return newCtx.StartDate;
    }
  }
}
