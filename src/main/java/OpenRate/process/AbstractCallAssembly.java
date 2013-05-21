/* ====================================================================
 * Limited Evaluation License:
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * Tiger Shore Management or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */

package OpenRate.process;

import OpenRate.resource.CacheFactory;
import OpenRate.cache.CallAssemblyCache;
import OpenRate.cache.ICacheManager;
import OpenRate.exception.InitializationException;
import OpenRate.lang.AssemblyCtx;
import OpenRate.utils.PropertyUtils;

/**
 * This class provides the infrastructure for performing call assembly, which is
 * the process of collecting and aggregating partial records of long calls or
 * contexts.
 */
public abstract class AbstractCallAssembly extends AbstractStubPlugIn
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractCallAssembly.java,v $, $Revision: 1.15 $, $Date: 2013-05-13 18:12:10 $";

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
      Message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
    }

    // Load up the mapping arrays
    AssemblyDB = (CallAssemblyCache) CMP.get(CacheObjectName);

    if (AssemblyDB == null)
    {
      Message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
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
