/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
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
      Message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
    }

    // Load up the mapping array
    NR = (NumberRangeCache)CMNR.get(CacheObjectName);

    if (NR == null)
    {
      Message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
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
