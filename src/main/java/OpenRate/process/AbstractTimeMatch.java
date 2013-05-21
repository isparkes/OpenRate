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
import OpenRate.cache.ICacheManager;
import OpenRate.cache.TimeModelCache;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.record.TimePacket;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * This class wraps the time model cache ready for use in a processing pipeline,
 * providing access methods for conversions of date formats. The cache itself
 * works with UTC dates, so the major work of this module is to prepare the
 * data we receive in the record ready for use in the cache.
 */
public abstract class AbstractTimeMatch
  extends AbstractPlugIn
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractTimeMatch.java,v $, $Revision: 1.35 $, $Date: 2013-05-13 18:12:10 $";

  // get the Cache manager for the zone map
  // We assume that there is one cache manager for
  // the zone, time and service maps, just to simplify
  // the configuration a bit

  // This is the object will be using the find the cache manager
  private ICacheManager CTM = null;

  /**
   * The time model cache object
   */
  protected TimeModelCache TM;

  // used for date calculations
    private GregorianCalendar tmpCal;

  /**
   * Variables for dealing with the CDR date
   */
  protected ConversionUtils conv;

// -----------------------------------------------------------------------------
// ------------------ Start of inherited Plug In functions ---------------------
// -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation.
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
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
                                                           ModuleName,
                                                           "DataCache",
                                                           "None");

    // Check that we have something we can use
    if (CacheObjectName.equalsIgnoreCase("None"))
    {
      Message = "Could not find data cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
    }

    // Try to access the cache object
    CTM = CacheFactory.getGlobalManager(CacheObjectName);

    if (CTM == null)
    {
      Message = "Could not find cache manager for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
    }

    // Load up the mapping arrays
    TM = (TimeModelCache)CTM.get(CacheObjectName);

    if (TM == null)
    {
      Message = "Could not find cache for <" + CacheObjectName + ">";
      throw new InitializationException(Message);
    }

    // Initialise variables that we will be using regularly - this is the
    // default that can be overwritten using "setDateFormat"
    conv = new ConversionUtils();
    conv.setInputDateFormat("yyyyMMddHHmmss");

    // Get the calendar instance
      tmpCal = (GregorianCalendar) Calendar.getInstance();
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
   * Set the date format
   *
   * @param DateFormat The new date format
   */
  public void setDateFormat(String DateFormat)
  {
    // Initialise variables that we will be using regularly
    conv.setInputDateFormat(DateFormat);
  }

 /**
  * Return the time zone for a date passed as a string.
  *
  * NO TIME SPLITTING
  *
  * @param TimeModel The ID of the time model we are using
  * @param CDRDateAsString The date of the CDR
  * @return The time zone for the model and the date
  */
  public String getTimeZone(String TimeModel, String CDRDateAsString)
  {
    long   tmpCDRDate;
    String TimeValue;

    try
    {
      tmpCDRDate = conv.convertInputDateToUTC(CDRDateAsString);
    }
    catch (ParseException ex)
    {
      pipeLog.error("error parsing date <" + CDRDateAsString + ">");
      return null;
    }

    // use the base method
    TimeValue = getTimeZone(TimeModel,tmpCDRDate);

    return TimeValue;
  }

 /**
  * Return the time zone for a date passed as a UTC value.
  *
  * NO TIME SPLITTING
  *
  * @param TimeModel The time model we are using
  * @param CDRDate The CDR date
  * @return The time zone for the model and CDR date
  */
  public String getTimeZone(String TimeModel, long CDRDate)
  {
    int    TMDayOfWeek;
    int    TMTime;
    String TimeValue;

    TMDayOfWeek = conv.getDayOfWeek(CDRDate)-1;
    TMTime = conv.getMinuteOfDay(CDRDate);
    TimeValue = TM.getEntry(TimeModel,TMDayOfWeek,TMTime);

    return TimeValue;
  }

 /**
  * Return the time zone for a date passed as a native date value.
  *
  * NO TIME SPLITTING
  *
  * @param TimeModel The time model we are using
  * @param CDRDate The CDR date
  * @return The time zone for the model and CDR date
  */
  public String getTimeZone(String TimeModel, Date CDRDate)
  {
    long   tmpCDRDate;
    String TimeValue;

    tmpCDRDate = CDRDate.getTime() / 1000;

    // use the base method
    TimeValue = getTimeZone(TimeModel,tmpCDRDate);

    return TimeValue;
  }

 /**
  * This method is used to check if a cdr is falling in between two or more
  * time zones if yes then it will create one time packet for each time zone
  * otherwise only one time packet.
  *
  * TIME SPLITTING
  *
  * @param TimeModel The time model to perform the lookup for
  * @param CDRStartDate The start date
  * @param CDREndDate The end date
  * @return The list of impacted time zones
  */
  public ArrayList<TimePacket> getTimeZone(String TimeModel, long CDRStartDate, long CDREndDate)
  {
    return TM.getEntry(TimeModel,CDRStartDate,CDREndDate,tmpCal);
  }

 /**
  * This method is used to check if a cdr is falling in between two or more
  * time zones if yes then it will create one time packet for each time zone
  * otherwise only one time packet.
  *
  * TIME SPLITTING
  *
  * @param TimeModel The time model to perform the lookup for
  * @param CDRStartDate The start date
  * @param CDREndDate The end date
  * @return The list of impacted time zones
  */
  public ArrayList<TimePacket> getTimeZone(String TimeModel, Date CDRStartDate, Date CDREndDate)
  {
    long   tmpCDRStartDate;
    long   tmpCDREndDate;

    tmpCDRStartDate = CDRStartDate.getTime() / 1000;
    tmpCDREndDate = CDREndDate.getTime() / 1000;

    return getTimeZone(TimeModel, tmpCDRStartDate, tmpCDREndDate);
  }

 /**
  * This method is used to check if a cdr is falling in between two or more
  * time zones if yes then it will create one time packet for each time zone
  * otherwise only one time packet.
  *
  * TIME SPLITTING
  *
  * @param TimeModel The time model to perform the lookup for
  * @param CDRStartDate The start date
  * @param Duration The duration of the call
  * @return The list of impacted time zones
  */
  public ArrayList<TimePacket> getTimeZone(String TimeModel, Date CDRStartDate, int Duration)
  {
    long   tmpCDRStartDate;
    long   tmpCDREndDate;

    tmpCDRStartDate = CDRStartDate.getTime() / 1000;
    tmpCDREndDate = tmpCDRStartDate + Duration;

    return getTimeZone(TimeModel, tmpCDRStartDate, tmpCDREndDate);
  }

 /**
  * This method is used to check if a cdr is falling in between two or more
  * time zones if yes then it will create one time packet for each time zone
  * otherwise only one time packet.
  *
  * TIME SPLITTING
  *
  * @param TimeModel The time model to perform the lookup for
  * @param CDRStartDate The start date
  * @param Duration The duration of the call
  * @return The list of impacted time zones
  */
  public ArrayList<TimePacket> getTimeZone(String TimeModel, Date CDRStartDate, double Duration)
  {
    long   tmpCDRStartDate;
    long   tmpCDREndDate;

    tmpCDRStartDate = CDRStartDate.getTime() / 1000;
    tmpCDREndDate = tmpCDRStartDate + Math.round(Duration);

    return getTimeZone(TimeModel, tmpCDRStartDate, tmpCDREndDate);
  }

 /**
  * This method is used to check if a cdr is falling in between two or more
  * time zones if yes then it will create one time packet for each time zone
  * otherwise only one time packet.
  *
  * TIME SPLITTING
  *
  * @param TimeModel The time model to perform the lookup for
  * @param CDRStartDate The start date
  * @param Duration The duration of the call
  * @return The list of impacted time zones
  */
  public ArrayList<TimePacket> getTimeZone(String TimeModel, long CDRStartDate, int Duration)
  {
    long   tmpCDREndDate;

    tmpCDREndDate = CDRStartDate + Duration;

    return getTimeZone(TimeModel, CDRStartDate, tmpCDREndDate);
  }

 /**
  * This method is used to check if a cdr is falling in between two or more
  * time zones if yes then it will create one time packet for each time zone
  * otherwise only one time packet.
  *
  * TIME SPLITTING
  *
  * @param TimeModel The time model to perform the lookup for
  * @param CDRStartDate The start date
  * @param Duration The duration of the call
  * @return The list of impacted time zones
  */
  public ArrayList<TimePacket> getTimeZone(String TimeModel, long CDRStartDate, double Duration)
  {
    long   tmpCDREndDate;

    tmpCDREndDate = CDRStartDate + Math.round(Duration);

    return getTimeZone(TimeModel, CDRStartDate, tmpCDREndDate);
  }
  
 /**
   * checks if the lookup result is valid or not
   * 
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidTimeMatchResult(ArrayList<String> resultToCheck)
  {
    if ( resultToCheck == null || resultToCheck.isEmpty())
    {
      return false;
    }
    
    if ( resultToCheck.get(0).equals(TimeModelCache.NO_TIME_MATCH))
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
  public boolean isValidTimeMatchResult(String resultToCheck)
  {
    if ( resultToCheck == null)
    {
      return false;
    }
    
    if (resultToCheck.equalsIgnoreCase(TimeModelCache.NO_TIME_MATCH))
    {
      return false;
    }
    
    return true;
  }
}
