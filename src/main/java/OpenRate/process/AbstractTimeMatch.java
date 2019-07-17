
package OpenRate.process;

import OpenRate.cache.ICacheManager;
import OpenRate.cache.TimeModelCache;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.record.TimePacket;
import OpenRate.resource.CacheFactory;
import OpenRate.utils.ConversionUtils;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * This class wraps the time model cache ready for use in a processing pipeline,
 * providing access methods for conversions of date formats. The cache itself
 * works with UTC dates, so the major work of this module is to prepare the data
 * we receive in the record ready for use in the cache.
 */
public abstract class AbstractTimeMatch
        extends AbstractPlugIn {
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
          throws InitializationException {
    // Variable for holding the cache object name
    String CacheObjectName;

    super.init(PipelineName, ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValueDef(PipelineName,
            ModuleName,
            "DataCache",
            "None");

    // Check that we have something we can use
    if (CacheObjectName.equalsIgnoreCase("None")) {
      message = "Could not find data cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // Try to access the cache object
    CTM = CacheFactory.getGlobalManager(CacheObjectName);

    if (CTM == null) {
      message = "Could not find cache manager for <" + CacheObjectName + ">";
      throw new InitializationException(message, getSymbolicName());
    }

    // Load up the mapping arrays
    TM = (TimeModelCache) CTM.get(CacheObjectName);

    if (TM == null) {
      message = "Could not find cache for <" + CacheObjectName + ">";
      throw new InitializationException(message, getSymbolicName());
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
   *
   * @return
   */
  @Override
  public IRecord procHeader(IRecord r) {
    return r;
  }

  /**
   * This is called when the synthetic trailer record is encountered, and has
   * the meaning that the stream is now finished.
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
   * Set the date format
   *
   * @param DateFormat The new date format
   */
  public void setDateFormat(String DateFormat) {
    // Initialise variables that we will be using regularly
    conv.setInputDateFormat(DateFormat);
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
  public String getTimeZone(String TimeModel, long CDRDate) {
    int TMDayOfWeek;
    int TMTime;
    String TimeValue;

    TMDayOfWeek = conv.getDayOfWeek(CDRDate) - 1;
    TMTime = conv.getMinuteOfDay(CDRDate);
    TimeValue = TM.getEntry(TimeModel, TMDayOfWeek, TMTime);

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
  public String getTimeZone(String TimeModel, Date CDRDate) {
    long tmpCDRDate;
    String TimeValue;

    tmpCDRDate = CDRDate.getTime() / 1000;

    // use the base method
    TimeValue = getTimeZone(TimeModel, tmpCDRDate);

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
  public ArrayList<TimePacket> getTimeZone(String TimeModel, long CDRStartDate, long CDREndDate) {
    return TM.getEntry(TimeModel, CDRStartDate, CDREndDate, tmpCal);
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
  public ArrayList<TimePacket> getTimeZone(String TimeModel, Date CDRStartDate, Date CDREndDate) {
    long tmpCDRStartDate;
    long tmpCDREndDate;

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
  public ArrayList<TimePacket> getTimeZone(String TimeModel, Date CDRStartDate, int Duration) {
    long tmpCDRStartDate;
    long tmpCDREndDate;

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
  public ArrayList<TimePacket> getTimeZone(String TimeModel, Date CDRStartDate, double Duration) {
    long tmpCDRStartDate;
    long tmpCDREndDate;

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
  public ArrayList<TimePacket> getTimeZone(String TimeModel, long CDRStartDate, int Duration) {
    long tmpCDREndDate;

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
  public ArrayList<TimePacket> getTimeZone(String TimeModel, long CDRStartDate, double Duration) {
    long tmpCDREndDate;

    tmpCDREndDate = CDRStartDate + Math.round(Duration);

    return getTimeZone(TimeModel, CDRStartDate, tmpCDREndDate);
  }

  /**
   * checks if the lookup result is valid or not
   *
   * @param resultToCheck The result to check
   * @return true if the result is valid, otherwise false
   */
  public boolean isValidTimeMatchResult(ArrayList<String> resultToCheck) {
    if (resultToCheck == null || resultToCheck.isEmpty()) {
      return false;
    }

    if (resultToCheck.get(0).equals(TimeModelCache.NO_TIME_MATCH)) {
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
  public boolean isValidTimeMatchResult(String resultToCheck) {
    if (resultToCheck == null) {
      return false;
    }

    if (resultToCheck.equalsIgnoreCase(TimeModelCache.NO_TIME_MATCH)) {
      return false;
    }

    return true;
  }
}
