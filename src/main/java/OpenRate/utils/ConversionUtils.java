/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package OpenRate.utils;

import OpenRate.CommonConfig;
import OpenRate.resource.ConversionCache;
import OpenRate.resource.ResourceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class offers conversion and formatting methods (primarily for dates), so
 * that we have a simple single set conversion handling methods.
 *
 * @author ian
 */
public class ConversionUtils
{
  // This is the format used for converting dates on input
  private volatile String InputDateFormat = CommonConfig.OR_DEFAULT_DATE_FORMAT;

  // This is the format used for converting dates on input
  private volatile String OutputDateFormat = CommonConfig.OR_DEFAULT_DATE_FORMAT;

  // format used for input, one per thread
  private final DateFormatHolder sdfIn =
          new DateFormatHolder(CommonConfig.OR_DEFAULT_DATE_FORMAT, false);

  // format used for output, one per thread
  private final DateFormatHolder sdfOut =
          new DateFormatHolder(CommonConfig.OR_DEFAULT_DATE_FORMAT, false);

  // set that we are using integer (UTC) format
  private volatile boolean integerFormat = false;

  // set that we are using long format (UTC + ms)
  private volatile boolean longFormat = false;

  // set that we are using long format (UTC + ms)
  private volatile boolean stringFormat = true;

  // the time zone that the date utilities below work in
  private volatile TimeZone timeZone = TimeZone.getDefault();

  // used for general date manipulation, one per thread
  private final ThreadLocal<CalendarHolder> calendars = new ThreadLocal<>();

  // Used to cache access to the conversion objects
  private static volatile ConversionCache tmpConvCache = null;

 /**
  * constructor - initialise the internal formatting object
  */
  public ConversionUtils()
  {
    // Nothing to do: the formatters and the calendar are created per thread,
    // on first use, from the fields above.
  }

 /**
  * Holds a date formatter per thread.
  *
  * SimpleDateFormat is not thread safe, and a ConversionUtils object is
  * shared: the ConversionCache hands the same object to everything that asks
  * for a given type, and getConversionUtilsObject() is a process wide
  * singleton used from the pipeline worker threads. Two threads parsing at
  * the same time do not fail, they return wrong dates, so each thread gets
  * its own formatter.
  *
  * The pattern can be changed while the framework is running, so a thread
  * rebuilds its formatter whenever it finds one built for a different
  * pattern.
  */
  private static final class DateFormatHolder
  {
    private volatile String pattern;
    private volatile boolean lenient;
    private final ThreadLocal<SimpleDateFormat> perThread = new ThreadLocal<>();

    DateFormatHolder(String pattern, boolean lenient)
    {
      this.pattern = pattern;
      this.lenient = lenient;
    }

   /**
    * Change the pattern. Threads pick the change up on their next call.
    */
    void setPattern(String newPattern, boolean newLenient)
    {
      this.lenient = newLenient;
      this.pattern = newPattern;
    }

   /**
    * Get this thread's formatter for the current pattern.
    */
    SimpleDateFormat get()
    {
      String wantedPattern = pattern;
      boolean wantedLenient = lenient;
      SimpleDateFormat sdf = perThread.get();

      if ((sdf == null) ||
          (sdf.isLenient() != wantedLenient) ||
          (wantedPattern.equals(sdf.toPattern()) == false))
      {
        sdf = new SimpleDateFormat(wantedPattern);
        sdf.setLenient(wantedLenient);
        perThread.set(sdf);
      }

      return sdf;
    }
  }

 /**
  * Holds a calendar per thread, with the time zone it was last set to.
  *
  * Calendar is not thread safe either, and every date helper below works by
  * setting a time on it and reading fields back, so two threads sharing one
  * would silently read each other's dates.
  */
  private static final class CalendarHolder
  {
    private final Calendar calendar;
    private TimeZone zone;

    CalendarHolder(TimeZone zone)
    {
      this.zone = zone;
      this.calendar = new GregorianCalendar(zone);
    }
  }

 /**
  * Get this thread's calendar, set to the current time zone.
  *
  * @return The calendar to use on this thread
  */
  private Calendar getCal()
  {
    TimeZone wanted = timeZone;
    CalendarHolder holder = calendars.get();

    if (holder == null)
    {
      holder = new CalendarHolder(wanted);
      calendars.set(holder);
    }
    else if (holder.zone != wanted)
    {
      holder.calendar.setTimeZone(wanted);
      holder.zone = wanted;
    }

    return holder.calendar;
  }

 /**
  * Get the access to the singleton conversion cache
  *
  * @return Conversion Cache Reference
  */
  public static ConversionCache getConversionCache()
  {
    ConversionCache result = tmpConvCache;

    if (result == null)
    {
      synchronized (ConversionUtils.class)
      {
        result = tmpConvCache;

        if (result == null)
        {
          // Get access to the conversion cache
          ResourceContext ctx = new ResourceContext();

          // try the new Logging model.
          result = (ConversionCache) ctx.get(ConversionCache.RESOURCE_KEY);
          tmpConvCache = result;
        }
      }
    }

    return result;
  }

 /**
  * Holder for the lazily created singleton. The previous version did an
  * unsynchronised null check on a plain static field, so two threads could
  * each build an instance, and a thread could pick up a partly constructed
  * one. Class initialisation gives exactly one instance and safe
  * publication, with no lock on the common path.
  */
  private static final class SingletonHolder
  {
    private static final ConversionUtils INSTANCE = new ConversionUtils();
  }

 /**
  * Get the access to the singleton conversion utils object
  *
  * @return Conversion Cache Reference
  */
  public static ConversionUtils getConversionUtilsObject()
  {
    return SingletonHolder.INSTANCE;
  }

//------------------------------------------------------------------------------
//---------------------------- Date Utilities ----------------------------------
//------------------------------------------------------------------------------

 /**
  * Convert an amorphic string to a UTC date representation for input
  *
  * @param amorphicDate the amorphic date string to read and convert
  * @return The short UTC date format
  * @throws ParseException
  */
  public long convertInputDateToUTC(String amorphicDate) throws ParseException
  {
    long tmpUTCDate = 0;

    if (integerFormat)
    {
      return Integer.parseInt(amorphicDate);
    }
    else if (longFormat)
    {
      return Long.parseLong(amorphicDate) / 1000;
    }
    else if (stringFormat)
    {
      if (amorphicDate == null)
      {
        return 0;
      }
      else
      {
        tmpUTCDate = sdfIn.get().parse(amorphicDate).getTime() / 1000;
      }
    }

    return tmpUTCDate;
  }

 /**
  * This converts the UTC date into day of week
  *
  * @param UTCDateValue The long UTC date the convert
  * @return The integer day of the week
  */
  public int getDayOfWeek(long UTCDateValue)
  {
    getCal().setTimeInMillis(UTCDateValue*1000);
    return getCal().get(Calendar.DAY_OF_WEEK);
  }

 /**
  * Utility function to get the minute of the day from a long UTC value
  *
  * @param UTCDateValue The long UTC date the convert
  * @return The minute of the day
  */
  public int getMinuteOfDay(long UTCDateValue)
  {
    getCal().setTimeInMillis(UTCDateValue*1000);
    return getCal().get(Calendar.HOUR_OF_DAY) * 60 + getCal().get(Calendar.MINUTE);
  }

 /**
  * Sets the standard overall date format for input to a new value for this
  * converter. The allowed values here are:
  * "Integer" - take a UTC date (without milliseconds),
  * "Long" - take a UTC date with milliseconds
  * Other - any other value that can be treated as a date string using the
  * SimpleDateFormat formatting strings.
  *
  * @param newFormat The new format to adopt
  * @return True if the change was successful otherwise false
  */
  public boolean setInputDateFormat(String newFormat)
  {
    if (newFormat.equalsIgnoreCase("integer"))
    {
      // set UTC format
      integerFormat = true;
      longFormat = false;
      stringFormat = false;
    }
    else if (newFormat.equalsIgnoreCase("long"))
    {
      // set UTC long format with milliseconds
      integerFormat = false;
      longFormat = true;
      stringFormat = false;
    }
    else
    {
      // Note that, as before, a formatter built here is lenient: only the
      // ones built at construction time have leniency switched off.
      sdfIn.setPattern(newFormat, true);

      // set a string format
      integerFormat = false;
      longFormat = false;
      stringFormat = true;
    }

    // set the input date format
    InputDateFormat = newFormat;

    return true;
  }

 /**
  * Gets the standard overall date format for input
  *
  * @return The current date format string
  */
  public String getInputDateFormat()
  {
    return InputDateFormat;
  }

 /**
  * Sets the standard overall date format for input to a new value for this
  * converter. The allowed values here are:
  * "Integer" - take a UTC date (without milliseconds),
  * "Long" - take a UTC date with milliseconds
  * Other - any other value that can be treated as a date string using the
  * SimpleDateFormat formatting strings.
  *
  * @param newFormat The new format to adopt
  * @return True if the change was successful otherwise false
  */
  public boolean setOutputDateFormat(String newFormat)
  {
    if (newFormat.equalsIgnoreCase("integer"))
    {
      // set UTC format
      integerFormat = true;
      longFormat = false;
      stringFormat = false;
    }
    else if (newFormat.equalsIgnoreCase("long"))
    {
      // set UTC long format with milliseconds
      integerFormat = false;
      longFormat = true;
      stringFormat = false;
    }
    else
    {
      // Note that, as before, a formatter built here is lenient: only the
      // ones built at construction time have leniency switched off.
      sdfOut.setPattern(newFormat, true);

      // set a string format
      integerFormat = false;
      longFormat = false;
      stringFormat = true;
    }

    // set the input date format
    OutputDateFormat = newFormat;

    return true;
  }

 /**
  * Gets the standard overall date format for output
  *
  * @return The current date format string
  */
  public String getOutputDateFormat()
  {
    return OutputDateFormat;
  }

 /**
  * This formats a date given as a long into a standard string format
  *
  * @param dateToFormat The long date
  * @return The formatted date string
  */
  public String formatLongDate(long dateToFormat)
  {
    return sdfOut.get().format(new Date(dateToFormat*1000));
  }

 /**
  * This formats a date given as a Date into a standard string format
  *
  * @param dateToFormat The date value to format
  * @return The formatted date string
  */
  public String formatLongDate(Date dateToFormat)
  {
    if (dateToFormat == null)
    {
      return "Null Date";
    }
    else
    {
      return sdfOut.get().format(dateToFormat);
    }
  }

 /**
  * Converts a string in the standard date format to a date
  *
  * @param DateToFormat The date in the string format
  * @return The converted dagte
  * @throws java.text.ParseException
  */
  public Date getDatefromLongFormat(String DateToFormat) throws ParseException
  {
    Date tmpDate = sdfIn.get().parse(DateToFormat);

    return tmpDate;
  }

  /**
   * This function converts a date to a GMT date
   *
   * @param date The date to convert
   * @return The date at GMT
   */
  public Date getGmtDate( Date date )
  {
     TimeZone tz = TimeZone.getDefault();
     Date ret = new Date( date.getTime() - tz.getRawOffset() );

     // if we are now in DST, back off by the delta.  Note that we are
     // checking the GMT date, this is the KEY.
     if ( tz.inDaylightTime( ret ))
     {
        Date dstDate = new Date( ret.getTime() - tz.getDSTSavings() );

        // check to make sure we have not crossed back into standard time
        // this happens when we are on the cusp of DST (7pm the day before
        // the change for PDT)
        if ( tz.inDaylightTime( dstDate ))
        {
           ret = dstDate;
        }
     }

     return ret;
  }

  /**
   * This function sees if a date is in Daylight Savings Time (DST)
   *
   * @param date The date the check
   * @return True if the date is in DST otherwise false
   */
  public boolean getDateInDST( Date date )
  {
     TimeZone tz = TimeZone.getDefault();

     boolean RetValue = tz.inDaylightTime(date);

     return RetValue;
  }

 /**
  * Gets the rounded start date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month start date
  */
  public long getUTCMonthStart(Date EventStartDate)
  {
    Date roundedDate = getMonthStart(EventStartDate);
    getCal().setTime(roundedDate);
    long validityMonthStart = getCal().getTimeInMillis() / 1000;

    return validityMonthStart;
  }

 /**
  * Gets the rounded end date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month end date
  */
  public long getUTCMonthEnd(Date EventStartDate)
  {
    Date roundedDate = getMonthEnd(EventStartDate);
    getCal().setTime(roundedDate);
    long validityMonthEnd = getCal().getTimeInMillis() / 1000;

    return validityMonthEnd;
  }

 /**
  * Gets the rounded start date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month start date
  */
  public Date getMonthStart(Date EventStartDate)
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTime(EventStartDate);
    getCal().set(Calendar.HOUR_OF_DAY,0);
    getCal().set(Calendar.MINUTE,0);
    getCal().set(Calendar.SECOND,0);
    getCal().set(Calendar.MILLISECOND,0);
    getCal().set(Calendar.SECOND,0);
    getCal().set(Calendar.DATE,1);

    return getCal().getTime();
  }

 /**
  * Gets the rounded end date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month end date
  */
  public Date getMonthEnd(Date EventStartDate)
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTime(EventStartDate);
    getCal().set(Calendar.HOUR_OF_DAY,0);
    getCal().set(Calendar.MINUTE,0);
    getCal().set(Calendar.SECOND,0);
    getCal().set(Calendar.MILLISECOND,0);
    getCal().set(Calendar.DATE,1);
    getCal().add(Calendar.MONTH,1);
    getCal().add(Calendar.SECOND,-1);

    return getCal().getTime();
  }

	/**
	 * Gets the rounded start date of the year the given event date is in
	 *
	 * @param EventStartDate
	 *            The date of the event
	 * @return The UTC year start date
	 */
	public long getUTCYearStart(Date EventStartDate) {
		Date roundedDate = getYearStart(EventStartDate);
		getCal().setTime(roundedDate);
		long validityYearStart = getCal().getTimeInMillis() / 1000;

		return validityYearStart;
	}

	/**
	 * Gets the rounded end date of the year the given event date is in
	 *
	 * @param EventStartDate
	 *            The date of the event
	 * @return The UTC year end date
	 */
	public long getUTCYearEnd(Date EventStartDate) {
		Date roundedDate = getYearEnd(EventStartDate);
		getCal().setTime(roundedDate);
		long validityYearEnd = getCal().getTimeInMillis() / 1000;

		return validityYearEnd;
	}

	/**
	 * Gets the rounded start date of the year the given event date is in
	 *
	 * @param EventStartDate
	 *            The date of the event
	 * @return The year start date
	 */
	public Date getYearStart(Date EventStartDate) {
		// Get the yearly counter validity periods for this CDR
		getCal().setTime(EventStartDate);
		getCal().set(Calendar.DAY_OF_YEAR, 1);
		getCal().set(Calendar.HOUR_OF_DAY, 0);
		getCal().set(Calendar.MINUTE, 0);
		getCal().set(Calendar.SECOND, 0);
		getCal().set(Calendar.MILLISECOND, 0);

		return getCal().getTime();
	}

	/**
	 * Gets the rounded end date of the year the given event date is in
	 *
	 * @param EventStartDate
	 *            The date of the event
	 * @return The year end date
	 */
	public Date getYearEnd(Date EventStartDate) {
		// Get the yearly counter validity periods for this CDR
		getCal().setTime(EventStartDate);
		getCal().set(Calendar.DAY_OF_YEAR, 366); // 366 for leap year
		getCal().set(Calendar.HOUR_OF_DAY, 0);
		getCal().set(Calendar.MINUTE, 0);
		getCal().set(Calendar.SECOND, 0);
		getCal().set(Calendar.MILLISECOND, 0);
		getCal().add(Calendar.SECOND, -1);

		return getCal().getTime();
	}
  
 /**
  * Gets the UTC start of the day the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC day start date, in seconds
  */
  public long getUTCDayStart(Date EventStartDate)
  {
    Date roundedDate = getDayStart(EventStartDate);
    getCal().setTime(roundedDate);
    long validityDayStart = getCal().getTimeInMillis() / 1000;

    return validityDayStart;
  }

 /**
  * Gets the UTC start of the day the given number of days after (before) the
  * day the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days to offset by
  * @return The UTC day start date, in seconds
  */
  public long getUTCDayStart(Date EventStartDate, int offset)
  {
    Date roundedDate = getDayStart(EventStartDate,offset);
    getCal().setTime(roundedDate);
    long validityDayStart = getCal().getTimeInMillis() / 1000;

    return validityDayStart;
  }

 /**
  * Gets the UTC date of the given event date
  *
  * @param EventStartDate The date of the event
  * @return The UTC representation of the date
  */
  public long getUTCDate(Date EventStartDate)
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTime(EventStartDate);
    long validityDayStart = getCal().getTimeInMillis() / 1000;

    return validityDayStart;
  }

 /**
  * Gets the Java date of the given event date from the UTC Event Date
  *
  * @param EventStartDate The UTC date of the event
  * @return The start date
  */
  public Date getDateFromUTC(long EventStartDate)
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTimeInMillis(EventStartDate*1000);

    return getCal().getTime();
  }

 /**
  * Gets the UTC end of the day the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC day end date, in seconds
  */
  public long getUTCDayEnd(Date EventStartDate)
  {
    Date roundedDate = getDayEnd(EventStartDate);
    getCal().setTime(roundedDate);

    long validityDayEnd = getCal().getTimeInMillis() / 1000;

    return validityDayEnd;
  }

 /**
  * Gets the UTC end of the day the given number of days after (before) the
  * day the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days in the future (past) to get the end date for
  * @return The UTC day end date, in seconds
  */
  public long getUTCDayEnd(Date EventStartDate, int offset)
  {
    Date roundedDate = getDayEnd(EventStartDate,offset);
    getCal().setTime(roundedDate);

    long validityDayEnd = getCal().getTimeInMillis() / 1000;

    return validityDayEnd;
  }

 /**
  * Gets the start of the day the given number of days after (before) the day
  * the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days to offset by
  * @return The day start date
  */
  public Date getDayStart(Date EventStartDate , int offset)
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTime(EventStartDate);
    getCal().set(Calendar.HOUR_OF_DAY,0);
    getCal().set(Calendar.MINUTE,0);
    getCal().set(Calendar.SECOND,0);
    getCal().set(Calendar.MILLISECOND,0);
    getCal().add(Calendar.DAY_OF_MONTH,offset);

    return getCal().getTime();
  }

 /**
  * Gets the end of the day the given number of days after (before) the day
  * the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days in the future (past) to get the end date for
  * @return The day end date
  */
  public Date getDayEnd(Date EventStartDate, int offset)
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTime(EventStartDate);
    getCal().set(Calendar.HOUR_OF_DAY,0);
    getCal().set(Calendar.MINUTE,0);
    getCal().set(Calendar.SECOND,0);
    getCal().set(Calendar.MILLISECOND,0);
    getCal().add(Calendar.DATE,1);
    getCal().add(Calendar.SECOND,-1);
    getCal().add(Calendar.DAY_OF_MONTH,offset);

    return getCal().getTime();
  }

 /**
  * Gets the start of the day the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The day start date
  */
  public Date getDayStart(Date EventStartDate )
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTime(EventStartDate);
    getCal().set(Calendar.HOUR_OF_DAY,0);
    getCal().set(Calendar.MINUTE,0);
    getCal().set(Calendar.SECOND,0);
    getCal().set(Calendar.MILLISECOND,0);

    return getCal().getTime();
  }

 /**
  * Gets the end of the day the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The day end date
  */
  public Date getDayEnd(Date EventStartDate)
  {
    // Get the montly counter validity periods for this CDR
    getCal().setTime(EventStartDate);
    getCal().set(Calendar.HOUR_OF_DAY,0);
    getCal().set(Calendar.MINUTE,0);
    getCal().set(Calendar.SECOND,0);
    getCal().set(Calendar.MILLISECOND,0);
    getCal().add(Calendar.DATE,1);
    getCal().add(Calendar.SECOND,-1);

    return getCal().getTime();
  }

 /**
  * Return the current timestamp
  *
  * @return the current UTC date in millseconds
  */
  public long getCurrentUTCms()
  {
    return Calendar.getInstance().getTimeInMillis();
  }

 /**
  * Return the current timestamp
  *
  * @return the current UTC date in seconds
  */
  public long getCurrentUTC()
  {
    return getCurrentUTCms() / 1000;
  }

 /**
   * Add seconds to a date to get a new date
   *
   * @param inputDate The date to adjust
   * @param duration The number of seconds offset
   * @return The adjusted date
   */
  public Date addDateSeconds(Date inputDate, int duration)
  {
    getCal().setTime(inputDate);
    getCal().add(Calendar.SECOND, duration);

    return getCal().getTime();
  }

 /**
   * Add seconds to a date to get a new date
   *
   * @param inputDate The date to adjust
   * @param duration The number of seconds offset
   * @return The adjusted date
   */
  public Date addDateSeconds(Date inputDate, long duration)
  {
    getCal().setTime(inputDate);
    getCal().add(Calendar.SECOND, (int) duration);

    return getCal().getTime();
  }

 /**
   * Add seconds to a date to get a new date
   *
   * @param inputDate The date to adjust
   * @param duration The number of seconds offset
   * @return The adjusted date
   */
  public Date addDateSeconds(Date inputDate, double duration)
  {
    getCal().setTime(inputDate);
    getCal().add(Calendar.SECOND, (int) duration);

    return getCal().getTime();
  }

//------------------------------------------------------------------------------
//----------------------- Floating Point Utilities -----------------------------
//------------------------------------------------------------------------------

 /**
  * Round a value at the given number of decimal places, using the given
  * rounding mode.
  *
  * We use BigDecimal.valueOf() and not new BigDecimal(double). The
  * constructor takes the *exact binary* value of the double, which is almost
  * never the decimal value the caller had in mind: 0.10 is really
  * 0.10000000000000000555 and so rounds UP to 0.11, while 2.67 is really
  * 2.66999999999999992894 and does not move at all. Which way a value goes
  * then depends on its binary representation, which the caller cannot see.
  * valueOf() goes through Double.toString() and so rounds the decimal value
  * that was actually passed in.
  *
  * @param valueToRound The unrounded value
  * @param decimalPlaces The number of decimal places to round to
  * @param mode The rounding mode to apply
  * @return The rounded value
  */
  private double round(double valueToRound, int decimalPlaces, RoundingMode mode)
  {
    return BigDecimal.valueOf(valueToRound).setScale(decimalPlaces, mode).doubleValue();
  }

 /**
  * Perform standard rounding on a double value. This rounding rounds UP if
  * the digit after the last decimal place to round is >=5, and away from zero
  * for negative values (HALF_UP).
  *
  * @param valueToRound The unrounded value
  * @param decimalPlaces The number of decimal places to round to
  * @return The rounded value
  */
  public double getRoundedValue(double valueToRound, int decimalPlaces)
  {
    return round(valueToRound, decimalPlaces, RoundingMode.HALF_UP);
  }

 /**
  * Perform standard rounding on a double value. This rounding rounds UP to the
  * next largest value (away from zero)
  *
  * @param valueToRound The unrounded value
  * @param decimalPlaces The number of decimal places to round to
  * @return The rounded value
  */
  public double getRoundedValueRoundUp(double valueToRound, int decimalPlaces)
  {
    return round(valueToRound, decimalPlaces, RoundingMode.UP);
  }

 /**
  * Perform standard rounding on a double value. This rounding rounds DOWN to the
  * next smallest value (towards zero)
  *
  * @param valueToRound The unrounded value
  * @param decimalPlaces The number of decimal places to round to
  * @return The rounded value
  */
  public double getRoundedValueRoundDown(double valueToRound, int decimalPlaces)
  {
    return round(valueToRound, decimalPlaces, RoundingMode.DOWN);
  }

 /**
  * Perform standard rounding on a double value. This rounding rounds HALF EVEN.
  * Half Even rounding is good for financial applications where the rounding
  * e.g. 2.12345 to 2.1235 would cause a bias over time.
  *
  * @param valueToRound The unrounded value
  * @param decimalPlaces The number of decimal places to round to
  * @return The rounded value
  */
  public double getRoundedValueRoundHalfEven(double valueToRound, int decimalPlaces)
  {
    return round(valueToRound, decimalPlaces, RoundingMode.HALF_EVEN);
  }

  /**
   * For handling different time zone conversions.
   * 
   * @param newTimeZone The tome zone to set
   */
  public void setTimeZone(TimeZone newTimeZone) {
    timeZone = newTimeZone;
  }
  
}
