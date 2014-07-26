/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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

package OpenRate.utils;

import OpenRate.CommonConfig;
import OpenRate.resource.ConversionCache;
import OpenRate.resource.ResourceContext;
import java.math.BigDecimal;
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
  private String InputDateFormat = CommonConfig.OR_DEFAULT_DATE_FORMAT;

  // This is the format used for converting dates on input
  private String OutputDateFormat = CommonConfig.OR_DEFAULT_DATE_FORMAT;

  // format used for input
  private SimpleDateFormat sdfIn = null;

  // format used for output
  private SimpleDateFormat sdfOut = null;

  // set that we are using integer (UTC) format
  private boolean integerFormat = false;

  // set that we are using long format (UTC + ms)
  private boolean longFormat = false;

  // set that we are using long format (UTC + ms)
  private boolean stringFormat = true;

  // used for general date manipulation
  private Calendar cal;

  // Used to cache access to the conversion objects
  private static ConversionCache tmpConvCache = null;

  // Singleton instance
  private static ConversionUtils convUtilsObj;

 /**
  * constructor - initialise the internal formatting object
  */
  public ConversionUtils()
  {
    // This is the date format we are using for the output
    sdfOut = new SimpleDateFormat(OutputDateFormat);
    sdfOut.setLenient(false);

    // This is the date format we are using for the input
    sdfIn = new SimpleDateFormat(InputDateFormat);
    sdfIn.setLenient(false);

    // used for date manipulation
    cal = new GregorianCalendar();
  }

 /**
  * Get the access to the singleton conversion cache
  *
  * @return Conversion Cache Reference
  */
  public static ConversionCache getConversionCache()
  {
    if (tmpConvCache == null)
    {
      // Get access to the conversion cache
      ResourceContext ctx    = new ResourceContext();

      // try the new Logging model.
      tmpConvCache = (ConversionCache) ctx.get(ConversionCache.RESOURCE_KEY);
    }

    return tmpConvCache;
  }

 /**
  * Get the access to the singleton conversion utils object
  *
  * @return Conversion Cache Reference
  */
  public static ConversionUtils getConversionUtilsObject()
  {
    if(convUtilsObj == null)
    {
      convUtilsObj = new ConversionUtils();
    }

    return convUtilsObj;
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
        tmpUTCDate = sdfIn.parse(amorphicDate).getTime() / 1000;
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
    cal.setTimeInMillis(UTCDateValue*1000);
    return cal.get(Calendar.DAY_OF_WEEK);
  }

 /**
  * Utility function to get the minute of the day from a long UTC value
  *
  * @param UTCDateValue The long UTC date the convert
  * @return The minute of the day
  */
  public int getMinuteOfDay(long UTCDateValue)
  {
    cal.setTimeInMillis(UTCDateValue*1000);
    return cal.get(Calendar.HOUR_OF_DAY) * 60 + cal.get(Calendar.MINUTE);
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
      sdfIn = new SimpleDateFormat(newFormat);

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
      sdfOut = new SimpleDateFormat(newFormat);

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
    return sdfOut.format(new Date(dateToFormat*1000));
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
      return sdfOut.format(dateToFormat);
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
    Date tmpDate = sdfIn.parse(DateToFormat);

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
    cal.setTime(roundedDate);
    long validityMonthStart = cal.getTimeInMillis() / 1000;

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
    cal.setTime(roundedDate);
    long validityMonthEnd = cal.getTimeInMillis() / 1000;

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
    cal.setTime(EventStartDate);
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.MILLISECOND,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.DATE,1);

    return cal.getTime();
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
    cal.setTime(EventStartDate);
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.MILLISECOND,0);
    cal.set(Calendar.DATE,1);
    cal.add(Calendar.MONTH,1);
    cal.add(Calendar.SECOND,-1);

    return cal.getTime();
  }

 /**
  * Gets the rounded start date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month start date
  */
  public long getUTCDayStart(Date EventStartDate)
  {
    Date roundedDate = getDayStart(EventStartDate);
    cal.setTime(roundedDate);
    long validityDayStart = cal.getTimeInMillis() / 1000;

    return validityDayStart;
  }

 /**
  * Gets the rounded start date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days to offset by
  * @return The UTC month start date
  */
  public long getUTCDayStart(Date EventStartDate, int offset)
  {
    Date roundedDate = getDayEnd(EventStartDate,offset);
    cal.setTime(roundedDate);
    long validityDayStart = cal.getTimeInMillis() / 1000;

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
    cal.setTime(EventStartDate);
    long validityDayStart = cal.getTimeInMillis() / 1000;

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
    cal.setTimeInMillis(EventStartDate*1000);

    return cal.getTime();
  }

 /**
  * Gets the rounded end date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month end date
  */
  public long getUTCDayEnd(Date EventStartDate)
  {
    Date roundedDate = getDayEnd(EventStartDate);
    cal.setTime(roundedDate);

    long validityDayEnd = cal.getTimeInMillis() / 1000;

    return validityDayEnd;
  }

 /**
  * Gets the rounded end date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days in the future (past) to get the end date for
  * @return The UTC month end date
  */
  public long getUTCDayEnd(Date EventStartDate, int offset)
  {
    Date roundedDate = getDayEnd(EventStartDate,offset);
    cal.setTime(roundedDate);

    long validityDayEnd = cal.getTimeInMillis() / 1000;

    return validityDayEnd;
  }

 /**
  * Gets the rounded start date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days to offset by
  * @return The UTC month start date
  */
  public Date getDayStart(Date EventStartDate , int offset)
  {
    // Get the montly counter validity periods for this CDR
    cal.setTime(EventStartDate);
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.MILLISECOND,0);
    cal.add(Calendar.DAY_OF_MONTH,offset);

    return cal.getTime();
  }

 /**
  * Gets the rounded end date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @param offset The number of days in the future (past) to get the end date for
  * @return The UTC month end date
  */
  public Date getDayEnd(Date EventStartDate, int offset)
  {
    // Get the montly counter validity periods for this CDR
    cal.setTime(EventStartDate);
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.MILLISECOND,0);
    cal.add(Calendar.DATE,1);
    cal.add(Calendar.SECOND,-1);
    cal.add(Calendar.DAY_OF_MONTH,offset);

    return cal.getTime();
  }

 /**
  * Gets the rounded start date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month start date
  */
  public Date getDayStart(Date EventStartDate )
  {
    // Get the montly counter validity periods for this CDR
    cal.setTime(EventStartDate);
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.MILLISECOND,0);

    return cal.getTime();
  }

 /**
  * Gets the rounded end date of the month the given event date is in
  *
  * @param EventStartDate The date of the event
  * @return The UTC month end date
  */
  public Date getDayEnd(Date EventStartDate)
  {
    // Get the montly counter validity periods for this CDR
    cal.setTime(EventStartDate);
    cal.set(Calendar.HOUR_OF_DAY,0);
    cal.set(Calendar.MINUTE,0);
    cal.set(Calendar.SECOND,0);
    cal.set(Calendar.MILLISECOND,0);
    cal.add(Calendar.DATE,1);
    cal.add(Calendar.SECOND,-1);

    return cal.getTime();
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
    cal.setTime(inputDate);
    cal.add(Calendar.SECOND, duration);

    return cal.getTime();
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
    cal.setTime(inputDate);
    cal.add(Calendar.SECOND, (int) duration);

    return cal.getTime();
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
    cal.setTime(inputDate);
    cal.add(Calendar.SECOND, (int) duration);

    return cal.getTime();
  }

//------------------------------------------------------------------------------
//----------------------- Floating Point Utilities -----------------------------
//------------------------------------------------------------------------------

 /**
  * Perform standard rounding on a double value. This rounding rounds UP if
  * the last digit after the last decimal place to round is >=5
  *
  * @param valueToRound The unrounded value
  * @param decimalPlaces The number of decimal places to round to
  * @return The rounded value
  */
  public double getRoundedValue(double valueToRound, int decimalPlaces)
  {
    double helper;
    int    exponent;

    // Because of rounding errors in the floating point, we round to
    // one more decimal place than required
    exponent = (int) Math.pow(10, decimalPlaces);
    helper = Math.round((Math.round(valueToRound*10*exponent) + 5) / 10);

    return helper/exponent;
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
    // Convert input value
    BigDecimal tmpValue = new BigDecimal(valueToRound);
    return tmpValue.setScale(decimalPlaces,BigDecimal.ROUND_UP).doubleValue();
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
    // Convert input value
    BigDecimal tmpValue = new BigDecimal(valueToRound);
    return tmpValue.setScale(decimalPlaces,BigDecimal.ROUND_DOWN).doubleValue();
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
    // Convert input value
    BigDecimal tmpValue = new BigDecimal(valueToRound);
    return tmpValue.setScale(decimalPlaces,BigDecimal.ROUND_HALF_EVEN).doubleValue();
  }
}
