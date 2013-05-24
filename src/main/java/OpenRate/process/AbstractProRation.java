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

import OpenRate.CommonConfig;
import OpenRate.lang.ProRatingResult;
import OpenRate.utils.ConversionUtils;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * This module works on the start and end dates given in the event, and
 * calculates the pro-ration that should be applied to the event. It calculates
 * the number of whole months and days outside the whole months that should
 * be charged for, and then calculates the pricing factor based on these.
 *
 * Note that if the start date is equal to the end date, exactly one day will
 * be charged. If the start date is after the end date, this will have the
 * effect of calculating a refund.
 *
 * If you want to skip the proration (for example for the case of rating one
 * off events), set the end date to 2000-01-01, which will cause the processing
 * to always return a pro-ration factor of 1.
 *
 * @author ian
 */
public abstract class AbstractProRation extends AbstractStubPlugIn
{
  private static Date FirstJan2000;
  private Calendar workingCalendar;
  private Calendar helperCalendar;
  private static final long MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;

  /**
   * Constructor - Create the class
   */
  public AbstractProRation()
  {
    super();

    // get the calendar object for working with the dates
    workingCalendar = new GregorianCalendar();
    workingCalendar.setLenient(false);

    // get the calendar helper object
    helperCalendar = new GregorianCalendar();

    SimpleDateFormat sdfIn = new SimpleDateFormat("dd/MM/yyyy");
    String firstJanDate = "01/01/2000";

    try
    {
      FirstJan2000 = sdfIn.parse(firstJanDate);
    }
    catch (ParseException ex)
    {
      Message = "Error getting date in <" + getSymbolicName() + ">";
      pipeLog.fatal(Message);
    }
  }

 /**
  * This calculates the total period to be considered for this event. Given an
  * input start and end date, the multiplier factor is returned for the total
  * multipler to a simple monthly fee.
  *
  * For example, given a monthly fee of EUR10, the total factor calculated for
  * the period "20120103000000" to "20120303000000" will be 2 months and 1 day.
  *
  * The factor that is returned can be calculated either by using the real
  * number of days in the month, (useCalendarDays = true), or a standard 30 days
  * (useCalendarDays = false).
  *
  * For calculating a refund, put the end date before the start date
  * (i.e. invert the parameters). This method works on the basis of days
  * (not hours, minutes or seconds).
  *
  * @param StartDate the start of the period to calculate for
  * @param EndDate the end of the period to calculate for
  * @param useCalendarDays True = use the real number of days in the month, otherwise assume 30
  * @return The processed record
  */
  public ProRatingResult calculateProRatedMonth(Date StartDate, Date EndDate, boolean useCalendarDays)
  {
    int     DaysInMonth;
    int     TotalPeriodDays;
    int     WholeMonths = 0;
    int     TotalDays = 0;
    double  ProRationOffset = 0;
    boolean refunding = false;

    ProRatingResult result = new ProRatingResult();

    // if we find the end date 01JAN2000, get out of here - it is a purchase
    if (EndDate.compareTo(FirstJan2000) == 0)
    {
      result.setProRationFactor(1);

      // Get the component parts
      result.setDaysInPeriod(0);
      result.setMonthsInPeriod(0);

      // no need to work anything else out - finished
      return result;
    }
    else
    {
      // see if we have a refund case
      if (StartDate.after(EndDate))
      {
        Date tmpDate = StartDate;
        StartDate = EndDate;
        EndDate = tmpDate;
        refunding = true;
      }

      // get out starting point
      workingCalendar.setTime(StartDate);

      // See how many days short of the first month we are
      helperCalendar.setTime(EndDate);

      // get the total number of days in the period
      TotalPeriodDays = getDaysBetweenDates(workingCalendar.getTime(), helperCalendar.getTime()) + 1;

      while (TotalPeriodDays > 0)
      {
        // Get the real number of days in the month
        DaysInMonth = workingCalendar.getActualMaximum(Calendar.DAY_OF_MONTH);

        if (TotalPeriodDays >= DaysInMonth)
        {
          // move on a whole month
          WholeMonths++;

          // reduce the days to work on
          TotalPeriodDays -= DaysInMonth;

          // move the date on and deal with the rolling month
          int Month = workingCalendar.get(Calendar.MONTH) + 1;
          if (Month == 12)
          {
            // Bump up the year
            workingCalendar.roll(Calendar.YEAR, true);
            Month = 0;
          }
          workingCalendar.set(Calendar.MONTH, Month);

        }
        else
        {
          // get the fractional part
          TotalDays = TotalPeriodDays;
          if (useCalendarDays)
          {
            // Make the calculation based on the real number of days in the month
            ProRationOffset = (double)TotalDays/(double)DaysInMonth;
          }
          else
          {
            // Assume 30 for the calculation part
            ProRationOffset = ((double)TotalDays/(double)DaysInMonth) *
                              ((double)DaysInMonth/(double)30);
          }

          TotalPeriodDays = 0;
        }
      }
    }

    if (refunding)
    {
      result.setProRationFactor(-(WholeMonths + ProRationOffset));
      result.setDaysInPeriod(-TotalDays);
      result.setMonthsInPeriod(-WholeMonths);
    }
    else
    {
      // Get the final prorated sum
      result.setProRationFactor(WholeMonths + ProRationOffset);

      // Get the component parts
      result.setDaysInPeriod(TotalDays);
      result.setMonthsInPeriod(WholeMonths);
    }

    // set the period dates
    result.setPeriodStartDate(StartDate);
    result.setPeriodEndDate(EndDate);

    // return the updated record
    return result;
  }

 /**
  * This calculates the pro-ration factor to be considered for this event. This
  * takes the validity start and end date into the algorithm, and then
  * returns the total pro-ration factor for the month of the EventDate. Thus the
  * result is bounded between 0 and 1. 0 means that there were no days of
  * validity, 1 means that all days were covered by the validity.
  *
  * @param StartDate the start of the period to calculate for
  * @param EndDate the end of the period to calculate for
  * @param EventDate The Date to calculate the pro-ration factor for
  * @param useCalendarDays True = use the real number of days in the month, otherwise assume 30
  * @return The pro rating result
  */
  public ProRatingResult calculateProRationFactor(Date StartDate, Date EndDate, Date EventDate, boolean useCalendarDays)
  {
    int     DaysInMonth;
    int     daysInPeriod;
    Date    periodStart;
    Date    periodEnd;

    ProRatingResult result = new ProRatingResult();

    // Deal with the easy cases first: no validity
    if (EventDate.before(StartDate) || EventDate.after(EndDate))
    {
      // No validity coverage
      result.setProRationFactor(0);

      // Get the component parts
      result.setDaysInPeriod(0);
      result.setMonthsInPeriod(0);

      result.setPeriodStartDate(new Date(CommonConfig.LOW_DATE));
      result.setPeriodStartDate(new Date(CommonConfig.HIGH_DATE));

      // no need to work anything else out - finished
      return result;
    }
    else
    {
      // See if we have a pro-ration to do: if the Event Date is not in the
      // Start month or end month, then we are home and dry
      workingCalendar.setTime(EventDate);
      int EventMonth = workingCalendar.get(Calendar.MONTH) + 1 + workingCalendar.get(Calendar.YEAR)*12;
      DaysInMonth = workingCalendar.getActualMaximum(Calendar.DAY_OF_MONTH);

      // Default the period start and end to the start and end of the month
      // we will only change these in the code later if they are wrong
      // (often they will be right)
      periodStart = ConversionUtils.getConversionUtilsObject().getMonthStart(EventDate);
      periodEnd   = ConversionUtils.getConversionUtilsObject().getMonthEnd(EventDate);

      // get the months of the validity start and end
      workingCalendar.setTime(StartDate);
      int StartMonth = workingCalendar.get(Calendar.MONTH) + 1 + workingCalendar.get(Calendar.YEAR)*12;
      workingCalendar.setTime(EndDate);
      int EndMonth = workingCalendar.get(Calendar.MONTH) + 1 + workingCalendar.get(Calendar.YEAR)*12;

      // Check the cases based on the month identifiers
      if (EventMonth == StartMonth)
      {
        // check if we can just look at the start month
        if (StartMonth == EndMonth)
        {
          // both start and end in the same month, just return the number of
          // days divided by the days in the month
          daysInPeriod = getDaysBetweenDates(StartDate,EndDate);

          // The period dates are just the original validity dates
          periodStart = StartDate;
          periodEnd = EndDate;
        }
        else
        {
          // We need to calculate up to the end of the month
          workingCalendar.setTime(ConversionUtils.getConversionUtilsObject().getMonthEnd(StartDate));
          daysInPeriod = getDaysBetweenDates(StartDate,workingCalendar.getTime());

          // The period dates are the original start date to the end of the month
          periodStart = StartDate;
        }
      }
      else if (EventMonth == EndMonth)
      {
        // check if we can just look at the start month
        if (StartMonth == EndMonth)
        {
          // both start and end in the same month, just return the number of
          // days divided by the days in the month
          daysInPeriod = getDaysBetweenDates(StartDate,EndDate);
        }
        else
        {
          // We need to calculate from the start of the month
          workingCalendar.setTime(ConversionUtils.getConversionUtilsObject().getMonthStart(EndDate));
          daysInPeriod = getDaysBetweenDates(workingCalendar.getTime(),EndDate);

          // The period dates are the start of the month to the original  end date
          periodEnd = EndDate;
        }
      }
      else
      {
        // full month in the middle
        daysInPeriod = DaysInMonth;
      }

      // Set the default values
      result.setMonthsInPeriod(0);

      // Now work out the factor
      if (useCalendarDays)
      {
        if (daysInPeriod > 30)
        {
          daysInPeriod = 30;
        }

        if (DaysInMonth > 30)
        {
          DaysInMonth = 30;
        }
      }

      // set up the output
      result.setDaysInPeriod(daysInPeriod);
      result.setProRationFactor((double)daysInPeriod / (double) DaysInMonth);
      result.setPeriodStartDate(periodStart);
      result.setPeriodEndDate(periodEnd);
    }

    // return the updated record
      return result;
  }

  // -----------------------------------------------------------------------------
  // ----------------------- Start of utility functions --------------------------
  // -----------------------------------------------------------------------------

 /**
  * Calculate the number of days between two dates
  *
  * @param startDate The start date of the period
  * @param endDate The end date of the period
  * @return The number of days difference
  */
  public int getDaysBetweenDates(Date startDate, Date endDate)
  {
    long diff = endDate.getTime() - startDate.getTime();
    double daysdiff = diff / (double) MILLISECONDS_PER_DAY;
    int days = (int) Math.ceil(daysdiff);
    return Math.abs(days);
  }
}
