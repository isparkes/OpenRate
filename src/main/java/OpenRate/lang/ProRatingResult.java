

package OpenRate.lang;

import java.util.Date;

/**
 * This class holds the results of the pro ration
 *
 * @author ian
 */
public class ProRatingResult
{
  private double proRationFactor = 0;
  private int    daysInPeriod = 0;
  private int    monthsInPeriod = 0;
  private Date   periodStartDate;
  private Date   periodEndDate;

  /**
   * Get the pro-ration factor.
   *
   * @return the proRationFactor
   */
  public double getProRationFactor()
  {
    return proRationFactor;
  }

  /**
   * Set the pro-ration factor
   *
   * @param proRationFactor the proRationFactor to set
   */
  public void setProRationFactor(double proRationFactor)
  {
    this.proRationFactor = proRationFactor;
  }

  /**
   * Get the number of days in the pro-ration period
   *
   * @return the daysInPeriod
   */
  public int getDaysInPeriod()
  {
    return daysInPeriod;
  }

  /**
   * Set the number of days in the pro-ration period
   *
   * @param daysInPeriod the daysInPeriod to set
   */
  public void setDaysInPeriod(int daysInPeriod)
  {
    this.daysInPeriod = daysInPeriod;
  }

  /**
   * Get the number of months in the pro-ration period
   *
   * @return the monthsInPeriod
   */
  public int getMonthsInPeriod()
  {
    return monthsInPeriod;
  }

  /**
   * Set the number of months in the pro-ration period
   *
   * @param monthsInPeriod the monthsInPeriod to set
   */
  public void setMonthsInPeriod(int monthsInPeriod)
  {
    this.monthsInPeriod = monthsInPeriod;
  }

  /**
   * @return the periodStartDate
   */
  public Date getPeriodStartDate() {
    return periodStartDate;
  }

  /**
   * @param periodStartDate the periodStartDate to set
   */
  public void setPeriodStartDate(Date periodStartDate) {
    this.periodStartDate = periodStartDate;
  }

  /**
   * @return the periodEndDate
   */
  public Date getPeriodEndDate() {
    return periodEndDate;
  }

  /**
   * @param periodEndDate the periodEndDate to set
   */
  public void setPeriodEndDate(Date periodEndDate) {
    this.periodEndDate = periodEndDate;
  }
}
