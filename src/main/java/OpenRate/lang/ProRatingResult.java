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
