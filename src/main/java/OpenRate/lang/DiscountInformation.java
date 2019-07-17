

package OpenRate.lang;

/**
 * Provides information about discounting that was applied:
 *
 *   - If a balance impact was created
 *   - If the record was discounted
 *   - The ID and Record ID of the counter impacted
 *   - The value consumed
 *   - The value of the counter after the impact
 *   - DiscountFlag. 0 if no discount applied, 1 if entirely discounted,
 *                   2 if partially discounted (counter exhausted)
 *
 * @author ian
 */
public class DiscountInformation
{
  private boolean BalanceCreated = false;
  private boolean DiscountApplied = false;
  private double  DiscountedValue = 0;
  private int     CounterId;
  private long    RecId;
  private double  NewBalanceValue = 0;
  private int     DiscountFlag = 0;

 /**
  * Returns if a balance was created
  *
  * @return the BalanceCreated
  */
  public boolean isBalanceCreated()
  {
    return BalanceCreated;
  }

 /**
  * Sets the BalanceCreated flag
  *
  * @param BalanceCreated the BalanceCreated to set
  */
  public void setBalanceCreated(boolean BalanceCreated) {
    this.BalanceCreated = BalanceCreated;
  }

 /**
  * Returns true if a discount was applied
  *
  * @return the DiscountApplied
  */
  public boolean isDiscountApplied()
  {
    return DiscountApplied;
  }

 /**
  * Set the discount applied flag
  *
  * @param DiscountApplied the DiscountApplied to set
  */
  public void setDiscountApplied(boolean DiscountApplied)
  {
    this.DiscountApplied = DiscountApplied;
  }

 /**
  * Return the value of the discount
  *
  * @return the DiscountedValue
  */
  public double getDiscountedValue()
  {
    return DiscountedValue;
  }

  /**
   * @param DiscountedValue the DiscountedValue to set
   */
  public void setDiscountedValue(double DiscountedValue) {
    this.DiscountedValue = DiscountedValue;
  }

 /**
  * Get the counter ID of the counter the discount was applied to
  *
  * @return the CounterId
  */
  public int getCounterId()
  {
    return CounterId;
  }

 /**
  * Set the counter ID of the counter the discount was applied to
  *
  * @param CounterId the CounterId to set
  */
  public void setCounterId(int CounterId)
  {
    this.CounterId = CounterId;
  }

 /**
  * Get the record id of the counter the discount was applied to
  *
  * @return the RecId
  */
  public long getRecId()
  {
    return RecId;
  }

 /**
  * Set the record id of the counter the discount was applied to
  *
  * @param RecId the RecId to set
  */
  public void setRecId(long RecId)
  {
    this.RecId = RecId;
  }

 /**
  * Get the new balance value of the counter. This is the value after the
  * discount was applied to it.
  *
  * @return the NewBalanceValue
  */
  public double getNewBalanceValue()
  {
    return NewBalanceValue;
  }

 /**
  * Get the new balance value fo the counter
  *
  * @param NewBalanceValue the NewBalanceValue to set
  */
  public void setNewBalanceValue(double NewBalanceValue)
  {
    this.NewBalanceValue = NewBalanceValue;
  }

 /**
  * Get the value of the discount flag:
  *  1 = fully discounted
  *  2 = threshold crossing
  *  3 = refund
  *  4 = aggregate
  *
  * @return the DiscountFlag
  */
  public int getDiscountFlag()
  {
    return DiscountFlag;
  }

 /**
  * Set the value of the discount flag
  *
  * @param DiscountFlag the DiscountFlag to set
  */
  public void setDiscountFlag(int DiscountFlag)
  {
    this.DiscountFlag = DiscountFlag;
  }
}
