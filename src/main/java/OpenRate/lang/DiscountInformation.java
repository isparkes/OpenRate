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
