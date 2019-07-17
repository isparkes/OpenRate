package OpenRate.record;

/**
 * Balance Impact class to map the balance impacts in the pipe.
 *
 * @author Ian
 */
public class BalanceImpact
{
 /**
  * The BalanceImpact structure holds the information about the impacts that
  * have been created on the record. This is used for both rating and discounting
  * cases. For the management of the fields, see the comments. D = Discounting
  * R = Rating
  */

 /**
  * (D,R) The name of Balance Group - can be used to manage impacts on
  * multiple balance groups in a single event.
  * @ added by Denis, Benjamin
  */
  public long balanceGroup=0;

 /**
  * The type of packet D = Discount, R = Rating
  */
  public String type=null;

 /**
  * (D) The name of the CPI that caused this impact
  */
  public String cpiName=null;

 /**
  * (D) The name of the rule that caused the impact
  */
  public String ruleName=null;

 /**
  * (D,R) The RUM that was used to cause the impact
  */
  public String rumUsed=null;

 /**
  * (D) The amount of the RUM used (R) the original RUM value
  */
  public double rumValueUsed;

 /**
  * (D) The amount of the RUM after (R) 0
  */
  public double rumValueAfter;

 /**
  * (D) The value of the RUM after (R) The Rated Value
  */
  public double balanceDelta;

 /**
  * (D) The value of the RUM after (R) The Rated Value
  */
  public double balanceAfter;

 /**
  * (R) The Resource that was impacted
  */
  public String Resource=null;

 /**
  * The ID of the counter
  */
  public int    counterID;

 /**
  * Internal identifier of the Counter period
  */
  public long   recID;

 /**
  * Start of the counter period
  */
  public long   startDate;

 /**
  * End of the counter period
  */
  public long   endDate;

  /**
   * Creates a new instance of BalanceImpact
   */
  public BalanceImpact()
  {
  }
}
