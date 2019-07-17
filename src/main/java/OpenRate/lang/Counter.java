

package OpenRate.lang;

import OpenRate.CommonConfig;


/**
 * A counter is a balance counter, that holds the record ID, the validity period
 * and the current balance.
 *
 * @author ian
 */
public class Counter
{
  /**
   * used to hold the current balance
   */
  public double CurrentBalance = 0;

  /**
   * The unique id of this counter
   */
  public long   RecId    = 0;

  /**
   * the validity start and end of this counter, default for ever
   */
  public long   validFrom = CommonConfig.LOW_DATE;

  /**
   * the validity start and end of this counter, default for ever
   */
  public long   validTo = CommonConfig.HIGH_DATE;

 /** Creates a new instance of Counter */
  public Counter()
  {
  }
}
