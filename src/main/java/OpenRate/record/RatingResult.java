

package OpenRate.record;

import java.util.ArrayList;

/**
 * These are used to pass the results of rating back to the module so that
 * RUM consumption can be handled, and diagnostics can be written
 *
 * @author ian
 */
public class RatingResult
{
 /**
  * the total of all tiers rated
  */
  public double RatedValue = 0;

 /**
  * the amount of RUM consumed
  */
  public double RUMUsed = 0;

 /**
  * the amount of RUM consumed rounded to the next beat
  */
  public double RUMUsedRounded = 0;

 /**
  * the breakdown of the individual rating steps
  */
  public  ArrayList<RatingBreakdown> breakdown;
}

