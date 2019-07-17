

package OpenRate.record;

/**
 * This class maps the initial and updated value of the RUM for the RatingRecord
 * for RUM based rating.
 *
 * @author ian
 */
public class RUMInfo
{
 /**
  * Name of the RUM
  */
  public String RUMName = null;

 /**
  * The original RUM quantity
  */
  public double OrigQuantity = 0;

 /**
  * The current RUM quantity
  */
  public double RUMQuantity = 0;

  /**
   * Creates a new instance of the RUMInfo block
   *
   * @param RUM The name of the RUM to
   * @param Quantity The value to set the RUM to
   */
  public RUMInfo(String RUM, double Quantity)
  {
    RUMName = RUM;
    OrigQuantity = Quantity;
    RUMQuantity = Quantity;
  }
}
