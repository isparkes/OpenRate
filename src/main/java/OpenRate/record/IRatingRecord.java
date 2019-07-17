

package OpenRate.record;

import java.io.Serializable;
import java.util.List;

/**
 * IRatingRecord type used for data being passed through the Pipeline. The
 * primitive record ancestor for all rating records that can pass through a
 * framework.
 */
public interface IRatingRecord extends Serializable
{
 /**
  * Get the value of an existing RUM, or 0 if not found
  *
  * @param RUM The RUM value to get
  * @return The current value of the RUM
  */
  public double getRUMValue(String RUM);

 /**
  * Get the existing RUM values
  *
  * @return The current RUM list
  */
  public List<RUMInfo> getRUMs();

 /**
  * Set the value of a RUM overwriting any existing value.
  *
  * @param RUM The RUM value to set
  * @param newValue The new value to set
  */
  public void setRUMValue(String RUM, double newValue);

 /**
  * Set the value of a RUM, return true if OK, false if not OK (e.g. overwrite
  * existing value)
  *
  * @param RUM The RUM value to set
  * @param valueDelta The delta to apply to the RUM value
  * @return true if the delta was applied, otherwise false
  */
  public boolean updateRUMValue(String RUM, double valueDelta);

 /**
  * Get the UTC event date of the rating record
  *
  * @return The long UTC event date of the record
  */
  public long getUTCEventDate();

 /**
  * Get the number of balance impacts that are available. This is implemented as
  * a function to allow the charge packet definition to be overwritten.
  *
  * @return The number of balance impacts
  */
  public int getBalanceImpactCount();

 /**
  * This returns the balance impact at the given index.
  *
  * @param index The index to use for the retrieval
  * @return The requested balance impact
  */
  public BalanceImpact getBalanceImpact(int index);

  /* This returns all balance impacts.
  *
  * @return The requested balance impacts
  */
  public List<BalanceImpact> getBalanceImpacts();
  
 /**
  * Adds a balance impact
  *
  * @param newBI The new balance impact to add
  */
  public void addBalanceImpact(BalanceImpact newBI);

 /**
  * Create a charge packet
  *
  * @return the new charge packet
  */
  public BalanceImpact newBalanceImpact();

}
