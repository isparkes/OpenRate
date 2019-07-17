

package OpenRate.lang;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A balance group is a collection of balance elements, grouped by ID. It is
 * structured as a list of counter groups, which groups all of the "balance
 * buckets" with the same identifier.
 *
 * Thus a balance group contains any number of counter groups, each of which can
 * contain any number of counter "buckets". A bucket therefore is a period
 * of validity of a counter.
 *
 * @author ian
 */
public class BalanceGroup
{
  // Used to hold the counters
  private final Map<Integer, CounterGroup> counterList;

  // Used for keeping track of balances to purge
  private boolean balanceDirty = false;

  // Used for giving each counter a unique id
  private long currentRecId = 0;

 /**
  * Used for locking
  */
  public Object balanceLock;

 /** Creates a new instance of BalanceGroup */
  public BalanceGroup()
  {
    counterList = new ConcurrentHashMap<>(10);
  }

 /**
  * Add the counter to the counter group using the next RecID available
  *
  * @param counterId The ID of the counter
  * @param validFrom The validity start date of the counter
  * @param validTo The validity end date of the counter
  * @param currentBal The current balance
  * @return The added counter
  */
  public Counter addCounter(int counterId, long validFrom, long validTo, double currentBal)
  {
    CounterGroup tmpCounterGroup;

    if (!counterList.containsKey(counterId))
    {
      // Create the new counter list
      tmpCounterGroup = new CounterGroup();
      counterList.put(counterId,tmpCounterGroup);
      ++currentRecId;
      return tmpCounterGroup.addCounter(currentRecId,validFrom,validTo,currentBal);
    }
    else
    {
      tmpCounterGroup = counterList.get(counterId);
      ++currentRecId;
      return tmpCounterGroup.addCounter(currentRecId,validFrom,validTo,currentBal);
    }
  }

 /**
  * Add the counter to the counter group, using a specified RecID
  *
  * @param counterId The ID of the counter
  * @param recId The Rec ID to use
  * @param validFrom The validity start date of the counter
  * @param validTo The validity end date of the counter
  * @param currentBal The current balance
  */
  public void addCounter(int counterId, long recId, long validFrom, long validTo, double currentBal)
  {
    CounterGroup tmpCounterGroup;

    if (!counterList.containsKey(counterId))
    {
      // Create the new counter list
      tmpCounterGroup = new CounterGroup();
      counterList.put(counterId,tmpCounterGroup);
      tmpCounterGroup.addCounter(recId,validFrom,validTo,currentBal);
    }
    else
    {
      tmpCounterGroup = counterList.get(counterId);
      tmpCounterGroup.addCounter(recId,validFrom,validTo,currentBal);
    }
  }

 /**
  * Add a counter group to an existing balance group
  *
  * @param counterId The counter group id
  * @return The new Counter group
  */
  public CounterGroup addCounterGroup(int counterId)
  {
    CounterGroup tmpCounterGroup;

    tmpCounterGroup = new CounterGroup();
    counterList.put(counterId,tmpCounterGroup);

    return tmpCounterGroup;
  }

 /**
  * Return the iterator of the counters in the balance group
  *
  * @return The iterator to the counter list
  */
  public Iterator<Integer> getCounterIterator()
  {
    return counterList.keySet().iterator();
  }

 /**
  * Get the counter group for the counter ID
  *
  * @param counterId The counter group to recover
  * @return The recovered counter group
  */
  public CounterGroup getCounterGroup(int counterId)
  {
    CounterGroup tmpCounterGroup = null;

    if (counterList.containsKey(counterId))
    {
      tmpCounterGroup = counterList.get(counterId);
    }

    return tmpCounterGroup;
  }

 /**
  * This marks a balance group as dirty, so that it can be analysed for
  * write-back at a later date
  */
  public void markDirty()
  {
    setBalanceDirty(true);
  }

 /**
  * This sets the current rec id
  *
  * @param newRecId The new rec id
  */
  public void setRecId(long newRecId)
  {
    currentRecId = newRecId;
  }

 /**
  * This gets the current Record ID
  *
  * @return The current rec id
  */
  public long getRecId()
  {
    return currentRecId;
  }

  /**
   * @return the balanceDirty
   */
  public boolean isBalanceDirty() {
    return balanceDirty;
  }

  /**
   * @param balanceDirty the balanceDirty to set
   */
  public void setBalanceDirty(boolean balanceDirty) {
    this.balanceDirty = balanceDirty;
  }
}
