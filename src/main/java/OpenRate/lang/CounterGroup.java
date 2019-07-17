

package OpenRate.lang;

import java.util.ArrayList;

/**
 * A counter group is a grouping of all the counters with the same counter id.
 * Each counter in the group has a validity from and to date, and validity
 * periods may overlap. In addition, each counter in the group has a "recID"
 * which can be used to locate it for update it or access it. The RecID is
 * managed at balance group level.
 *
 * @author ian
 */
public class CounterGroup
{
 /**
  * List of the counters in this counter group.
  */
  public ArrayList<Counter> counters;

 /** Creates a new instance of BalanceGroup */
  public CounterGroup()
  {
    counters = new ArrayList<>();
  }

 /**
  * Add a counter to the group
  *
  * @param recId The rec Id to add for
  * @param validFrom The start of the validity of the counter
  * @param validTo The end of the validity of the counter
  * @param currentBal The initial value of the counter
  * @return The created counter
  */
  public Counter addCounter(long recId, long validFrom, long validTo, double currentBal)
  {
    Counter tmpCounter;

    tmpCounter = new Counter();
    tmpCounter.RecId = recId;
    tmpCounter.validFrom = validFrom;
    tmpCounter.validTo = validTo;
    tmpCounter.CurrentBalance = currentBal;
    counters.add(tmpCounter);

    return tmpCounter;
  }

 /**
  * Get an individual counter as referenced by a date in long format
  *
  * @param counterDate The date of the counter to get from the group
  * @return The recovered counter
  */
  public Counter getCounterByUTCDate(long counterDate)
  {
    int i;
    Counter tmpCounter;

    for(i = 0 ; i < counters.size() ; i++ )
    {
      tmpCounter = counters.get(i);
      if ((tmpCounter.validFrom <= counterDate) & (tmpCounter.validTo > counterDate))
      {
        return tmpCounter;
      }
    }

    return null;
  }

 /**
  * Get an individual counter as referenced by a date in long format
  *
  * @param recId The record ID of the counter to get
  * @return The recovered counter
  */
  public Counter getCounterById(int recId)
  {
    int i;
    Counter tmpCounter;

    for(i = 0 ; i < counters.size() ; i++ )
    {
      tmpCounter = counters.get(i);
      if (tmpCounter.RecId == recId)
      {
        return tmpCounter;
      }
    }

    return null;
  }

 /**
  * Get all of the counters in the group
  *
  * @return The entire counter group
  */
  public ArrayList<Counter> getCounters()
  {
    return counters;
  }
}
