/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
