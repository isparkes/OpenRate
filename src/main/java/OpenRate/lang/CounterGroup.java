/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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
