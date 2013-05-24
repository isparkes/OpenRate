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

import OpenRate.CommonConfig;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author ian
 */

/**
  * The CustInfo structure holds the information about the customer account,
  * including the validity dates, the product list and the balance group
  * reference. Note that we are using the dates as long integers to reduce
  * the total amount of storage that is required.
  */
public class CustInfo
{
  /**
   * The list of the audit segments we are dealing with. These are held as an
   * ordered list in memory, the "createAuditSegment" inserting them into the
   * right place in the list.
   *
   * If you are loading a large number of these, loading performance can be
   * increased by retrieving them from the DB in chronological order.
   */
  public ArrayList<AuditSegment> CustAudSegments;

  /**
   * The external customer id is the ID known to the outside world
   */
  public String  ExternalCustId;

 /**
  * The balance group is used for holding customer level balances
  */
  public long balanceGroup;

 /**
  * The start date of the customer relationship. Used only in the flat
  * customer model (audited model uses audited validity)
  */
  public long custValidFrom = CommonConfig.LOW_DATE;

 /**
  * The end date of the customer relationship. Used only in the flat
  * customer model (audited model uses audited validity)
  */
  public long custValidTo   = CommonConfig.HIGH_DATE;

  /** Creates a new instance of CustInfo */
  public CustInfo()
  {
    CustAudSegments = new ArrayList<>();
  }

 /**
  * Attempt to create an audit segment covering the given date. If we are not
  * able to create, return null, otherwise return the newly created segment.
  *
  * @param newAudSegValidFrom The date to recover for
  * @return The audit segment , or null if not possible to create
  */
  public AuditSegment createAuditSegment(long newAudSegValidFrom)
  {
    int i = 0;
    AuditSegment tmpAudSegment;
    long tmpAudSegValidFrom;

    Iterator<AuditSegment> segIter = CustAudSegments.iterator();

    while (segIter.hasNext())
    {
      tmpAudSegment = segIter.next();
      tmpAudSegValidFrom = tmpAudSegment.getUTCSegmentValidFrom();

      // see if we have found the right location for the insert
      if (tmpAudSegValidFrom < newAudSegValidFrom)
      {
        // the time of the audit segment to insert is earlier than the previous
        // segment - move on
      }
      else if (tmpAudSegValidFrom > newAudSegValidFrom)
      {
        // the time of the audit segment to insert is later than the previous
        // segment - insert
        tmpAudSegment = new AuditSegment();
        tmpAudSegment.setUTCSegmentValidFrom(newAudSegValidFrom);

        // Insert the element at the correct location
        CustAudSegments = insertElementAt(CustAudSegments, tmpAudSegment, i);

        // done - finish the loop
        return tmpAudSegment;
      }
      else
      {
        // could not create - already exists
        return null;
      }

      i++;
    }

    // If we got here, we did not insert, so add it to the tail of the List
    tmpAudSegment = new AuditSegment();
    tmpAudSegment.setUTCSegmentValidFrom(newAudSegValidFrom);
    CustAudSegments.add(tmpAudSegment);

    return tmpAudSegment;
  }

 /**
  * Find the audit segment matching the validity date
  *
  * @param AudSegValidFrom The validity date to search for
  * @return The best match audit segment
  */
  public AuditSegment getBestAuditSegmentMatch(long AudSegValidFrom)
  {
    int i;
    AuditSegment tmpAudSegment;
    long tmpAudSegVal;

    // search to see if we know this audit segment. While we were creating the
    // audit segments, they should be in the order of "newest first", so we have
    // to search the last first
    for ( i = CustAudSegments.size()-1 ; i>= 0 ; i--)
    {
      tmpAudSegment = CustAudSegments.get(i);
      tmpAudSegVal = tmpAudSegment.getUTCSegmentValidFrom();
      if (tmpAudSegVal <= AudSegValidFrom)
      {
        return tmpAudSegment;
      }
    }

    return null;
  }

 /**
  * Get an existing audit segment using the ID. If we can't find it, then
  * return null
  *
  * @param ID The audit segment ID to find
  * @return The found audit segment or null
  */
  public AuditSegment getAuditSegmentByID(long ID)
  {
    int i;
    AuditSegment tmpAudSegment;
    long tmpAudSegVal;

    // search to see if we know this audit segment
    for ( i = 0; i<CustAudSegments.size() ; i++)
    {
      tmpAudSegment = CustAudSegments.get(i);
      tmpAudSegVal = tmpAudSegment.getAuditSegmentID();
      if (tmpAudSegVal == ID)
      {
        return tmpAudSegment;
      }
    }

    return null;
  }

 /**
  * Simulate insert at (which is not available in ArrayList
  *
  * @param oldList
  * @param audSeg
  * @param i
  * @return
  */
  private ArrayList<AuditSegment> insertElementAt(ArrayList<AuditSegment> oldList, AuditSegment audSeg, int i)
  {
    ArrayList<AuditSegment> newList = new ArrayList<>();

    Iterator<AuditSegment> oldListIter = oldList.iterator();

    int position = 0;
    while (oldListIter.hasNext())
    {
      if (position == i)
      {
        newList.add(audSeg);
      }

      // add the element from the old list
      newList.add(oldListIter.next());
      position++;
    }

    return newList;
  }
}
