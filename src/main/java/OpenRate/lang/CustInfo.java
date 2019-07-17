

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
