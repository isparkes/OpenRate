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

package OpenRate.buffer;

import OpenRate.record.IRecord;
import java.util.Collection;
import java.util.LinkedList;


/**
 * Buffer implementation using an LinkedListQueue as the buffering element.
 * As this is one of the performance sensitive parts of the system, you might
 * want to define different buffering structures, depending on the limitations
 * of the application that you are working on. However the ArrayListQueue will
 * fulfill the requirements for a large majority of applications, as it:
 *  - Preserves record ordering
 *  - Allows easy addition and extraction
 *  - Is reasonably fast
 */
public class LinkedListQueueBuffer
  extends AbstractBuffer
{
  // The main storage object of the buffer
  private LinkedList<IRecord> queueHelperBatch;

  // the locking object so that we make sure that the multi-threadedness of
  // the object is guaranteed
  private final Object lock;

  /**
   * Default constructor.
   */
  public LinkedListQueueBuffer()
  {
    super();

    //queueHelper = Collections.synchronizedList(new LinkedList());
    queueHelperBatch = new LinkedList<>();
    lock = new Object();
  }

 /**
  * Push an entire collection of batch records into the buffer.
  * This is the main event for the addition of records into a buffer.
  *
  * @param collection The collection of records to push
  */
  @Override
  public void push(Collection<IRecord> collection)
  {
    synchronized (lock)
    {
      // blocks waiting for queue availability.
      queueHelperBatch.addAll(collection);
    }

    // tell the downstream modules that there is stuff to do
    notifyMonitors();
  }

 /**
  * Retrieve a number of batch records from the buffer. The number of records
  * that is returned is either all of the records available in the case that the
  * number of records is less than the specified maximum, or the specified
  * maximum. Note that the primary reason for limiting the number of records
  * that can be pulled at a time is to:
  *  - make sure that a particularly slow module does not block the processing
  *  - to allow multiple instances of the same plug in to work on records
  *    in parallel, thus removing bottlenecks
  *
  * Note that the records are retrieved from the head of the list, despite the
  * fact that retrieving from the tail could provide a higher performance. In
  * the case that you really do not care about ordering, you could tweak this.
  */
  @Override
  public Collection<IRecord> pull(int max)
  {
    LinkedList<IRecord> list = new LinkedList<>();

    synchronized (lock)
    {
      int size = queueHelperBatch.size();

      for (int i = 0; (i < max) && (size > 0); ++i)
      {
        // It would be faster to pull from the end of the array instead
        // of the beginning, but if order matters (and mostly it will) we have
        // to work from the head of the queue.
        // In cases where order really does not matter, we could use:
        // Object o = queueHelper.remove(--size);
        IRecord o = queueHelperBatch.remove();
        list.add(o);
      }
    }

    return list;
  }

 /**
  * Return the number of events in the buffer
  */
  @Override
  public int getEventCount()
  {
    return queueHelperBatch.size();
  }
}
