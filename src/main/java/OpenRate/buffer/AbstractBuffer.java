/* ====================================================================
 * Limited Evaluation License:
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

package OpenRate.buffer;

import OpenRate.audit.AuditUtils;
import java.util.HashSet;
import java.util.Iterator;


/**
 * Abstract FIFO Buffer class.
 *
 * This is the heart of the record passing scheme, and defines the record
 * transport strategy, which is used to transport records through the pipeline
 * between one plugin and the next. Each plugin reads (pulls) records from the
 * input buffer, works on them, and then writes (pushes) them to the output
 * buffer. Exceptions to this are of course input adapters, which read from some
 * external source, and push records into the pipeline, and output adapters,
 * which read records and (may) destroy them.
 *
 * This abstract class must be extended with a storage class, which is able to
 * contain records, as this abstract class only deals with the monitor
 * managment.
 */
public abstract class AbstractBuffer
  implements IBuffer
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AbstractBuffer.java,v $, $Revision: 1.31 $, $Date: 2013-05-13 18:12:11 $";

  // this hash set contains all of the monitors to this buffer. These will
  // be notified when new records are ready.
  private final HashSet<IMonitor> monitors = new HashSet<>();

  private String Supplier;
  private String Consumer;

 /**
  * Constructor for AbstractBuffer
  */
  public AbstractBuffer()
  {
    // Add the version map
    AuditUtils.getAuditUtils().buildHierarchyVersionMap(this.getClass());
  }

 /**
  * notifyMonitors notifies all monitors to this buffer, triggering each of
  * them in turn.
  */
  protected void notifyMonitors()
  {
    synchronized (monitors)
    {
      Iterator<IMonitor> iter = monitors.iterator();

      while (iter.hasNext())
      {
        IMonitor m = iter.next();
        m.notify(BufferEvent.NEW_RECORDS);
      }
    }
  }

 /**
  * registerMonitor adds a new monitor to the internal list of monitors to
  * this buffer.
  *
  * @param m The monitor object to be added
  */
  @Override
  public void registerMonitor(IMonitor m)
  {
    synchronized (monitors)
    {
      monitors.add(m);
    }
  }

 /**
  * Get the buffer supplier name
  *
  * @return the name of the assigned buffer supplier
  */
  @Override
  public String getSupplier()
  {
    return Supplier;
  }

 /**
  * Set the buffer supplier name
  *
  * @param newSupplier the name of the assigned buffer supplier
  */
  @Override
  public void setSupplier(String newSupplier)
  {
    Supplier = newSupplier;
  }

 /**
  * Get the buffer consumer name
  *
  * @return the name of the assigned buffer consumer
  */
  @Override
  public String getConsumer()
  {
    return Consumer;
  }

 /**
  * Set the buffer consumer name
  *
  * @param newComsumer the name of the assigned buffer consumer
  */
  @Override
  public void setConsumer(String newComsumer)
  {
    Consumer = newComsumer;
  }
}
