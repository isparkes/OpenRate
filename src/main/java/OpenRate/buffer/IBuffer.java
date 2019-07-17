

package OpenRate.buffer;

/**
 * The IBuffer interface is the basic mode of transport of records around the
 * processing architecture. It is basically an FIFO buffer that a "supplier"
 * can put records into, and where they will be stored until a "consumner"
 * pulls them out. The processing is therefore asynchronous, which is a great
 * advantage when dealing with complex "mesh" processing structures.
 *
 * This is an empty grouping class, to group ISupplier and IConsumer, which do
 * the actual definition work.
 */
public interface IBuffer
  extends IConsumer,
          ISupplier
{
 /**
  * Get the buffer supplier name
  *
  * @return the name of the assigned buffer supplier
  */
  public String getSupplier();

 /**
  * Set the buffer supplier name
  *
  * @param newSupplier the name of the assigned buffer supplier
  */
  public void setSupplier(String newSupplier);

 /**
  * Get the buffer consumer name
  *
  * @return the name of the assigned buffer consumer
  */
  public String getConsumer();

 /**
  * Set the buffer consumer name
  *
  * @param newComsumer name of the assigned buffer consumer
  */
  public void setConsumer(String newComsumer);
}
