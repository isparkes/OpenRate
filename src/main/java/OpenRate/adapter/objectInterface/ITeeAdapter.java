

package OpenRate.adapter.objectInterface;

import OpenRate.record.IRecord;
import java.util.Collection;

/**
 * The ITeeAdapter interface is used to pass a batch of records to the
 * transport layer. The transport can be anything (e.g. socket, buffer), so this
 * forms the interface from the management layer to the transport layer.
 *
 * @author ian
 */
public interface ITeeAdapter
{
 /**
  * Push the collected batch of records into the transport layer
  *
  * @param batchToPush The batch we are pushing
  */
  public void pushTeeBatch(Collection<IRecord> batchToPush);
}
