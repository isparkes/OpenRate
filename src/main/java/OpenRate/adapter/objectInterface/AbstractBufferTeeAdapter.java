

package OpenRate.adapter.objectInterface;

import OpenRate.buffer.ArrayListQueueBuffer;
import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.LinkedBufferCache;
import OpenRate.resource.ResourceContext;
import OpenRate.utils.PropertyUtils;
import java.util.Collection;

/**
 * This adapter tees into a Real Time pipeline and takes a feed of the events
 * for putting into a batch pipeline. This is usually used for persistence
 * of RT events in a batch mode, however, it can also be used for balance
 * updates in a batch pipeline.
 *
 * @author ian
 */
public abstract class AbstractBufferTeeAdapter
        extends AbstractTeeAdapter
{
  // The output buffer we are writing to in tee mode
  ArrayListQueueBuffer outputBuffer = new ArrayListQueueBuffer();

  @Override
  public void init(String PipelineName, String ModuleName) throws InitializationException
  {
    super.init(PipelineName, ModuleName);

    String CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName, ModuleName, "CacheName");

    // Get access to the conversion cache
    ResourceContext ctx    = new ResourceContext();

    // get the reference to the buffer cache
    LinkedBufferCache LBC = (LinkedBufferCache) ctx.get(CacheObjectName);

    if (LBC == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    String BufferName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName, ModuleName, "BufferName");

    outputBuffer.setSupplier(getSymbolicName());
    outputBuffer.setConsumer(CacheObjectName);

    // Store the buffer
    LBC.putBuffer(BufferName, outputBuffer);
  }

 /**
  * Push the collected batch of records into the transport layer
  *
  * @param batchToPush The batch we are pushing
  */
  @Override
  public void pushTeeBatch(Collection<IRecord> batchToPush)
  {
    // simplest case - just push the buffer into the linked buffer
    outputBuffer.push(batchToPush);
  }
}
