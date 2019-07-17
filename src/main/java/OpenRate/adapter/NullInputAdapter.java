package OpenRate.adapter;

import OpenRate.exception.ProcessingException;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import java.util.ArrayList;
import java.util.Collection;

/**
 * The Null Input Adapter is used primarily for testing. It does not create or
 * source records, and is used to build pipelines in the unit tests so that we
 * can test the whole framework. It is not a generally useful module. But we
 * love it anyway.
 */
public class NullInputAdapter
        extends AbstractInputAdapter {

  /**
   * Retrieve a batch of records from the adapter.
   *
   * @return The collection of records that was loaded
   * @throws OpenRate.exception.ProcessingException
   */
  @Override
  protected Collection<IRecord> loadBatch() throws ProcessingException {
    ArrayList<IRecord> outBatch = new ArrayList<>();
    return outBatch;
  }

  /**
   * This is called when the synthetic Header record is encountered, and has the
   * meaning that the stream is starting. In this case we have to open a new
   * dump file each time a stream starts. *
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  @Override
  public HeaderRecord procHeader(HeaderRecord r) throws ProcessingException {
    return r;
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public IRecord procValidRecord(IRecord r) throws ProcessingException {
    return r;
  }

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  public IRecord procErrorRecord(IRecord r) throws ProcessingException {
    return r;
  }

  /**
   * This is called just before the trailer, and allows any pending record to be
   * pushed into the pipe before the trailer. Note that this is useful when
   * there is no trailer in a file, otherwise the file (not the synthetic
   * trailer) trailer will normally be used for this.
   *
   * @return The possible pending record in the adapter at the moment
   * @throws ProcessingException
   */
  public IRecord purgePendingRecord() throws ProcessingException {
    return null;
  }

  /**
   * This is called when the synthetic trailer record is encountered, and has
   * the meaning that the stream is now finished. In this example, all we do is
   * pass the control back to the transactional layer.
   *
   * @param r The record we are working on
   * @return The processed record
   * @throws ProcessingException
   */
  @Override
  public TrailerRecord procTrailer(TrailerRecord r) throws ProcessingException {
    return r;
  }
}
