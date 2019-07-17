package OpenRate.adapter;

import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;
import java.util.Collection;

/**
 * The Null Output Adapter sends the records it receives to the bit bucket,
 * where they are destroyed. This is the equivalent of the /dev/null output.
 *
 * Note that even though this null output adapter is a transactional module, we
 * do not add it as a client to the transaction manager. This is because the
 * null output adapter can never have a problem closing a transaction, and we
 * therefore just do not manage it.
 */
public class NullOutputAdapter
        extends AbstractOutputAdapter {

  /**
   * closeStream() is called by the pipeline to finish off any streaming that
   * might be required, such as closing writers etc.
   *
   * @param TransactionNumber The transaction we are working on
   */
  @Override
  public void closeStream(int TransactionNumber) {
    // Do nothing
  }

  /**
   * This is called when the synthetic Header record is encountered, and has the
   * meaning that the stream is starting. This is for information to the
   * implementing module only, and need not be hooked, as it is handled
   * internally by the child class.
   *
   * This implementation ALWAYS returns null, as it is a generic sink for the
   * end of the pipeline.
   *
   * @param r The record we are working on
   * @return The processed record
   */
  @Override
  public HeaderRecord procHeader(HeaderRecord r) {
    return null;
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here. Note that the result is a collection for the case that we
   * have to re-expand after a record compression input adapter has done
   * compression on the input stream.
   *
   * This implementation ALWAYS returns null, as it is a generic sink for the
   * end of the pipeline.
   *
   * @param r The record we are working on
   * @return The collection of processed records
   */
  public Collection<IRecord> procValidRecord(IRecord r) {
    return null;
  }

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * This implementation ALWAYS returns null, as it is a generic sink for the
   * end of the pipeline.
   *
   * @param r The record we are working on
   * @return The collection of processed records
   */
  public Collection<IRecord> procErrorRecord(IRecord r) {
    return null;
  }

  /**
   * This is called when the synthetic trailer record is encountered, and has
   * the meaning that the stream is now finished. This returns void, because we
   * do not write stream headers, thus this is for information to the
   * implementing module only.
   *
   * This implementation ALWAYS returns null, as it is a generic sink for the
   * end of the pipeline.
   *
   * @param r The record we are working on
   * @return The processed record
   */
  @Override
  public TrailerRecord procTrailer(TrailerRecord r) {
    return null;
  }

  /**
   * Prepare the current (valid) record for outputting. The prepValidRecord
   * calls the procValidRecord() method for the record, and then writes the
   * resulting records to the output file one at a time. This is the "record
   * expansion" part of the "record compression" strategy.
   *
   * @param r The current record we are working on
   * @return The prepared record
   */
  @Override
  public IRecord prepValidRecord(IRecord r) {
    return r;
  }

  /**
   * Prepare the current (error) record for outputting. The prepValidRecord
   * calls the procValidRecord() method for the record, and then writes the
   * resulting records to the output file one at a time. This is the "record
   * expansion" part of the "record compression" strategy.
   *
   * @param r The current record we are working on
   * @return The prepared record
   */
  @Override
  public IRecord prepErrorRecord(IRecord r) {
    return r;
  }
}
