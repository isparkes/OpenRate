
package ExampleApplications.SimpleApplication;

import OpenRate.adapter.file.FlatFileInputAdapter;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;

/**
 * This class is an example of how one would write an InputAdapter. An input
 * adapter implements a single loadBatch() method that selects a set of work and
 * returns it. The framework takes the Collection & pushes it into the first
 * channel where the pipeline begins work on it. Typically the loadBatch()
 * method would read records from a Messaging System, or a DB Table, or a flat
 * file, but in this simplest possible case, it just create a bunch of records
 * out of thin air and passes them on to the pipeline.
 */
public class SimpleInputAdapter
        extends FlatFileInputAdapter {

  private int intRecordNumber;

  /**
   * Constructor for SimpleInputAdapter.
   */
  public SimpleInputAdapter() {
    super();
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------
  /**
   * This is called when the synthetic Header record is encountered, and has the
   * meaning that the stream is starting. In this example we have nothing to do
   *
   * @return
   */
  @Override
  public HeaderRecord procHeader(HeaderRecord r) {
    // Reset the internal record number counter - we are starting a new stream
    intRecordNumber = 0;

    // Nothing else to do - return
    return r;
  }

  /**
   * This is called when a data record is encountered. You should do any normal
   * processing here. For the input adapter, we probably want to change the
   * record type from FlatRecord to the record(s) type that we will be using in
   * the processing pipeline.
   *
   * This is also the location for accumulating records into logical groups
   * (that is records with sub records) and placing them in the pipeline as they
   * are completed. If you receive a sub record, simply return a null record in
   * this method to indicate that you are handling it, and that it will be
   * purged at a later date.
   *
   * @return
   */
  @Override
  public IRecord procValidRecord(FlatRecord r) {
    String tmpData;
    SimpleRecord tmpDataRecord = null;

    /* The source of the record is FlatRecord, because we are using the
     * FlatFileInputAdapter as the source of the records. We cast the record
     * to this to extract the data, and then create the target record type
     * (SimpleRecord) and cast this back to the generic class before passing
     * back
     */
    // Get the data we are going to work on from the input record
    tmpData = r.getData();
    
    // Determine if there is anything to do (if it is a detail record) and if
    // there is, do it, otherwise leave things as they are.
    // For this application, it is enough to know that the record is
    // not a detail, so if it is not "020" - "060" or "1xx" OR "2xx", ignore it
    if (tmpData.startsWith("02") | tmpData.startsWith("03")
            | tmpData.startsWith("04") | tmpData.startsWith("05")
            | tmpData.startsWith("06") | tmpData.startsWith("1")
            | tmpData.startsWith("2")) {
      // Create the container record for processing
      tmpDataRecord = new SimpleRecord();

      // Set the data into the record
      tmpDataRecord.setOriginalData(tmpData);

      // Map the data, parsing it into the fields
      tmpDataRecord.mapData();

      // set the record number
      intRecordNumber++;
      tmpDataRecord.recordNumber = intRecordNumber;
    }

    // Return the modified record in the Common record format (IRecord)
    return (IRecord) tmpDataRecord;
  }

  /**
   * This is called when a data record with errors is encountered. You should do
   * any processing here that you have to do for error records, e.g. statistics,
   * special handling, even error correction!
   *
   * The input adapter is not expected to provide any records here.
   *
   * @return
   */
  @Override
  public IRecord procErrorRecord(FlatRecord r) {
    // The FlatFileInputAdapter is not able to create error records, so we
    // do not have to do anything for this
    return r;
  }

  /**
   * This is called when the synthetic trailer record is encountered, and has
   * the meaning that the stream is now finished. In this example, all we do is
   * pass the control back to the transactional layer.
   *
   * @return
   */
  @Override
  public TrailerRecord procTrailer(TrailerRecord r) {
    // Nothing needed here
    return r;
  }
}
