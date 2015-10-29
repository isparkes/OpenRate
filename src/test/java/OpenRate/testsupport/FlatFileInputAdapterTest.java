package OpenRate.testsupport;

import OpenRate.adapter.file.FlatFileInputAdapter;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;

/**
 * Very basic flat file input adapter for test purposes. Reads FlatRecords in
 * and passes them on nearly unmodified (only record number is set) for 
 * processing.
 *
 * @author ian
 */
public class FlatFileInputAdapterTest
        extends FlatFileInputAdapter {

  //  This is the stream record number counter which tells us the number of the compressed records
  private int streamRecordNumber;

  @Override
  public HeaderRecord procHeader(HeaderRecord r) {
    // get the stream name and log it
    String tmpStreamName = r.getStreamName();
    getPipeLog().info("Opening file " + tmpStreamName);

    // reset the record numbering
    streamRecordNumber = 0;

    return r;
  }

  @Override
  public IRecord procValidRecord(FlatRecord r) {
    getPipeLog().info("Got valid record with data: " + r.getData());
    
    // Number the record
    r.recordNumber = streamRecordNumber;
    streamRecordNumber++;

    return (IRecord) r;
  }

  @Override
  public IRecord procErrorRecord(FlatRecord r) {
    // The FlatFileInputAdapter is not able to create error records, so we
    // do not have to do anything for this
    return r;
  }

  @Override
  public TrailerRecord procTrailer(TrailerRecord r) {
    r.setRecordCount(streamRecordNumber);

    // get the stream name and log it
    String tmpStreamName = r.getStreamName();
    getPipeLog().info("Opening file " + tmpStreamName);
    
    return r;
  }
}
