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
  public IRecord procHeader(IRecord r) {
    // get the stream name and log it
    HeaderRecord header = (HeaderRecord) r;
    String tmpStreamName = header.getStreamName();
    getPipeLog().info("Opening file " + tmpStreamName);

    // reset the record numbering
    streamRecordNumber = 0;

    return r;
  }

  @Override
  public IRecord procValidRecord(IRecord r) {
    FlatRecord currentRecord = (FlatRecord) r;
    getPipeLog().info("Got valid record with data: " + currentRecord.getData());
    
    // Number the record
    currentRecord.RecordNumber = streamRecordNumber;
    streamRecordNumber++;

    return (IRecord) currentRecord;
  }

  @Override
  public IRecord procErrorRecord(IRecord r) {
    // The FlatFileInputAdapter is not able to create error records, so we
    // do not have to do anything for this
    return r;
  }

  @Override
  public IRecord procTrailer(IRecord r) {
    TrailerRecord trailer = (TrailerRecord) r;
    trailer.setRecordCount(streamRecordNumber);

    // get the stream name and log it
    String tmpStreamName = trailer.getStreamName();
    getPipeLog().info("Opening file " + tmpStreamName);
    
    return r;
  }
}
