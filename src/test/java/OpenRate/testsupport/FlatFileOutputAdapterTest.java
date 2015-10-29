package OpenRate.testsupport;

import OpenRate.adapter.file.FlatFileOutputAdapter;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Very basic flat file output adapter for test purposes. Writes the flat
 * records we got to the output, without modifying them.
 *
 * @author ian
 */
public class FlatFileOutputAdapterTest extends FlatFileOutputAdapter {

  @Override
  public Collection<FlatRecord> procValidRecord(IRecord r) {
    Collection<FlatRecord> outbatch = new ArrayList<>();
    
    // just add the unmodified record to the out batch
    // Normally we will do some processing or transformation here, but not in
    // this test
    outbatch.add((FlatRecord) r);

    return outbatch;
  }

  @Override
  public Collection<FlatRecord> procErrorRecord(IRecord r) {
    Collection<FlatRecord> outbatch = new ArrayList<>();
    
    // just add the unmodified record to the out batch
    // Normally we will do some processing or transformation here, but not in
    // this test
    outbatch.add((FlatRecord) r);

    return outbatch;
  }
}
