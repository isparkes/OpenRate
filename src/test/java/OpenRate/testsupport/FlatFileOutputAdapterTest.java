/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
