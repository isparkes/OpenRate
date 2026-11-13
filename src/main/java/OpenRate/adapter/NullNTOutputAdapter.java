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
package OpenRate.adapter;

import OpenRate.record.IRecord;
import java.io.IOException;
import java.util.Collection;

/**
 * The Output Adapter is responsible for persisting the completed records to the
 * target data store. The target data store may be a message queue, a database
 * table, or a flat file. It is completely up the implementor to determine where
 * to save the records.
 *
 * This utility output adapter simply discards all output records without doing
 * any processing on them, and is useful as a generic record "sink" at the end
 * of a pipeline.
 */
public class NullNTOutputAdapter extends AbstractNTOutputAdapter {

  /**
   * closeStream() is called by the pipeline to finish off any streaming that
   * might be required, such as closing writers etc.
   *
   * @param TransactionNumber The transaction we are working on
   */
  @Override
  public void closeStream(int TransactionNumber) {
    // Nothing to do
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
  public IRecord procHeader(IRecord r) {
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
  @Override
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
  @Override
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
  public IRecord procTrailer(IRecord r) {
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
   * @throws java.io.IOException
   */
  @Override
  public IRecord prepValidRecord(IRecord r) throws IOException {
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
   * @throws java.io.IOException
   */
  @Override
  public IRecord prepErrorRecord(IRecord r) throws IOException {
    return r;
  }
}
