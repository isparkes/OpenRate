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

import OpenRate.adapter.realTime.AbstractRTAdapter;
import OpenRate.exception.ProcessingException;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;

/**
 *
 * @author ian
 */
public class NullRTAdapter extends AbstractRTAdapter {

  @Override
  public IRecord performInputMapping(FlatRecord RTRecordToProcess) throws ProcessingException {
    return RTRecordToProcess;
  }

  @Override
  public FlatRecord performValidOutputMapping(IRecord RTRecordToProcess) throws ProcessingException {
    return (FlatRecord) RTRecordToProcess;
  }

  @Override
  public FlatRecord performErrorOutputMapping(IRecord RTRecordToProcess) {
    return (FlatRecord) RTRecordToProcess;
  }

  @Override
  public IRecord procInputValidRecord(IRecord r) throws ProcessingException {
    return r;
  }

  @Override
  public IRecord procInputErrorRecord(IRecord r) throws ProcessingException {
    return r;
  }

  @Override
  public IRecord procOutputValidRecord(IRecord r) {
    return r;
  }

  @Override
  public IRecord procOutputErrorRecord(IRecord r) {
    return r;
  }
  
}
