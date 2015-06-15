/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
