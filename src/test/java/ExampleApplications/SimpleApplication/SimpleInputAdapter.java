/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * Tiger Shore Management or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */
package ExampleApplications.SimpleApplication;

import OpenRate.adapter.file.FlatFileInputAdapter;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;

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
  extends FlatFileInputAdapter
{
  private int intRecordNumber;

 /**
  * Constructor for SimpleInputAdapter.
  */
  public SimpleInputAdapter()
  {
    super();
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------
 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting. In this example we have nothing to do
  */
  @Override
  public IRecord procHeader(IRecord r)
  {
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
  * (that is records with sub records) and placing them in the pipeline as
  * they are completed. If you receive a sub record, simply return a null record
  * in this method to indicate that you are handling it, and that it will be
  * purged at a later date.
  */
  @Override
  public IRecord procValidRecord(IRecord r)
  {
    String tmpData;
    SimpleRecord tmpDataRecord = null;
    FlatRecord tmpFlatRecord;

   /* The source of the record is FlatRecord, because we are using the
    * FlatFileInputAdapter as the source of the records. We cast the record
    * to this to extract the data, and then create the target record type
    * (SimpleRecord) and cast this back to the generic class before passing
    * back
    */
    tmpFlatRecord = (FlatRecord)r;

    // Get the data we are goingt to work on from the input record
    tmpData = tmpFlatRecord.getData();

    // Determine if there is anything to do (if it is a detail record) and if
    // there is, do it, otherwise leave things as they are.
    // For this application, it is enough to know that the record is
    // not a detail, so if it is not "020" - "060" or "1xx" OR "2xx", ignore it
    if (tmpData.startsWith("02") | tmpData.startsWith("03") |
        tmpData.startsWith("04") | tmpData.startsWith("05") |
        tmpData.startsWith("06") | tmpData.startsWith("1") |
        tmpData.startsWith("2"))
    {
      // Create the container record for processing
      tmpDataRecord = new SimpleRecord();

      // Set the data into the record
      tmpDataRecord.setOriginalData(tmpData);

      // Map the data, parsing it into the fields
      tmpDataRecord.mapData();

      // set the record number
      intRecordNumber++;
      tmpDataRecord.RecordNumber = intRecordNumber;
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
  */
  @Override
  public IRecord procErrorRecord(IRecord r)
  {
    // The FlatFileInputAdapter is not able to create error records, so we
    // do not have to do anything for this
    return r;
  }

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished. In this example, all we do is
  * pass the control back to the transactional layer.
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
    // Nothing needed here
    return r;
  }
}
