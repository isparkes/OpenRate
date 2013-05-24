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
 * any processing on them, and is useful as a generic record "sink" at the
 * end of a pipeline.
 */
public class NullNTOutputAdapter extends AbstractNTOutputAdapter
{
 /**
  * closeStream() is called by the pipeline to finish off any streaming that
  * might be required, such as closing writers etc.
  *
  * @param TransactionNumber The transaction we are working on
  */
  @Override
  public void closeStream(int TransactionNumber)
  {
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
  public IRecord procHeader(IRecord r)
  {
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
  public Collection<IRecord> procValidRecord(IRecord r)
  {
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
  public Collection<IRecord> procErrorRecord(IRecord r)
  {
    return null;
  }

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished. This returns void, because we do
  * not write stream headers, thus this is for information to the implementing
  * module only.
  *
  * This implementation ALWAYS returns null, as it is a generic sink for the
  * end of the pipeline.
  *
  * @param r The record we are working on
  * @return The processed record
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {
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
  public IRecord prepValidRecord(IRecord r) throws IOException
  {
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
  public IRecord prepErrorRecord(IRecord r) throws IOException
  {
    return r;
  }
}
