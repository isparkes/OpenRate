/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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
 * The OpenRate Project or its officially assigned agents be liable to any
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

import OpenRate.record.RatingRecord;
import java.util.ArrayList;

/**
 * A Record corresponds to a unit of work that is being processed by the
 * pipeline. Records are created in the InputAdapter, pass through the Pipeline,
 * and written out in the OutputAdapter. Any stage of the pipeline my update
 * the record in any way, provided that later stages in the processing and the
 * output adapter know how to treat the record they receive.
 *
 * As an alternative, you may define a less flexible record format as you wish
 * and fill in the fields as required, but this costs performance.
 *
 * Generally, the record should know how to handle the following operations by
 * linking the appropriate method:
 *
 * mapOriginalData()   [mandatory]
 * -----------------
 * Transformation from a flat record as read by the input adapter to a formatted
 * record.
 *
 * unmapOriginalData() [mandatory if you wish to write output files]
 * -------------------
 * Transformation from a formatted record to a flat record ready for output.
 *
 * getDumpInfo()       [optional]
 * -------------
 * Preparation of the dump equivalent of the formatted record, ready for
 * dumping out to a dump file. The OpenRate.process.Dump module uses this
 * information.
 *
 * In this simple example, we require only to read the "B-Number", and write the
 * "Destination" as a result of this. Because of the simplicity of the example
 * we do not perform a full mapping, we just handle the fields we want directly,
 * which is one of the advantages of the BBPA model (map as much as you want or
 * as little as you have to).
 *
 */
public final class SimpleRecord extends RatingRecord
{
 /**
  *
  */
  private static final long serialVersionUID = 1L;

  // These are the mappings to the fields we are going to be using
  public static final int B_NUMBER_IDX = 16;
  public static final int DESTINATION_IDX = 17;

  // The detail record type tells us that this a record which we will manipulate
  // Standard values are:
  //   10 - Header Record
  //   20 - Detail Record
  //   90 - Trailer Record
  // Different sorts of detail records are usually mapped in the range 20-89
  public static final int DETAIL_RECORD = 20;

  // Worker variables to save references during processing. We are using the
  // B-Number to look up the destination.
  public String B_Number = null;
  public String Destination = null;

 /**
  * Default Constructor for SimpleRecord.
  */
  public SimpleRecord()
  {
    super();
  }

 /**
  * Overloaded Constructor for SimpleRecord.
  *
  * @param OriginalData - the flat record we are to map
  */
  public SimpleRecord(String OriginalData)
  {
    super();

    // Set the data we received
    this.setOriginalData(OriginalData);
  }

  /**
   * We split up the record at the tabs, and put the information into fields
   * so that we can manipulate it as we want. For the purposes of this example
   * we only need the B Number field. In general we put often used values in
   * local working variables in the record, for speed any clarity of code.
   */
  public void mapData()
  {

    this.fields = this.getOriginalData().split("\\t");

    // Pull out the B-Number and make it easy to access for the lookup. Note
    // that we don't have to do this, we could just as easily leave it where
    // it is and extract it at the time of the lookup, but doing it this way
    // makes the code more readable and potetially faster.
    B_Number = getField(B_NUMBER_IDX);

    // Set the record type id - this is used to manage the processing
    RECORD_TYPE = DETAIL_RECORD;
  }

  /**
   * Reconstruct the record from the field values, replacing the original
   * structure of tab separated records
   *
   * @return The unmapped original data
   */
  public String unmapOriginalData()
  {

    int NumberOfFields;
    int i;
    StringBuilder tmpReassemble;

    if (this.RECORD_TYPE == DETAIL_RECORD)
    {
      // We use the string buffer for the reassembly of the record. Avoid
      // just concatenating strings, as it is a LOT slower because of the
      // java internal string handling (it has to allocate/deallocate many
      // times to rebuild the string).
      tmpReassemble = new StringBuilder(1024);

      // write the destination information back
      this.setField(DESTINATION_IDX, Destination);
      NumberOfFields = this.fields.length;

      for (i = 0; i < NumberOfFields; i++)
      {

        if (i == 0)
        {
          tmpReassemble.append(this.fields[i]);
        }
        else
        {
          tmpReassemble.append("\t");
          tmpReassemble.append(this.fields[i]);
        }
      }

      return tmpReassemble.toString();
    }
    else
    {
      // just return the untampered with original
      return this.getOriginalData();
    }
  }

  /**
   * Return the dump-ready data
   *
   * @return The dump info strings
   */
  @Override
  public ArrayList<String> getDumpInfo()
  {

    ArrayList<String> tmpDumpList;
    tmpDumpList = new ArrayList<>();

    // Format the fields
    if (this.RECORD_TYPE == DETAIL_RECORD)
    {
      tmpDumpList.add("============ BEGIN RECORD ============");
      tmpDumpList.add("  Record Number         = <" + this.recordNumber + ">");
      tmpDumpList.add("--------------------------------------");
      tmpDumpList.add("  B Number              = <" + this.B_Number + ">");
      tmpDumpList.add("  Destination           = <" + this.Destination + ">");

      // Use the standard function to get the error information
      tmpDumpList.addAll(this.getErrorDump());
    }

    return tmpDumpList;
  }

  public Object getSourceKey()
  {
    return null;
  }
}
