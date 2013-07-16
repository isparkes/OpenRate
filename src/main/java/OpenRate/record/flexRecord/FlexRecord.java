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

package OpenRate.record.flexRecord;

import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.record.AbstractRecord;
import OpenRate.record.ErrorType;
import OpenRate.record.IError;
import OpenRate.record.RecordError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A flex record is a type of record that is used in situations where we want
 * to decouple the record definition from the code. We provide a configuration
 * file for the record hierarchy and methods to create the record template
 * and fill it.
 *
 * The structure of a flex record is divided into two parts:
 * 1) The definition of the structure. This is a hierarchical list of the
 *    structure of the records, divided into blocks. This is more or less static
 *    once the flex record has been defined
 * 2) The instances of the blocks that have been created. This is dynamic and
 *    changes as new block instances are created. However, for speed there is a
 *    trick that we perform here. The actual fields of the record are not stored
 *    in a hierarchical manner, only the *indexes* are. The actual fields of the
 *    records are stored in flat vectors.
 */
public class FlexRecord extends AbstractRecord
{
  private static final long serialVersionUID = -4654007386889558251L;

 /**
  * Data type string
  */
  public final static int FIELD_TYPE_STRING  = 0;

 /**
  * Data type integer
  */
  public final static int FIELD_TYPE_INTEGER = 1;

 /**
  * Data type float
  */
  public final static int FIELD_TYPE_FLOAT   = 2;
  
  // The symbolic module name of the class stack
  private String symbolicName = "FlecRecord";

  /**
   * @return the defRoot
   */
  public RecordBlockDef getDefRoot() {
    return defRoot;
  }

  /**
   * @param defRoot the defRoot to set
   */
  public void setDefRoot(RecordBlockDef defRoot) {
    this.defRoot = defRoot;
  }

    /**
     * @return the symbolicName
     */
    public String getSymbolicName() {
        return symbolicName;
    }

 /**
  * This is the implementation of the record structure. The structure has been
  * defined in the Definition class, and this holds the data once the instances
  * of the blocks have been created.
  */
  protected class RecordBlock
  {
    // Holds the fields
    Object[] Fields;

    // Holds the access data to the fields
    HashMap<String, Integer> FieldMap;
  }

  /**
   * Maps the field information
   */
  public class FieldInfo
  {
    String FieldName;
    int    FieldType;
    Object FieldValue;
  }

  // This is the index to allow us to find the mapping quickly. This takes the
  // name of the block and returns the map info, the current number of blocks
  // of that type that have been created and the index to the block object
  HashMap<String, RecordBlock> BlockIndex;

  // This is the index to allow us to find the mapping quickly. This takes the
  // name of the block and returns the map info, the current number of blocks
  // of that type that have been created and the index to the block object
  HashMap<String, Integer> BlockCount;

  /**
   * This is the root block
   */
  private RecordBlockDef defRoot;

 /**
  * Creates a new instance of FlatRecord
  *
  * @param RootName The name of the root
  * @param FieldCount The number of fields
  */
  public FlexRecord(String RootName, int FieldCount)
  {
    super();

    // Prepare the block index
    BlockIndex = new HashMap<>(10);
    BlockCount = new HashMap<>(10);
  }

  private RecordBlockDef FindBlock(String BlockName) throws InitializationException
  {
    int            i;
    RecordBlockDef tmpRecordBlock = null;
    String[]       tmpBlockPath;
    String         tmpRootBlock;
    String         tmpPathSoFar;

    // force correct splitting
    tmpRootBlock = BlockName + ".";

    // Find the block that we are working on
    tmpBlockPath = tmpRootBlock.split("\\.");

    if (!tmpBlockPath[0].equalsIgnoreCase("ROOT"))
    {
      throw new InitializationException("Block definition must begin with ROOT.",getSymbolicName());
    }
    else
    {
      tmpRecordBlock = getDefRoot();
      tmpPathSoFar = "ROOT";

      // find the correct record block to work on
      for (i = 1 ; i < tmpBlockPath.length ; i++)
      {
        tmpPathSoFar = tmpPathSoFar + ":" + tmpBlockPath[i];

        if (tmpRecordBlock.ChildTemplates.containsKey(tmpPathSoFar))
        {
          // Found definition - get it
          tmpRecordBlock = (RecordBlockDef) tmpRecordBlock.ChildTemplates.get(tmpPathSoFar);
        }
        else
        {
          throw new InitializationException("Cannot find path to <" + BlockName + ">",getSymbolicName());
        }
      }
    }

    return tmpRecordBlock;
  }

 /**
  * Adds a field definition to a block. The number of fields have already been
  * set during block creation.
  *
  * @param RootBlockName The name of the root block
  * @param NewFieldName The new field name
  * @param FieldNumber The field number
  * @param FieldType The field type
  * @throws InitializationException
  */
  public void AddFieldDef(String RootBlockName, String NewFieldName, int FieldNumber, String FieldType) throws InitializationException
  {
    RecordBlockDef tmpRecordBlock;
    Boolean     FieldTypeFound = false;
    int         tmpFieldNumber;

    tmpFieldNumber = FieldNumber - 1;

    tmpRecordBlock = FindBlock(RootBlockName);

    if (tmpRecordBlock != null)
    {
      if ((tmpFieldNumber < 0) | (FieldNumber>tmpRecordBlock.NumberOfFields))
      {
        throw new InitializationException("Field Index <" + FieldNumber + "> invalid for block <" + RootBlockName + ">",getSymbolicName());
      }

      if (tmpRecordBlock.FieldNames[tmpFieldNumber] != null)
      {
        throw new InitializationException("Field Index <" + FieldNumber + "> overwrites existing definition <" + tmpRecordBlock.FieldNames[tmpFieldNumber] + "> in block <" + RootBlockName + ">",getSymbolicName());
      }

      // Add the field definition
      tmpRecordBlock.FieldNames[tmpFieldNumber] = NewFieldName;

      // parse the field type
      if (FieldType.equalsIgnoreCase("STRING"))
      {
        tmpRecordBlock.FieldTypes[tmpFieldNumber] = FIELD_TYPE_STRING;
        FieldTypeFound = true;
      }

      // parse the field type
      if (FieldType.equalsIgnoreCase("INTEGER"))
      {
        tmpRecordBlock.FieldTypes[tmpFieldNumber] = FIELD_TYPE_INTEGER;
        FieldTypeFound = true;
      }

      // parse the field type
      if (FieldType.equalsIgnoreCase("FLOAT"))
      {
        tmpRecordBlock.FieldTypes[tmpFieldNumber] = FIELD_TYPE_FLOAT;
        FieldTypeFound = true;
      }

      if (!FieldTypeFound)
      {
        throw new InitializationException("Cannot find field type <" + FieldType + ">",getSymbolicName());
      }

      tmpRecordBlock.FieldNameIndex.put(RootBlockName + "." + NewFieldName,tmpFieldNumber);
    }
    else
    {
      throw new InitializationException("Cannot find block <" + RootBlockName + ">",getSymbolicName());
    }
  }

 /**
  * Adds a block definition to a block. This function is mostly validation, and
  * calls AddRecordBlockDef to do the real work
  *
  * @param RootBlockName The name of the root block
  * @param NewBlockName The name of the new block
  * @param FieldCount The number of fields
  * @throws InitializationException
  */
  public void AddBlockDef(String RootBlockName, String NewBlockName, int FieldCount)
    throws InitializationException
  {
    RecordBlockDef tmpRecordBlock;
    RecordBlockDef ParentBlock = null;

    if (!NewBlockName.equalsIgnoreCase("ROOT"))
    {
      ParentBlock = FindBlock(RootBlockName);

      // check the credentials of the parent block
      if (ParentBlock != null)
      {
        // now we should have the correct block to work on, so do it
        if (ParentBlock.ChildTemplates.containsKey(NewBlockName))
        {
          throw new InitializationException("Block <" + NewBlockName + "> already exists in <" + RootBlockName + ">",getSymbolicName());
        }
      }
      else
      {
        throw new InitializationException("Cannot find block <" + RootBlockName + ">",getSymbolicName());
      }
    }

    // Now create the block
    tmpRecordBlock = new RecordBlockDef();

    if (FieldCount > 0)
    {
      tmpRecordBlock.NumberOfFields = FieldCount;
      //tmpRecordBlock.Fields     = new String[FieldCount];
      tmpRecordBlock.FieldNames = new String[FieldCount];
      tmpRecordBlock.FieldTypes = new int[FieldCount];
    }

    // Initialise the child definition map
    tmpRecordBlock.ChildTemplates = new HashMap<>(5);

    // Initialise the child instance map
    //tmpRecordBlock.Children = new HashMap(5);

    // Initialise the child instance map
    tmpRecordBlock.FieldNameIndex = new HashMap<>(10);

    // Create the mapping definition ArrayList
    tmpRecordBlock.Mapping = new ArrayList<>();

    if (NewBlockName.equalsIgnoreCase("ROOT"))
    {
      setDefRoot(tmpRecordBlock);

      // Set the name path
      tmpRecordBlock.BlockName = NewBlockName;
    }
    else
    {
      // Set the name path
      tmpRecordBlock.BlockName = RootBlockName + ":" + NewBlockName;

      ParentBlock.ChildTemplates.put(tmpRecordBlock.BlockName,tmpRecordBlock);
    }
  }

 /**
  * This adds the mapping to a block
  *
  * @param BlockName The block name
  * @param Offset The offset
  * @param FieldName The name of the field
  * @throws InitializationException
  */
  public void AddMappingDef(String BlockName, int Offset, String FieldName) throws InitializationException
  {
    RecordBlockDef tmpRecordBlock;
    MapElement  tmpMapElement;
    boolean     FieldFound = false;
    int         i;

    tmpRecordBlock = FindBlock(BlockName);

    if (tmpRecordBlock != null)
    {
      // Verify that the map is possible
      for (i = 0 ; i < tmpRecordBlock.FieldNames.length ; i++)
      {
        if (FieldName.equalsIgnoreCase(tmpRecordBlock.FieldNames[i]))
        {
          tmpMapElement = new MapElement();
          tmpMapElement.OffsetFrom = Offset;
          tmpMapElement.OffsetTo = i;
          tmpMapElement.Name = FieldName;
          tmpMapElement.Type = tmpRecordBlock.FieldTypes[i];

          // Add the mapping record
          tmpRecordBlock.Mapping.add(tmpMapElement);

          FieldFound = true;
        }
      }
    }
    else
    {
      throw new InitializationException("Cannot find block <" + BlockName + ">",getSymbolicName());
    }

    if (!FieldFound)
    {
      throw new InitializationException("Field name <" + FieldName + "> not found in block <" + BlockName + ">",getSymbolicName());
    }
  }

 /**
  * This adds a definition of the separator for a block
  *
  * @param BlockName The name of the block
  * @param Separator The separator
  * @throws InitializationException
  */
  public void MapSeparatorDef(String BlockName, String Separator) throws InitializationException
  {
    RecordBlockDef tmpRecordBlock;
    String tmpSep;

    tmpRecordBlock = FindBlock(BlockName);

    if (Separator.equalsIgnoreCase("semicolon"))
    {
      tmpSep = ";";
    }
    else
    {
      tmpSep = Separator;
    }

    if (tmpRecordBlock != null)
    {
      tmpRecordBlock.Separator = tmpSep;
    }
    else
    {
      throw new InitializationException("Cannot find block <" + BlockName + ">",getSymbolicName());
    }
  }

 /**
  * This performs the actual mapping of an input record to a block. In the case
  * of the root block, no block instance is created. In the case of child blocks
  * the instance is created according to the definition. The Block Index is
  * maintained in both cases (the root block, although created is not yet in
  * the index)
  *
  * In each case the field indexes are updated, and the field containers are
  * created.
  *
  * @param BlockName The name of the block
  * @param tmpData The data
  * @throws ProcessingException
  */
  public void MapRecord(String BlockName, String tmpData) throws ProcessingException
  {
    MapElement  tmpMapElement;
    RecordBlockDef tmpRecordBlockDef;
    String[]    tmpFields;
    int         i;
    String      tmpCurrentFieldStr;
    int         tmpCurrentFieldInt;
    double      tmpCurrentFieldFloat = 0;
    RecordBlock tmpRecordBlock;
    String      tmpBlockName;
    Integer     tmpBlockCounter;

    // Search for the definition
    try
    {
      tmpRecordBlockDef = FindBlock(BlockName);
    }
    catch (InitializationException ie)
    {
       throw new ProcessingException(ie,"FlexRecord");
    }

    if (tmpRecordBlockDef != null)
    {
      tmpFields = tmpData.split(tmpRecordBlockDef.Separator);

      // check the length of the data we have
      if (tmpFields.length < tmpRecordBlockDef.NumberOfFields)
      {
        throw new ProcessingException("Input data too short for mapping block <" + BlockName + ">","FlexRecord");
      }

      // Create the block
      tmpRecordBlock = new RecordBlock();
      tmpRecordBlock.FieldMap = new HashMap<>(10);
      tmpRecordBlock.Fields = new Object[tmpRecordBlockDef.NumberOfFields];

      // Now try the mapping
      for(i = 0 ; i < tmpRecordBlockDef.Mapping.size() ; i++)
      {
        tmpMapElement = (MapElement) tmpRecordBlockDef.Mapping.get(i);

        switch (tmpMapElement.Type)
        {
          case FIELD_TYPE_STRING:
          {
            // Get the value to map
            tmpCurrentFieldStr = tmpFields[tmpMapElement.OffsetFrom];
            tmpRecordBlock.Fields[tmpMapElement.OffsetTo] = tmpCurrentFieldStr;
            tmpRecordBlock.FieldMap.put(tmpMapElement.Name,tmpMapElement.OffsetTo);
            break;
          }

          case FIELD_TYPE_INTEGER:
          {
            // Get the value to map
            tmpCurrentFieldStr = tmpFields[tmpMapElement.OffsetFrom];
            try
            {
              tmpCurrentFieldInt = Integer.parseInt(tmpCurrentFieldStr);
            }
            catch (NumberFormatException nfe)
            {
              this.addError(new RecordError("Conversion Error",ErrorType.DATA_VALIDATION));
              tmpCurrentFieldInt = 0;
            }

            tmpRecordBlock.Fields[tmpMapElement.OffsetTo] = tmpCurrentFieldInt;
            tmpRecordBlock.FieldMap.put(tmpMapElement.Name,tmpMapElement.OffsetTo);
            break;
          }

          case FIELD_TYPE_FLOAT:
          {
            // Get the value to map
            tmpCurrentFieldStr = tmpFields[tmpMapElement.OffsetFrom];
            try
            {
              tmpCurrentFieldFloat = Double.parseDouble(tmpCurrentFieldStr);
            }
            catch (NumberFormatException nfe)
            {
              this.addError(new RecordError("Conversion Error",ErrorType.DATA_VALIDATION));
            }

            tmpRecordBlock.Fields[tmpMapElement.OffsetTo] = tmpCurrentFieldFloat;
            tmpRecordBlock.FieldMap.put(tmpMapElement.Name,tmpMapElement.OffsetTo);
            break;
          }

        }
      }

      // we have the block, now perform the split and store the data
      if (BlockName.equalsIgnoreCase("ROOT"))
      {
        // we add the root without an index
        BlockIndex.put(BlockName,tmpRecordBlock);

        if (!BlockCount.containsKey(BlockName))
        {
          // ToDo check this
          tmpBlockCounter = 0;
        }
        else
        {
          tmpBlockCounter = BlockCount.get(BlockName);
        }
        tmpBlockCounter++;
        BlockCount.put(BlockName,tmpBlockCounter);
      }
      else
      {
        // we have to know the index of this block
        if (!BlockCount.containsKey(BlockName))
        {
          // ToDo check this
          tmpBlockCounter = 0;
        }
        else
        {
          tmpBlockCounter = BlockCount.get(BlockName);
        }
        tmpBlockName = BlockName + "_" + Integer.toString(tmpBlockCounter);
        BlockIndex.put(tmpBlockName,tmpRecordBlock);
        tmpBlockCounter++;
        BlockCount.put(BlockName,tmpBlockCounter);
      }
    }
    else
    {
      throw new ProcessingException("Cannot find block <" + BlockName + ">","FlexRecord");
    }
  }

 /**
  * Dump the record information
  * We need to iterate through all of the blocks outputting the name and the
  * data associated with the fields
  */
  @Override
  public ArrayList<String> getDumpInfo()
  {
    ArrayList<String> tmpDumpList;
    tmpDumpList = new ArrayList<>();

    if (this != null)
    {
      tmpDumpList.addAll(DumpBlockIter("ROOT",1));
    }

    return tmpDumpList;
  }

 /**
  * This is a recursive way of getting the information from the blocks
  */
  private Collection<String> DumpBlockIter(String StartBlock, int Level)
  {
    RecordBlockDef     tmpRecordBlockDef;
    RecordBlock        tmpRecordBlock;
    int                i;
    Collection<String> tmpChildren;
    Iterator<String>   tmpChildIter;
    int                blockCounter;
    String             tmpChildBlockName;
    Integer            tmpBlockCounter;
    String             currentIndent = "";
    String             PaddedName;
    String             FieldType = null;

    ArrayList<String> tmpDumpList;
    tmpDumpList = new ArrayList<>();

    for (i=0 ; i < Level ; i++)
    {
      currentIndent = currentIndent + "  ";
    }

    // Start the search from the root
    tmpRecordBlockDef = getDefRoot();

    try
    {
      tmpRecordBlockDef = FindBlock(StartBlock);
    }
    catch (InitializationException ex)
    {
      ex.printStackTrace();
    }

    // Output this block
    if (tmpRecordBlockDef != null)
    {
      tmpDumpList.add(currentIndent + "=====<BLOCK=" + StartBlock + ">=====");
      tmpBlockCounter = BlockCount.get(StartBlock);
      for (blockCounter = 0 ; blockCounter < tmpBlockCounter ; blockCounter++)
      {
        // get the data block
        if (StartBlock.equalsIgnoreCase("ROOT"))
        {
          tmpRecordBlock = BlockIndex.get(StartBlock);
        }
        else
        {
          tmpRecordBlock = BlockIndex.get(StartBlock + "_" + Integer.toString(blockCounter));
        }

        for (i = 0 ; i < tmpRecordBlockDef.NumberOfFields ; i++)
        {
          PaddedName = currentIndent + tmpRecordBlockDef.FieldNames[i] + "                                                            ";

          if (tmpRecordBlock.Fields[i] instanceof Integer)
          {
            FieldType = ">  <integer>";
          }

          if (tmpRecordBlock.Fields[i] instanceof String)
          {
            FieldType = ">  <string>";
          }

          if (tmpRecordBlock.Fields[i] instanceof Double)
          {
            FieldType = ">  <float>";
          }

          tmpDumpList.add(PaddedName.substring(1,60) + " = <" + tmpRecordBlock.Fields[i] + FieldType);
        }
      }
    }

    // Now the child record types
    tmpChildren = tmpRecordBlockDef.ChildTemplates.keySet();
    tmpChildIter = tmpChildren.iterator();

    while (tmpChildIter.hasNext())
    {
      tmpChildBlockName = (String) tmpChildIter.next();

      tmpDumpList.addAll(DumpBlockIter(tmpChildBlockName,Level+1));
    }

    return tmpDumpList;
  }

 /**
  * FieldName is of the form ROOT:PATH.NAME
  *
  * @param FieldName The name of the field
  * @return The field info
  */
  public FieldInfo GetFieldInfo(String FieldName)
  {
    RecordBlock    tmpRecordBlock;
    Object tmpField;
    FieldInfo tmpResult;
    String[] SplitName;

    SplitName = FieldName.split("/.");

    tmpRecordBlock = BlockIndex.get(SplitName[0]);

    tmpField = tmpRecordBlock.FieldMap.get(SplitName[1]);

    tmpResult = new FieldInfo();

    if (tmpField instanceof String)
    {
      tmpResult.FieldType = FIELD_TYPE_STRING;
    }

    if (tmpField instanceof Integer)
    {
      tmpResult.FieldType = FIELD_TYPE_INTEGER;
    }

    if (tmpField instanceof Double)
    {
      tmpResult.FieldType = FIELD_TYPE_FLOAT;
    }

    tmpResult.FieldValue = tmpField;
    tmpResult.FieldName = SplitName[1];

    return tmpResult;
  }

  /**
   * Add a single error to the error list for this record
   *
   * @param error The new error to add
   */
  @Override
  public void addError(IError error)
  {
  }

 /**
  * FieldName is of the form ROOT:PATH.NAME
  *
  * @param FieldName The field name
  * @return The float value
  */
  public double GetFieldFloat(String FieldName)
  {
    int tmpFieldIndex;
    RecordBlock    tmpRecordBlock;
    Object tmpField;
    double tmpResult = 0;
    String[] SplitName;

    SplitName = FieldName.split("~");

    tmpRecordBlock = BlockIndex.get(SplitName[0]);

    tmpFieldIndex = ((Number)tmpRecordBlock.FieldMap.get(SplitName[1])).intValue();
    tmpField = tmpRecordBlock.Fields[tmpFieldIndex];

    if (tmpField instanceof Double)
    {
      tmpResult = ((Number)tmpField).doubleValue();
    }
    else if (tmpField instanceof Integer)
    {
      tmpResult = ((Number)tmpField).doubleValue();
    }
    else
    {
      this.addError(new RecordError("Cannot retrieve float value",ErrorType.DATA_VALIDATION));
    }

    return tmpResult;
  }

  /**
   * Put a float field
   *
   * @param FieldName The name of the field
   * @param NewValue The value
   */
  public void PutFieldFloat(String FieldName, double NewValue)
  {
    int tmpFieldIndex;
    RecordBlock    tmpRecordBlock;
    String[] SplitName;

    SplitName = FieldName.split("~");

    tmpRecordBlock = BlockIndex.get(SplitName[0]);

    tmpFieldIndex = ((Number)tmpRecordBlock.FieldMap.get(SplitName[1])).intValue();
    tmpRecordBlock.Fields[tmpFieldIndex] = NewValue;
  }  
}
