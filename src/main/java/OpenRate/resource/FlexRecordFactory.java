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
package OpenRate.resource;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.record.flexRecord.FlexRecord;
import OpenRate.utils.PropertyUtils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


/**
 * FlexRecordFactory class manages the creation of records with a defined
 * Business Oriented structure. The mappings of the fields and hierarchy
 * are loaded at system startup, and input plug-ins request a copy of the record
 * for use as a template during input. This allows the abstraction of the
 * external data format to the internal format. It does however have a performance
 * cost, but is useful for situations where the business user cannot accept
 * programming to define a record.
 *
 * This has been created primarily for a rules engine implementation.
 */
public class FlexRecordFactory implements IResource
{
  // This is the symbolic name of the resource
  private String symbolicName;

  /**
   * key used by ResourceContext to find FlexRecordFactory type
   */
  public static final String KEY = "FlexRecordFactory";

  // This is the master record definition that we will be constructing
  private FlexRecord MasterRecord;
  
  /**
   * Constructor
   */
  public FlexRecordFactory()
  {
    super();
    MasterRecord = new FlexRecord("ROOT",0);
  }

  /**
   * This init method will be called while Resources are being registered and
   * initialised.  It obtains all the cacheable objects calls the loading
   * methods before adding into the CacheManagers.
   *
   * The record definition is like this:
   * BLOCKPATH;BLOCK;NEWBLOCKNAME;FIELDCOUNT; defines a new block
   * BLOCKPATH;FIELD;FIELDNUMBER;FIELDNAME;FIELDTYPE; defines a new field in the block
   *
   * Thus to define a new FlexRecord template with a hierarchy:
   *
   * CUSTOMER (Block)
   *  |
   *  + -- NAME (String)
   *  |
   *  + -- AGE (int)
   *  |
   *  + -- CHILDREN (Block)
   *        |
   *        + CHILDNAME (String)
   *        |
   *        + CHILDSEX (String)
   *        |
   *        + CHILDAGE (int)
   *
   * we would have the following configuration:
   *
   * ROOT;BLOCK;ROOT;2
   * ROOT;FIELD;1;NAME;STRING
   * ROOT;FIELD;2;AGE;INTEGER
   * ROOT;FIELD;2;AGE;INTEGER
   * ROOT;BLOCK;CHILDREN;3
   * ROOT.CHILDREN;FIELD;1;CHILDNAME;STRING
   * ROOT.CHILDREN;FIELD;2;CHILDSEX;STRING
   * ROOT.CHILDREN;FIELD;3;CHILDAGE;INTEGER
   *
   * The FlexRecordFactory also performs mappings from an external record
   * to a FlexRecord.
   *
   * A map definition is composed of any number of
   * BLOCKPATH;MAP;OFFSET;MAPTOFIELD
   *
   * A separator is also defined
   * MAPNAME;SEPARATOR;";"
   *
   * There will be a "MAP" entry for every field in the input record that
   * you wish to map. Data you do not wish to map can be ignored. The entry
   * "MAPTOFIELD" is the integer offset of the field in the input record.
   */
  @Override
  public void init(String ResourceName)
            throws InitializationException
  {
    // Pick each from config table and load for each.
    String         tmpRecordDefinitionName;
    BufferedReader inFile;
    int            FileLine = 0;
    int            Command;
    String         tmpFileRecord;
    String[]       DefinitionLine;
    int            tmpFieldCount;
    int            tmpFieldOffset;

    // here we go
    OpenRate.getOpenRateFrameworkLog().info("Starting FlexRecordFactory initialisation");

    // Set the symbolic name
    symbolicName = ResourceName;

    tmpRecordDefinitionName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName, "RecordDefintion",
                                                                "None");

    if (tmpRecordDefinitionName.equals("None"))
    {
      // we found no record definition file. Crash and burn
      throw new InitializationException("RecordDefinition file name not found",getSymbolicName());
    }

    // Now try to open the definition file, and work on it
    try
    {
      inFile = new BufferedReader(new FileReader(tmpRecordDefinitionName));
    }
    catch (FileNotFoundException exFileNotFound)
    {
            OpenRate.getOpenRateFrameworkLog().error(
            "Not able to read the record definition file : <" +
            tmpRecordDefinitionName + ">");
      throw new InitializationException("Not able to read the record definition file : <" +
                                        tmpRecordDefinitionName + ">",
                                        exFileNotFound,getSymbolicName());
    }

    // File open, now get the stuff
    try
    {
      while (inFile.ready())
      {
        tmpFileRecord = inFile.readLine();
        FileLine++;

        if ((tmpFileRecord.startsWith("#")) |
            tmpFileRecord.trim().equals(""))
        {
          // Comment line, or blank line. Ignore
        }
        else
        {
          DefinitionLine = tmpFileRecord.split(";");

          Command = 0;

          if (DefinitionLine[1].equalsIgnoreCase("BLOCK"))
          {
            Command = 1;
          }

          if (DefinitionLine[1].equalsIgnoreCase("FIELD"))
          {
            Command = 2;
          }

          if (DefinitionLine[1].equalsIgnoreCase("MAP"))
          {
            Command = 3;
          }

          if (DefinitionLine[1].equalsIgnoreCase("SEPARATOR"))
          {
            Command = 4;
          }

          switch (Command)
          {
            //case "BLOCK":
            case 1:
            {
              // A new block - create the block
              DefinitionLine = tmpFileRecord.split(";",4);
              tmpFieldCount = Integer.parseInt(DefinitionLine[3]);
              MasterRecord.AddBlockDef(DefinitionLine[0],DefinitionLine[2],tmpFieldCount);
            }
            break;

           //case "FIELD":
            case 2:
            {
              DefinitionLine = tmpFileRecord.split(";",5);
              tmpFieldCount = Integer.parseInt(DefinitionLine[2]);
              MasterRecord.AddFieldDef(DefinitionLine[0],DefinitionLine[3],tmpFieldCount,DefinitionLine[4]);
            }
            break;

           //case "MAP":
            case 3:
            {
              DefinitionLine = tmpFileRecord.split(";",4);
              tmpFieldOffset = Integer.parseInt(DefinitionLine[2]);
              MasterRecord.AddMappingDef(DefinitionLine[0],tmpFieldOffset,DefinitionLine[3]);
            }
            break;

           //case "SEPARATOR":
            case 4:
            {
              DefinitionLine = tmpFileRecord.split(";",3);
              MasterRecord.MapSeparatorDef(DefinitionLine[0],DefinitionLine[2]);
            }
            break;
          }
        }
      }
    }
    catch (IOException ex)
    {
            OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + tmpRecordDefinitionName +
            "> in record <" + FileLine + ">. IO Error.");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
            OpenRate.getOpenRateFrameworkLog().fatal(
            "Error reading input file <" + tmpRecordDefinitionName +
            "> on line <" + FileLine + ">. Malformed Record.");
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
                OpenRate.getOpenRateFrameworkLog().error("Error closing input file <" + tmpRecordDefinitionName +
                  ">", ex);
      }
    }

        OpenRate.getOpenRateFrameworkLog().info("FlexRecordFactory initialised");
  }

  /**
   * As part of Resource object it cleans up the CacheManagers
   * which in turn will make cached data garbage collected.
   */
  @Override
  public void close()
  {
  }

  /**
   * Create a new flex record
   *
   * @return The new instance
   */
  public FlexRecord CreateNewFlexRecord()
  {
    FlexRecord tmpFlexRecord = new FlexRecord("ROOT",0);

    // Copy the contents of the definitions
    tmpFlexRecord.setDefRoot(MasterRecord.getDefRoot());

    return tmpFlexRecord;
  }

  /**
   * Get the reference to the factory
   *
   * @return The record factory
   * @throws ConfigurationException
   */
  public static FlexRecordFactory GetFlexRecordFactory()
    throws InitializationException
  {
    FlexRecordFactory   FR;
    ResourceContext ctx = new ResourceContext();

    // try the new Logging model.
    FR = (FlexRecordFactory)ctx.get(FlexRecordFactory.KEY);

    // return the value we got
    return FR.getReference();
  }

 /**
  * This is a way of returning a non-static reference via a static call
  */
  private FlexRecordFactory getReference()
  {
    return this;
  }

 /**
  * Return the resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }
}
