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

package OpenRate.cache;

import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.logging.LogUtil;
import OpenRate.transaction.ISyncPoint;
import OpenRate.utils.PropertyUtils;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * The aggregation cache is used to produce aggregation results against a set
 * of keys over a stream of records. The results can be purged using the
 * "writeResults" method.
 *
 * This has got more complicated with the introduction of "overlayed transaction
 * handling" (processing multiple transactions in parallel). We now have to
 * manage many states in parallel, and in the case that they are not purged,
 * we have to merge into an overall aggregation state during the transaction
 * commit.
 *
 * The results are created for each transaction, and are kept seperate from
 * the main results until the end of the transaction, and then at that point
 * they are merged into the main results.
 */
public class AggregationCache
     extends AbstractCache
  implements ICacheable,
             ICacheLoader,
             IEventInterface,
             ISyncPoint
{
  // this is the location of the config file
  private String AggregationConfigFile = null;

  // This is the directory where we will be writing the results
  private String AggregationResultPath;

  private int    BUF_SIZE = 8192;

 /**
  * This stores all the cacheable data. The KeyList is the list of aggregation
  * keys that we know about, the scenario list is the mapping of all the
  * scenarios that have been defined.
  */
  private HashMap<String, AggScenarioList> keyList;
  private HashMap<String, AggScenario>     scenarioList;

  // When we merge output results, this is the order we do them in
  private class MergeString
  {
    ArrayList<AggScenario> MergeOrder;
  }

  // This is used to know the order of the merge that will be done
  private HashMap<String, MergeString> MergeStrings;

  // List of Services that this Client supports
  private final static String SERVICE_PERSIST = "Persist";
  private final static String SERVICE_PURGE = "Purge";
  private final static String SERVICE_OBJECT_COUNT = "ObjectCount";

  // Variables for managing the sync points
  private int syncStatus = 0;

  // The scnario list turns a key into a group of scenarios
  private class AggScenarioList
  {
    ArrayList<String> scenarioMap;
  }

  // An aggregation scenario is the container for the configuration of each
  // aggregation, and holds the grouping, the field to aggregate, the operation
  // the description and the results
  private class AggScenario
  {
    // The description of the aggregation
    String description = "";

    // The indexes of the grouping key fields
    ArrayList<Integer> groupingFieldList;

    // The number of grouping fields we are working on
    int groupingFieldIndex;

    // The index of the input field
    int inpField;

    // The operation to perform 1 = count, 2 = sum
    int operation = 0;

    // If this is true, we know that the result is handled in another scenario
    boolean merged = false;

    // The file name of the results
    String fileName = null;

    // These are the results - we hold the transactions in process and the
    // overall merged transaction result
    HashMap<String, AggResultList> resultCache;
  }

  // The aggregation result class holds the results for each individual
  // aggregation scenario
  private class AggResult
  {
    int    count = 0;
    double sum = 0;
    double max = 0;
    double min = 0;
  }

  // In order to deal with overlayed transactions, we have to be able to
  // manage multiple concurrent results being built
  private class AggResultList
  {
    // These hold the in process transactions
    HashMap<Integer,AggResult> currentTransactionResults;

    // This is the accumulated overall result
    AggResult AccumulatedResult;

    // The list of the grouping fields in the scenario
    ArrayList<String> AggFields;
  }

  // This is used during the write to collect the results
  private class MergedAggregation
  {
    // The name of the object
    String Scenario;

    // Ther filename to write to
    String FileName;

    // The results
    ArrayList<String> ResultList;
  }

 /**
  * The Aggregation cache is used for performing aggregations on input records.
  */
  public AggregationCache()
  {
    keyList      = new HashMap<>(50);
    scenarioList = new HashMap<>(50);
    MergeStrings = new HashMap<>(50);
  }

 /**
  * loadCache is called automatically on startup of the
  * cache factory, as a result of implementing the CacheLoader
  * interface. This should be used to load any data that needs loading, and
  * to set up variables.
  *
  * The aggregation is defined in the following way:
  *
  * KEY;SCENARIONAME;TRIGGERKEY
  * SCENARIO;SCENARIONAME;DESCRIPTION
  * OPERATION;SCENARIONAME;OPDEFINITION
  * GROUPINGFIELDOFFSET;SCENARIONAME;OFFSETVALUE
  * INPFIELDOFFSET;SCENARIONAME;OFFSETVALUE
  * OUTPUTFILE;SCENARIONAME;FILENAME
  * MERGEOUTPUT;SCENARIONAME;MERGEINTOSCENARIONAME;MERGEORDER
  *
  * NOTE: It is usual to define the scenarios first, then the keys, and then
  * finally the Merge configuration. This minimizes errors in setting up.
  *
  * On an input parameter list defined as
  *   performAggregation(String[] FieldList, String[] KeyList);
  *
  * Where:
  *
  * KEY defines the key value that needs to be defined in the input key
  * list for this aggregation to be triggered. This is the filter, and can be
  * linked with a RegexMatch result, thus when the trigger key is fired, all
  * of the SCENARIOS attached to that trigger key will be fired. This allows
  * you to treat the scenarios as components that you can combine how you like./data/Data/Repository/OpenRate/src/OpenRate/cache/AggregationCache.java:812: warning: [unchecked] unchecked conversion
  *
  * SCENARIO defines a logical grouping of configuration items, meaning that
  * the configs are all held together with the common scenario name. This
  * means that a scenario is a group of OPERATIONs on an INPUT field, grouped by
  * the GROUPINGFIELD offsets.
  *
  * OPERATION is the operation to be performed, and can be "COUNT" or "SUM". A
  * count operation can be performed on any field type, a sum operation must be
  * performed on a numeric field type
  *
  * GROUPINGFIELDOFFSET defines the offset of the field to perform the aggregation
  * grouping on in the FieldList. Any number of these fields can be defined
  *
  * INPFIELDOFFSET defines the offset of the field to perform the aggregation
  * operation on in the FieldList. One of these fields can be defined for each
  * scenario
  *
  * MERGEOUTPUT is the command to merge multiple aggregations together, which
  * must have the same aggregation key to be mergeable. The first merge output
  * (the one with the merge key and the merge order "1") will write the results,
  * while the others will be merged into that one. This means that the first
  * merge output will have an output defined, while the others will NOT have
  * an output defined.
  *
  * OUTPUTFILE is the name of the file that the results will be written to when
  * an output is demanded. The output file should be defined before a merge
  * operation in the case that the two are used together.
  *
  * @param resourceName The name of the resource to load for
  * @param cacheName The name of the cache to load for
  */
  @Override
  public void loadCache(String resourceName, String cacheName)
                 throws InitializationException
  {
    BufferedReader    inFile;
    int               Command;
    String[]          DefinitionLine;
    int               FileLine = 0;
    String            tmpFileRecord = null;
    File              dir;

    // Get the source of the data to load
    setSymbolicName(cacheName);

    // Find the location of the configuration file
    getFWLog().info("Starting Aggregation Cache Configuration <" + getSymbolicName() + ">");

    AggregationConfigFile = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(resourceName,
                                                                      cacheName,
                                                                      "AggConfigFileName",
                                                                      "None");

    if (AggregationConfigFile.equals("None"))
    {
      getFWLog().error("Aggregation Config File not found for <" + getSymbolicName() + ">");
      throw new InitializationException("Aggregation Config File not found for <" + getSymbolicName() + ">");
    }

    // Get the location of the results files
    AggregationResultPath = PropertyUtils.getPropertyUtils().getDataCachePropertyValueDef(resourceName,cacheName,
                                                                      "AggResultPath",
                                                                      "None");

    if (AggregationResultPath.equals("None"))
    {
      getFWLog().error("Aggregation Result Path <AggResultPath> not found for <" + getSymbolicName() + ">");
      throw new InitializationException("Aggregation Result Path <AggResultPath> not found for <" + getSymbolicName() + ">");
    }

    // Now try to open the definition file, and work on it
    try
    {
      inFile = new BufferedReader(new FileReader(AggregationConfigFile));
    }
    catch (FileNotFoundException exFileNotFound)
    {
      getFWLog().error(
            "Not able to read the config file : <" +
            AggregationConfigFile + ">");
      throw new InitializationException("Not able to read the config file : <" +
                                        AggregationConfigFile + ">",
                                        exFileNotFound);
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
          DefinitionLine = tmpFileRecord.split(";",3);

          Command = 0;

          if (DefinitionLine[0].equalsIgnoreCase("SCENARIO"))
          {
            Command = 1;
          }

          if (DefinitionLine[0].equalsIgnoreCase("KEY"))
          {
            Command = 2;
          }

          if (DefinitionLine[0].equalsIgnoreCase("OPERATION"))
          {
            Command = 3;
          }

          if (DefinitionLine[0].equalsIgnoreCase("GROUPINGFIELDOFFSET"))
          {
            Command = 4;
          }

          if (DefinitionLine[0].equalsIgnoreCase("INPFIELDOFFSET"))
          {
            Command = 5;
          }

          if (DefinitionLine[0].equalsIgnoreCase("OUTPUTFILE"))
          {
            Command = 6;
          }

          if (DefinitionLine[0].equalsIgnoreCase("MERGEOUTPUT"))
          {
            Command = 7;
          }

          switch (Command)
          {
            //case "SCENARIO":
            case 1:
            {
              // A new block - create the block
              DefinitionLine = tmpFileRecord.split(";",3);
              addAggregationScenario(DefinitionLine[1],DefinitionLine[2]);
            }
            break;

           //case "KEY":
            case 2:
            {
              DefinitionLine = tmpFileRecord.split(";",3);
              addAggregationKey(DefinitionLine[1],DefinitionLine[2]);
            }
            break;

           //case "OPERATION":
            case 3:
            {
              DefinitionLine = tmpFileRecord.split(";",3);
              addAggregationOperation(DefinitionLine[1],DefinitionLine[2]);
            }
            break;

           //case "GROUPINGFIELDOFFSET":
            case 4:
            {
              DefinitionLine = tmpFileRecord.split(";",3);
              AddAggregationGroupingField(DefinitionLine[1],DefinitionLine[2]);
            }
            break;

           //case "INPFIELDOFFSET":
            case 5:
            {
              DefinitionLine = tmpFileRecord.split(";",3);
              AddAggregationAggField(DefinitionLine[1],DefinitionLine[2]);
            }
            break;

           //case "OUTPUTFILE":
            case 6:
            {
              DefinitionLine = tmpFileRecord.split(";",3);
              AddAggregationFile(DefinitionLine[1],DefinitionLine[2]);
            }
            break;

           //case "MERGEOUTPUT":
            case 7:
            {
              DefinitionLine = tmpFileRecord.split(";",4);
              addMerge(DefinitionLine[1],DefinitionLine[2],DefinitionLine[3]);
            }
            break;
          }
        }
      }
    }
    catch (IOException ex)
    {
      getFWLog().fatal(
            "Error reading input file <" + AggregationConfigFile +
            "> in record <" + FileLine + ">. IO Error. Message <" + ex.getMessage() + ">");
    }
    catch (ArrayIndexOutOfBoundsException ex)
    {
      getFWLog().fatal(
            "Error reading input file <" + AggregationConfigFile +
            "> in record <" + FileLine + ">. Malformed Record: <" + tmpFileRecord + ">");
    }
    finally
    {
      try
      {
        inFile.close();
      }
      catch (IOException ex)
      {
        getFWLog().error("Error closing input file <" + AggregationConfigFile +
                  ">", ex);
      }
    }

    // Test the configuration we have found
    dir = new File(AggregationResultPath);
    if ( dir.exists() & dir.canWrite())
    {
      getFWLog().info("Aggregation Result Path <" + AggregationResultPath + "> set for <" + getSymbolicName() + ">");
    }
    else
    {
      getFWLog().error("Aggregation Result Path <" + AggregationResultPath + "> either not defined or read only for <" + getSymbolicName() + ">");
      throw new InitializationException("Aggregation Result Path <" + AggregationResultPath + "> either not defined or read only for <" + getSymbolicName() + ">");
    }

    // Done
    getFWLog().info("Completed Aggregation Cache Configuration <" + getSymbolicName() + ">");
  }

  // -----------------------------------------------------------------------------
  // ------------------- Start of cache creation functions -----------------------
  // -----------------------------------------------------------------------------

 /**
  * Create a new scenario and initialise the values.
  *
  * @param scenarioName The name of this scenario
  * @param description The description of this scenario
  * @throws IntializationException
  */
  private void addAggregationScenario(String scenarioName, String description)
    throws InitializationException
  {
    AggScenario tmpAggScenario;

    if (scenarioList.containsKey(scenarioName))
    {
      getFWLog().error("Aggregation scenario <" + scenarioName + "> already defined");
      throw new InitializationException("Aggregation scenario <" + scenarioName + "> already defined");
    }

    tmpAggScenario = new AggScenario();
    tmpAggScenario.resultCache = new HashMap<>(1000);
    tmpAggScenario.groupingFieldList = new ArrayList<>();
    tmpAggScenario.groupingFieldIndex = 0;
    tmpAggScenario.description = description;

    // Add the scenario
    scenarioList.put(scenarioName,tmpAggScenario);
  }

 /**
  * Add a new key field to the scenario
  *
  * @param scenarioName The name of the scenario that we are adding the key to
  * @param keyValue The name of the key that should trigger the scenario
  * @throws IntializationException
  */
  private void addAggregationKey(String scenarioName, String keyValue)
    throws InitializationException
  {
    AggScenarioList tmpAggScenarioList;

    if (!keyList.containsKey(keyValue))
    {
      // create the new key value
      tmpAggScenarioList = new AggScenarioList();
      tmpAggScenarioList.scenarioMap = new ArrayList<>();
      keyList.put(keyValue,tmpAggScenarioList);
    }
    else
    {
      // just get the one that is already there
      tmpAggScenarioList = keyList.get(keyValue);
    }

    // Now try to get the scenario to add to the key
    if (!scenarioList.containsKey(scenarioName))
    {
      getFWLog().error("Aggregation scenario <" + scenarioName + "> not defined");
      throw new InitializationException("Aggregation scenario <" + scenarioName + "> not defined");
    }
    else
    {
      tmpAggScenarioList.scenarioMap.add(scenarioName);
    }
  }

 /**
  * Define the aggregation operation that we are going to perform
  *
  * @param scenarioName The name of the scenario that we are adding the operation to
  * @param operationValue The operation that this scenario should perform
  * @throws IntializationException
  */
  private void addAggregationOperation(String scenarioName, String operationValue)
    throws InitializationException
  {
    AggScenario tmpAggScenario;
    boolean     operationUnderstood = false;

    if (!scenarioList.containsKey(scenarioName))
    {
      getFWLog().error("Aggregation scenario <" + scenarioName + "> not defined");
      throw new InitializationException("Aggregation scenario <" + scenarioName + "> not defined");
    }
    else
    {
      tmpAggScenario = scenarioList.get(scenarioName);

      if (operationValue.equalsIgnoreCase("count"))
      {
        operationUnderstood = true;
        tmpAggScenario.operation = 1;
      }

      if (operationValue.equalsIgnoreCase("sum"))
      {
        operationUnderstood = true;
        tmpAggScenario.operation = 2;
      }

      if (operationValue.equalsIgnoreCase("max"))
      {
        operationUnderstood = true;
        tmpAggScenario.operation = 3;
      }

      if (operationValue.equalsIgnoreCase("min"))
      {
        operationUnderstood = true;
        tmpAggScenario.operation = 4;
      }

      if (!operationUnderstood)
      {
        getFWLog().error("Aggregation operation <" + operationValue + "> not understood in scenario <" + scenarioName + ">");
        throw new InitializationException("Aggregation operation <" + operationValue + "> not understood in scenario <" + scenarioName + ">");
      }
    }
  }

 /**
  * Add a grouping field to the aggregation
  *
  * @param ScenarioName The name of the scenario that we are adding the grouping to
  * @param AggregationOffset The offset of the field that we should group on
  * @throws IntializationException
  */
  private void AddAggregationGroupingField(String ScenarioName, String AggregationOffset)
    throws InitializationException
  {
    AggScenario tmpAggScenario;
    int OffsetValue = -1;

    if (!scenarioList.containsKey(ScenarioName))
    {
      getFWLog().error("Aggregation scenario <" + ScenarioName + "> not defined");
      throw new InitializationException("Aggregation scenario <" + ScenarioName + "> not defined");
    }
    else
    {
      tmpAggScenario = scenarioList.get(ScenarioName);

      try
      {
        OffsetValue = Integer.parseInt(AggregationOffset);
      }
      catch (NumberFormatException nfe)
      {
        getFWLog().error("Aggregation field offset <" + AggregationOffset + "> not numeric in scenario <" + ScenarioName + ">");
        throw new InitializationException("Aggregation field offset <" + AggregationOffset + "> not numeric in scenario <" + ScenarioName + ">");
      }

      tmpAggScenario.groupingFieldList.add(OffsetValue);
      tmpAggScenario.groupingFieldIndex++;
    }
  }

 /**
  * Add the field we are aggretaing on
  *
  * @param ScenarioName The name of the scenario that we are adding the field to
  * @param AggregationOffset The offset of the field that we should aggregate on
  * @throws IntializationException
  */
  private void AddAggregationAggField(String ScenarioName, String AggregationOffset)
    throws InitializationException
  {
    AggScenario tmpAggScenario;
    int OffsetValue = -1;

    if (!scenarioList.containsKey(ScenarioName))
    {
      getFWLog().error("Aggregation scenario <" + ScenarioName + "> not defined");
      throw new InitializationException("Aggregation scenario <" + ScenarioName + "> not defined");
    }
    else
    {
      tmpAggScenario = scenarioList.get(ScenarioName);

      try
      {
        OffsetValue = Integer.parseInt(AggregationOffset);
      }
      catch (NumberFormatException nfe)
      {
        getFWLog().error("Aggregation field offset <" + AggregationOffset + "> not numeric in scenario <" + ScenarioName + ">");
        throw new InitializationException("Aggregation field offset <" + AggregationOffset + "> not numeric in scenario <" + ScenarioName + ">");
      }

      tmpAggScenario.inpField = OffsetValue;
    }
  }

 /**
  * Define the file name for the scenario
  *
  * @param ScenarioName The name of the scenario that we are adding the file to
  * @param FileName The file that we are writing to
  * @throws IntializationException
  */
  private void AddAggregationFile(String ScenarioName, String FileName)
    throws InitializationException
  {
    AggScenario tmpAggScenario;

    if (!scenarioList.containsKey(ScenarioName))
    {
      getFWLog().error("Aggregation scenario <" + ScenarioName + "> not defined");
      throw new InitializationException("Aggregation scenario <" + ScenarioName + "> not defined");
    }
    else
    {
      tmpAggScenario = scenarioList.get(ScenarioName);
      tmpAggScenario.fileName = FileName;
    }
  }

 /**
  * Define the merge configuration
  *
  * @param scenarioName The name of the scenario that we are adding the merge to
  * @param FileName The file that we are writing to
  * @throws IntializationException
  */
  private void addMerge(String scenarioName, String mergeIntoScenarioName, String mergeOrder)
    throws InitializationException
  {
    AggScenario tmpAggScenario;
    AggScenario tmpMergeIntoScenario;
    int tmpMergeOrder;
    MergeString tmpMergeString;

    if (!scenarioList.containsKey(scenarioName))
    {
      getFWLog().error("Aggregation scenario <" + scenarioName + "> not defined");
      throw new InitializationException("Aggregation scenario <" + scenarioName + "> not defined");
    }
    else
    {
      if (!scenarioList.containsKey(mergeIntoScenarioName))
      {
        getFWLog().error("Aggregation scenario <" + mergeIntoScenarioName + "> not defined");
        throw new InitializationException("Aggregation scenario <" + mergeIntoScenarioName + "> not defined");
      }
      else
      {
        tmpAggScenario = scenarioList.get(scenarioName);
        tmpMergeIntoScenario = scenarioList.get(mergeIntoScenarioName);

        // get the merge order
        try
        {
          tmpMergeOrder = Integer.parseInt(mergeOrder);
        }
        catch (NumberFormatException nfe)
        {
          getFWLog().error("Merge order <" + mergeOrder + "> not numeric in scenario <" + scenarioName + ">");
          throw new InitializationException("Merge order <" + mergeOrder + "> not numeric in scenario <" + scenarioName + ">");
        }

        // If the merge order is 1 (the main merge target), we must have an output
        // defined, otherwise we must not

        if (tmpMergeOrder == 1)
        {
          // See if we already have the file defined
          if (tmpAggScenario.fileName == null)
          {
            getFWLog().error("Aggregation scenario <" + scenarioName + "> does not have an output defined. Define the output destination before defining a merge.");
            throw new InitializationException("Aggregation scenario <" + scenarioName + "> not defined");
          }
          else
          {
            // Store the information in the mergeorder object
            tmpMergeString = new MergeString();
            tmpMergeString.MergeOrder = new ArrayList<>();
            tmpMergeString.MergeOrder.add(tmpAggScenario);
            MergeStrings.put(scenarioName, tmpMergeString);
          }
        }
        else
        {
          // make sure that the file name is not defined
          if (tmpAggScenario.fileName != null)
          {
            getFWLog().error("Aggregation scenario <" + scenarioName + "> has an output defined, but is a merge subordinate. You cannot define an output for this scenario.");
            throw new InitializationException("Aggregation scenario <" + scenarioName + "> has an output defined, but is a merge subordinate. You cannot define an output for this scenario.");
          }
          else
          {
            // Check that the grouping keys are compatible
            if (tmpMergeIntoScenario.groupingFieldIndex != tmpAggScenario.groupingFieldIndex)
            {
              getFWLog().error("Aggregation scenario <" + scenarioName + "> does not have the same key structure as merge scenario <" + mergeIntoScenarioName + ">. They cannot be merged.");
              throw new InitializationException("Aggregation scenario <" + scenarioName + "> does not have the same key structure as merge scenario <" + mergeIntoScenarioName + ">. They cannot be merged.");
            }

            // Now check that they are identical
            for (int Index = 0 ; Index < tmpMergeIntoScenario.groupingFieldIndex ; Index++)
            {
              if (tmpMergeIntoScenario.groupingFieldList.get(Index) != tmpAggScenario.groupingFieldList.get(Index))
              {
                getFWLog().error("Aggregation scenarios <" + scenarioName + "> and <" + mergeIntoScenarioName + "> do not have identical grouping keys. They cannot be merged.");
                throw new InitializationException("Aggregation scenarios <" + scenarioName + "> and <" + mergeIntoScenarioName + "> do not have identical grouping keys. They cannot be merged.");
              }
            }

            // mark that we have delegated responsibility for writing elsewhere
            tmpAggScenario.merged = true;

            // Get the scenario we are merging into
            tmpMergeString = MergeStrings.get(mergeIntoScenarioName);

            // Add the new scenario to merge
            tmpMergeString.MergeOrder.add(tmpAggScenario);
          }
        }
      }
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of user interface level functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * This function performs the aggregation according to the configuration
  *
  * @param fieldList The list of fields to work on
  * @param keysToAggregate The keys to aggregate
  * @param transactionNumber The number of the transaction to aggregate for
  * @throws ProcessingException
  */
  public void aggregate(String[] fieldList, ArrayList<String> keysToAggregate, int transactionNumber)
    throws ProcessingException
  {
    AggScenarioList tmpAggScenarioList;
    AggScenario     tmpAggScenario;
    AggResult       tmpAggResult;
    AggResultList   tmpAggResultList;
    int         i;
    int         j;
    String      tmpKey;
    int         k;
    String      tmpScenarioKey;
    double      CurrentValue = 0;

    // Find the aggregations to do for the key list
    for ( i = 0 ; i < keysToAggregate.size() ; i++)
    {
      if (keyList.containsKey(keysToAggregate.get(i)))
      {
        // Get the scenario list
        tmpAggScenarioList = keyList.get(keysToAggregate.get(i));

        for ( k = 0 ; k < tmpAggScenarioList.scenarioMap.size() ; k++ )
        {
          tmpScenarioKey = tmpAggScenarioList.scenarioMap.get(k);

          tmpAggScenario = scenarioList.get(tmpScenarioKey);

          // Build the key
          tmpKey = "";
          for ( j = 0 ; j < tmpAggScenario.groupingFieldIndex ; j++)
          {
            tmpKey += fieldList[tmpAggScenario.groupingFieldList.get(j)-1];
          }

          // Retrieve the object, or create it
          if (tmpAggScenario.resultCache.containsKey(tmpKey))
          {
            // get the already created scenario object
            tmpAggResultList = tmpAggScenario.resultCache.get(tmpKey);

            // get the information for the transaction
            tmpAggResult = tmpAggResultList.currentTransactionResults.get(transactionNumber);

            // if does not exist, create it
            if (tmpAggResult == null)
            {
              tmpAggResult = new AggResult();
              tmpAggResultList.currentTransactionResults.put(transactionNumber, tmpAggResult);
            }
          }
          else
          {
            // Create the results object
            tmpAggResultList = new AggResultList();
            tmpAggResultList.currentTransactionResults = new HashMap<>();
            tmpAggResultList.AccumulatedResult = new AggResult();
            tmpAggScenario.resultCache.put(tmpKey,tmpAggResultList);

            // Add the aggregation result for the transaction
            tmpAggResult = new AggResult();
            tmpAggResultList.currentTransactionResults.put(transactionNumber, tmpAggResult);

            // Set up the agg field structure
            tmpAggResultList.AggFields = new ArrayList<>();

            // Add the information that we will need for outputting
            for ( j = 0 ; j < tmpAggScenario.groupingFieldIndex ; j++)
            {
              tmpAggResultList.AggFields.add(fieldList[tmpAggScenario.groupingFieldList.get(j)-1]);
            }
          }

          // Now perform the aggregation - we always count
          try
          {
            tmpAggResult.count++;
          }
          catch (NullPointerException npe)
          {
            String ErrorString = "Error accessing the aggregation result cache for scenario <" + tmpAggScenario + "> and transaction <" + transactionNumber + ">";
            getFWLog().error(ErrorString);
            throw new ProcessingException (ErrorString);
          }

          if (tmpAggScenario.operation > 1)
          {
            // Parse the input value and handle any errors
            try
            {
              CurrentValue = Double.parseDouble(fieldList[tmpAggScenario.inpField-1]);
            }
            catch (NumberFormatException nfe)
            {
              // log the error
              getFWLog().error("Error converting non numeric value <" +
                fieldList[tmpAggScenario.inpField-1] + "> in scenario <" +
                keysToAggregate.get(i) + " in module <" + getSymbolicName() +">");
            }

            if (tmpAggScenario.operation == 2)
            {
              //perform the summing
              tmpAggResult.sum += CurrentValue;
            }
            else if (tmpAggScenario.operation == 3)
            {
              //perform the max
              if (CurrentValue > tmpAggResult.max)
              {
                tmpAggResult.max = CurrentValue;
              }
            }
            else if (tmpAggScenario.operation == 4)
            {
              //perform the min
              if (CurrentValue < tmpAggResult.min)
              {
                tmpAggResult.min = CurrentValue;
              }
            }
          }
        }
      }
      else
      {
        String ErrorString = "Aggregation cache does not contain key <" + keysToAggregate.get(i) +">";
        getFWLog().error(ErrorString);
        throw new ProcessingException (ErrorString);
      }
    }
  }

 /**
  * This returns a collection of all of the results that have been calculated
  * and clears the cache
  *
  * @return A collection of the aggregation results
  */
  public ArrayList<String> getResults()
  {
    Set<String>         ScenarioKeySet;
    Iterator<String>    ScenarioKeySetIterator;
    Set         ResultKeySet;
    Iterator ResKeySetIterator;
    AggScenario tmpAggScenario;
    AggResultList   tmpAggResultList;
    AggResult   tmpAggResult;
    String      tmpLine;
    int         i;
    String      tmpScenario;
    String ResultIterator;

    ArrayList<String> Results = new ArrayList<>();

    // get all of the scenarios
    ScenarioKeySet = scenarioList.keySet();
    ScenarioKeySetIterator = ScenarioKeySet.iterator();

    // for each of the scenarios
    while (ScenarioKeySetIterator.hasNext())
    {
      tmpScenario = ScenarioKeySetIterator.next();
      tmpAggScenario = scenarioList.get(tmpScenario);

      // dump all of the information
      ResultKeySet = tmpAggScenario.resultCache.keySet();
      ResKeySetIterator = ResultKeySet.iterator();

      while (ResKeySetIterator.hasNext())
      {
        ResultIterator = (String) ResKeySetIterator.next();
        tmpAggResultList = tmpAggScenario.resultCache.get(ResultIterator);
        tmpAggResult = tmpAggResultList.AccumulatedResult;

        tmpLine = tmpScenario + ";";
        for (i = 0 ; i < tmpAggResultList.AggFields.size() ; i++)
        {
          tmpLine = tmpLine + tmpAggResultList.AggFields.get(i) + ";";
        }

        // Output the results
        switch (tmpAggScenario.operation)
        {
          // count
          case 1:
          {
            tmpLine = tmpLine + tmpAggResult.count + ";";
          }
          break;

          // sum
          case 2:
          {
            tmpLine = tmpLine + tmpAggResult.sum + ";";
          }
          break;

          // max
          case 3:
          {
            tmpLine = tmpLine + tmpAggResult.max + ";";
          }
          break;

          // min
          case 4:
          {
            tmpLine = tmpLine + tmpAggResult.min + ";";
          }
          break;
        }

        Results.add(tmpLine);
      }
    }

    // Now that we have written the results, we clear them
    purgeResults();

    // Return what we have created
    return Results;
  }

 /**
  * This writes the results to disk on demand, writing all of the results into
  * the files that have been defined in the scenarios. This works on the
  * aggregated object cache, not the transaction object cache
  *
  * @param baseName - the base name of the transaction for which we are writing
  */
  public void writeResults(String baseName)
  {
    Set<String>         ScenarioKeySet;
    Iterator<String>    ScenarioKeySetIterator;
    Set         resultKeySet;
    Iterator resKeySetIterator;
    AggScenario tmpAggScenario;
    AggScenario tmpMergedScenario;
    AggResult   tmpAggResult;
    AggResult   tmpMergedResult;
    String      tmpLine;
    int         i;
    String      tmpScenario;
    File        tmpFile;
    BufferedWriter writer;
    String      resultIterator;
    AggResultList tmpAggResultList;
    AggResultList tmpMergedResultList;
    ArrayList<MergedAggregation> ResultCache;
    MergedAggregation tmpMergedAggregation;
    MergeString tmpMergeString;
    Iterator<MergedAggregation> ResultsIterator;
    int Index;

    // Create the output cache
    ResultCache = new ArrayList<>();

    // get all of the scenarios
    ScenarioKeySet = scenarioList.keySet();
    ScenarioKeySetIterator = ScenarioKeySet.iterator();

    // for each of the scenarios
    while (ScenarioKeySetIterator.hasNext())
    {
      tmpScenario = ScenarioKeySetIterator.next();
      tmpAggScenario = scenarioList.get(tmpScenario);

      // Merging works like this:
      // If this scenario has not been delegated to another scenario, we write the
      // results here. Otherwise, we get the merge string and follow it.
      if (MergeStrings.containsKey(tmpScenario))
      {
        // there is a string defined for this scenario - we should follow it
        tmpMergedAggregation = new MergedAggregation();
        tmpMergedAggregation.Scenario = tmpScenario;
        tmpMergedAggregation.FileName = AggregationResultPath +
                                        System.getProperty("file.separator") +
                                        baseName + tmpAggScenario.fileName;
        tmpMergedAggregation.ResultList = new ArrayList<>();
        ResultCache.add(tmpMergedAggregation);

        // We use the results keys from the main scenario to merge
        resultKeySet = tmpAggScenario.resultCache.keySet();
        resKeySetIterator = resultKeySet.iterator();

        while (resKeySetIterator.hasNext())
        {
          resultIterator = (String) resKeySetIterator.next();
          tmpAggResultList = tmpAggScenario.resultCache.get(resultIterator);
          tmpAggResult = tmpAggResultList.AccumulatedResult;

          tmpLine = tmpScenario + ";";
          for (i = 0 ; i < tmpAggResultList.AggFields.size() ; i++)
          {
            tmpLine = tmpLine + tmpAggResultList.AggFields.get(i) + ";";
          }

          // Output the results
          switch (tmpAggScenario.operation)
          {
            // count
            case 1:
            {
              tmpLine = tmpLine + tmpAggResult.count + ";";
            }
            break;

            // sum
            case 2:
            {
              tmpLine = tmpLine + tmpAggResult.sum + ";";
            }
            break;

            // max
            case 3:
            {
              tmpLine = tmpLine + tmpAggResult.max + ";";
            }
            break;

            // min
            case 4:
            {
              tmpLine = tmpLine + tmpAggResult.min + ";";
            }
            break;
          }

          // Now get the rest of the results from the merge string
          tmpMergeString = MergeStrings.get(tmpScenario);
          for (Index = 1 ; Index < tmpMergeString.MergeOrder.size() ; Index++)
          {
            // Get the referenced scenario
            tmpMergedScenario = tmpMergeString.MergeOrder.get(Index);
            tmpMergedResultList = tmpMergedScenario.resultCache.get(resultIterator);
            tmpMergedResult = tmpMergedResultList.AccumulatedResult;

            // Output the results
            switch (tmpMergedScenario.operation)
            {
              // count
              case 1:
              {
                tmpLine = tmpLine + tmpMergedResult.count + ";";
              }
              break;

              // sum
              case 2:
              {
                tmpLine = tmpLine + tmpMergedResult.sum + ";";
              }
              break;

              // max
              case 3:
              {
                tmpLine = tmpLine + tmpMergedResult.max + ";";
              }
              break;

              // min
              case 4:
              {
                tmpLine = tmpLine + tmpMergedResult.min + ";";
              }
              break;
            }
          }

          // Add the result to the list
          tmpMergedAggregation.ResultList.add(tmpLine);
        }
      }
      else
      {
        // There is no merge string defined for this scenario so we use the
        // results keys from the main scenario to merge, but only if it has not
        // been delegated
        if (tmpAggScenario.merged == false)
        {
          tmpMergedAggregation = new MergedAggregation();
          tmpMergedAggregation.Scenario = tmpScenario;
          tmpMergedAggregation.FileName = AggregationResultPath +
                                          System.getProperty("file.separator") +
                                          baseName + tmpAggScenario.fileName;
          tmpMergedAggregation.ResultList = new ArrayList<>();
          ResultCache.add(tmpMergedAggregation);

          resultKeySet = tmpAggScenario.resultCache.keySet();
          resKeySetIterator = resultKeySet.iterator();

          while (resKeySetIterator.hasNext())
          {
            resultIterator = (String) resKeySetIterator.next();
            tmpAggResultList = tmpAggScenario.resultCache.get(resultIterator);
            tmpAggResult = tmpAggResultList.AccumulatedResult;

            tmpLine = tmpScenario + ";";
            for (i = 0 ; i < tmpAggResultList.AggFields.size() ; i++)
            {
              tmpLine = tmpLine + tmpAggResultList.AggFields.get(i) + ";";
            }

            // Output the results
            switch (tmpAggScenario.operation)
            {
              // count
              case 1:
              {
                tmpLine = tmpLine + tmpAggResult.count + ";";
              }
              break;

              // sum
              case 2:
              {
                tmpLine = tmpLine + tmpAggResult.sum + ";";
              }
              break;

              // max
              case 3:
              {
                tmpLine = tmpLine + tmpAggResult.max + ";";
              }
              break;

              // min
              case 4:
              {
                tmpLine = tmpLine + tmpAggResult.min + ";";
              }
              break;
            }

            // Add the result to the list
            tmpMergedAggregation.ResultList.add(tmpLine);
          }
        }
      }
    }

    // Now write the results to file
    ResultsIterator = ResultCache.iterator();

    while (ResultsIterator.hasNext())
    {
      tmpMergedAggregation = ResultsIterator.next();

      try
      {
        // Open the file for *appending*
        tmpFile = new File(tmpMergedAggregation.FileName);
        writer = new BufferedWriter(new FileWriter(tmpFile, true), BUF_SIZE);

        for (Index = 0 ; Index < tmpMergedAggregation.ResultList.size() ; Index++)
        {
          tmpLine = tmpMergedAggregation.ResultList.get(Index);
          writer.write(tmpLine);
          writer.newLine();
        }

        writer.close();
      }
      catch (IOException IOex)
      {
        getFWLog().error("Error writing aggregation file for scenario <" + tmpMergedAggregation.Scenario + ">. Message <" + IOex.getMessage() + ">");
      }
    }

    // Now that we have written the results, we clear them
    purgeResults();
  }

 /**
  * This purges the results from memory. This works on the aggregated result
  * cache, not the transaction object cache
  */
  public void purgeResults()
  {
    AggScenario tmpAggScenario;
    String      tmpScenario;
    Set<String>         ScenarioKeySet;
    Iterator<String>    ScenarioKeySetIterator;

    // get all of the scenarios
    ScenarioKeySet = scenarioList.keySet();
    ScenarioKeySetIterator = ScenarioKeySet.iterator();

    // for each of the scenarios
    while (ScenarioKeySetIterator.hasNext())
    {
      tmpScenario = ScenarioKeySetIterator.next();
      tmpAggScenario = scenarioList.get(tmpScenario);

      tmpAggScenario.resultCache.clear();
    }
  }

 /**
  * This purges the results from memory
  *
  * @return The number of results cached at present
  */
  public int countResults()
  {
    AggScenario tmpAggScenario;
    String      tmpScenario;
    Set<String>         ScenarioKeySet;
    Iterator<String>    ScenarioKeySetIterator;
    int         ResultObjectCount = 0;

    // get all of the scenarios
    ScenarioKeySet = scenarioList.keySet();
    ScenarioKeySetIterator = ScenarioKeySet.iterator();

    // for each of the scenarios
    while (ScenarioKeySetIterator.hasNext())
    {
      tmpScenario = ScenarioKeySetIterator.next();
      tmpAggScenario = scenarioList.get(tmpScenario);

      ResultObjectCount += tmpAggScenario.resultCache.size();
    }

    return ResultObjectCount;
  }

  // -----------------------------------------------------------------------------
  // ------------------ Start of transaction layer functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * This function commits the information from the transaction object into the
  * main cache, and clears down the transaction object
  *
  * @param transactionNumber
  */
  public void commitTransaction(int transactionNumber)
  {
    Set<String>         ScenarioKeySet;
    Iterator<String>    ScenarioKeySetIterator;
    Set         ResultKeySet;
    Iterator ResKeySetIterator;
    AggScenario tmpAggScenario;
    AggResult   tmpAggResult;
    AggResult   MergedAggResult;
    String      tmpScenario;
    String      ResultIterator;
    AggResultList tmpAggResultList;

    // get all of the scenarios
    ScenarioKeySet = scenarioList.keySet();
    ScenarioKeySetIterator = ScenarioKeySet.iterator();

    // for each of the scenarios
    while (ScenarioKeySetIterator.hasNext())
    {
      tmpScenario = ScenarioKeySetIterator.next();
      tmpAggScenario = scenarioList.get(tmpScenario);

      // dump all of the information
      ResultKeySet = tmpAggScenario.resultCache.keySet();
      ResKeySetIterator = ResultKeySet.iterator();

      while (ResKeySetIterator.hasNext())
      {
        ResultIterator = (String) ResKeySetIterator.next();
        tmpAggResultList = tmpAggScenario.resultCache.get(ResultIterator);

        // See if we have information for this transaction
        if (tmpAggResultList.currentTransactionResults.containsKey(transactionNumber))
        {
          // yes, so merge the information
          tmpAggResult = tmpAggResultList.currentTransactionResults.get(transactionNumber);
          MergedAggResult = tmpAggResultList.AccumulatedResult;

          // do the merge of the current results into the accumulated object
          MergedAggResult.count += tmpAggResult.count;
          MergedAggResult.sum += tmpAggResult.sum;

          if (tmpAggResult.max > MergedAggResult.max)
          {
            MergedAggResult.max = tmpAggResult.max;
          }

          if (tmpAggResult.min < MergedAggResult.min)
          {
            MergedAggResult.min = tmpAggResult.min;
          }
        }

        // remove the transaction information
        tmpAggResultList.currentTransactionResults.remove(transactionNumber);
      }
    }
  }

 /**
  * This function removed the information from the transaction object into the
  * main cache, and clears down the transaction object
  *
  * @param transactionNumber
  */
  public void rollbackTransaction(int transactionNumber)
  {
    Set<String>         scenarioKeySet;
    Iterator<String>    scenarioKeySetIterator;
    Set         resultKeySet;
    Iterator    resKeySetIterator;
    AggScenario tmpAggScenario;
    String      tmpScenario;
    String      resultIterator;
    AggResultList tmpAggResultList;

    // get all of the scenarios
    scenarioKeySet = scenarioList.keySet();
    scenarioKeySetIterator = scenarioKeySet.iterator();

    // for each of the scenarios
    while (scenarioKeySetIterator.hasNext())
    {
      tmpScenario = scenarioKeySetIterator.next();
      tmpAggScenario = scenarioList.get(tmpScenario);

      // dump all of the information
      resultKeySet = tmpAggScenario.resultCache.keySet();
      resKeySetIterator = resultKeySet.iterator();

      while (resKeySetIterator.hasNext())
      {
        resultIterator = (String) resKeySetIterator.next();
        tmpAggResultList = tmpAggScenario.resultCache.get(resultIterator);

        // See if we have information for this transaction
        if (tmpAggResultList.currentTransactionResults.containsKey(transactionNumber))
        {
          // remove the transaction information
          tmpAggResultList.currentTransactionResults.remove(transactionNumber);
        }
      }
    }
  }

  // -----------------------------------------------------------------------------
  // ---------------- Start of inherited ISyncPoint functions --------------------
  // -----------------------------------------------------------------------------

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  */
  @Override
  public int getSyncStatus()
  {
    return syncStatus;
  }

 /**
  * This is used for the pipeline synchronisation. See the description in the
  * OpenRate framework module to understand how this works.
  *
  * *** This is a stub function for the moment ***
  */
  @Override
  public void setSyncStatus(int newStatus)
  {
    if (newStatus == 2)
    {
      syncStatus = 3;
    }
    else if (newStatus == 4)
    {
      syncStatus = 5;
    }
    else
    {
      syncStatus = newStatus;
    }
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

  /**
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
   *
   */
  @Override
  public void registerClientManager() throws InitializationException
  {
    //Register this Client
    ClientManager.registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PERSIST, ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_PURGE, ClientManager.PARAM_DYNAMIC);
    ClientManager.registerClientService(getSymbolicName(), SERVICE_OBJECT_COUNT, ClientManager.PARAM_DYNAMIC);
  }

 /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world.
  *
  * @param Command - command that is understand by the client module
  * @param Init - we are performing initial configuration if true
  * @param Parameter - parameter for the command
  * @return The result of the event processing
  */
  @Override
  public String processControlEvent(String Command, boolean Init,
                                    String Parameter)
  {
    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_PURGE))
    {
      if (Parameter.equalsIgnoreCase("true"))
      {
        // Clear the persistence object
        purgeResults();

        ResultCode = 0;
      }
    }

    // Return the number of objects in the cache
    if (Command.equalsIgnoreCase(SERVICE_OBJECT_COUNT))
    {
      return Integer.toString(scenarioList.size()) + ":" +
             Integer.toString(keyList.size()) + ":" +
             Integer.toString(countResults());
    }

    if (Command.equalsIgnoreCase(SERVICE_PERSIST))
    {
      // get the name of the file to persist to
      if (Parameter.length() > 0)
      {
        writeResults(Parameter);
        ResultCode = 0;
      }
      else
      {
        return "No file name defined to purge to";
      }

      //try
      //{
      //  if (DataSourceType.equalsIgnoreCase("DB"))
      //  {
      // Reload
      //PersistToFile();
      //  }
      //}
      //catch (InitializationException ex)
      //{
      //  log.error("Command SERVICE_PERSIST not executed because of InitializationException thrown by loadData()", ex);
      //  return "Command not executed because of InitializationException thrown by loadData()";
      //}
      //ResultCode = 0;
    }

    if (ResultCode == 0)
    {
      getFWLog().debug(LogUtil.LogECICacheCommand(getSymbolicName(), Command, Parameter));

      return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }
}
