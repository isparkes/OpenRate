/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package OpenRate.record.flexRecord;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class defines the structures and mappings that allow us to construct
 * record definitions on the fly
 *
 * @author TGDSPIA1
 */
public class RecordBlockDef
{
  int NumberOfFields = 0;
  String[] FieldNames;
  int[]    FieldTypes;

  // the field separator for this block
  String   Separator = null;

  // This is the name of the block
  String   BlockName;

  // The mapping information that we will do when creating a new block of
  // this type. This is an ArrayList so that we hold all of the fields that are
  // to be mapped in a list.
  ArrayList<MapElement>   Mapping;

  // The ChildTemplates hashmap contains the definitions of the child blocks
  HashMap<String, RecordBlockDef> ChildTemplates;

  // This is the index to allow us to find the field names quickly. This takes
  // the full path name and returns the block reference and the field info
  // (Offset, type)
  HashMap<String, Integer> FieldNameIndex;
}

