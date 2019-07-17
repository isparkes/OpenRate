

package OpenRate.parser;

import java.util.HashMap;

/**
 *
 * @author ian
 */
public class HuaweiDef implements IASN1Def
{
  // These control the decoding and naming of the fields
  private HashMap<String,String> tagNames;
  private HashMap<String,Integer> tagTypes;
  private HashMap<String,String> cdrNames;
  
  @Override
  public void initASN1() {
  }

 /**
  * Initialise the tags for the decoding. The key into the hash maps is 
  * CDRtype hex;Tag ID hex[;Block ID Hex][;Block ID Hex][;Block ID Hex]
  * 
  * The exact name of a tag depends on the location in the tree as well as the 
  * tag id.
  */
  @Override
  public void initTags() {
    // Set up the hash maps
    tagNames = new HashMap<>();
    tagTypes = new HashMap<>();
    cdrNames = new HashMap<>();
    
    // MOC
    cdrNames.put("00", "MOC");
    tagNames.put("00;81", "IMSI");
    tagTypes.put("00;81", ASN1Parser.BCDString);
    tagNames.put("00;82", "IMEI");
    tagTypes.put("00;82", ASN1Parser.BCDString);
    tagNames.put("00;83", "MSISDN");
    tagTypes.put("00;83", ASN1Parser.BCDString);
    tagNames.put("00;84", "CallingNumber");
    tagTypes.put("00;84", ASN1Parser.BCDString);
    tagNames.put("00;85", "CalledNumber");
    tagTypes.put("00;85", ASN1Parser.BCDString);
    tagNames.put("00;97", "AnswerTimestamp");
    tagTypes.put("00;97", ASN1Parser.BCDString);
    tagNames.put("00;98", "ReleaseTimestamp");
    tagTypes.put("00;98", ASN1Parser.BCDString);
    tagNames.put("00;99", "Duration");
    tagTypes.put("00;99", ASN1Parser.INTEGER);
    
    // MOC
    cdrNames.put("01", "MTC");
    tagNames.put("01;81", "IMSI");
    tagTypes.put("01;81", ASN1Parser.BCDString);
    tagNames.put("01;82", "IMEI");
    tagTypes.put("01;82", ASN1Parser.BCDString);
    tagNames.put("01;83", "MSISDN");
    tagTypes.put("01;83", ASN1Parser.BCDString);
    tagNames.put("01;84", "CallingNumber");
    tagTypes.put("01;84", ASN1Parser.BCDString);
    tagNames.put("01;94", "AnswerTimestamp");
    tagTypes.put("01;94", ASN1Parser.BCDString);
    tagNames.put("01;95", "ReleaseTimestamp");
    tagTypes.put("01;95", ASN1Parser.BCDString);
    tagNames.put("01;96", "Duration");
    tagTypes.put("01;96", ASN1Parser.INTEGER);
    
    // SMMT
    cdrNames.put("07", "SMMT");
    tagNames.put("07;82", "IMSI");
    tagTypes.put("07;82", ASN1Parser.BCDString);
    tagNames.put("07;83", "IMEI");
    tagTypes.put("07;83", ASN1Parser.BCDString);
    tagNames.put("07;84", "MSISDN");
    tagTypes.put("07;84", ASN1Parser.BCDString);
    tagNames.put("07;88", "Timestamp");
    tagTypes.put("07;88", ASN1Parser.BCDString);
    
    // SUPS
    cdrNames.put("0a", "SUPS");
  }

  @Override
  public String getTagName(String tagPath)
  {
    if (tagNames.containsKey(tagPath))
    {
      return tagNames.get(tagPath);
    }
    else
    {
      return "";
    }
  }
  
  public String getCDRName(String tagPath)
  {
    if (cdrNames.containsKey(tagPath))
    {
      return cdrNames.get(tagPath);
    }
    else
    {
      return "Unknown";
    }
  }
  
  @Override
  public int getTagType(String tagPath)
  {
    if (tagTypes.containsKey(tagPath))
    {
      return tagTypes.get(tagPath);
    }
    else
    {
      return -1;
    }
  }
}
