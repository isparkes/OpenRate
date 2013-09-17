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
  
  private String[] tagNameSimple = new String[10];
  
  @Override
  public void initASN1() {
  }

 /**
  * Initialise the tags for the decoding. The key into the hash maps is 
  * CDRtype hex;Tag ID hex[;Block ID Hex]
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
    
    // List of tag names
    for (int idx=0;idx<10;idx++)
    {
      tagNameSimple[idx] = "Tag"+idx;
    }
  }

  @Override
  public String getTagName(int tagId) {
    if (tagId >= 0 && tagId < tagNameSimple.length)
    {
      return tagNameSimple[tagId];
    }
    else
    {
      return "";
    }
  }

  @Override
  public int getLength(int tagId) {
    return 0;
  }

  @Override
  public int getType(int tagId) {
    return 0;
  }

  @Override
  public String getTypeName(int tagId) {
    return "";
  }
  
  public String getCompositeName(String tagPath)
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
  
  public int getCompositeType(String tagPath)
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
