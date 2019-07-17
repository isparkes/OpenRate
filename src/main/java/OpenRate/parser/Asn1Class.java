package OpenRate.parser;

import java.util.ArrayList;

public class Asn1Class
{
  private boolean nullTag = false;
  private String tagname = "";
  private String rawTag = "";
  private int tag = 0;
  private int id = 0;
  private int length = 0;
  private byte[] value;
  public int RECORD_TYPE = 0;

  public int TAG_MASK = 0x1F;         /* Bits 5 - 1 */
  public int LEN_XTND = 0x80;         /* Indefinite or long form */
  public int LEN_MASK = 0x7F;         /* Bits 7 - 1 */
  public int EOC = 0x00;              /* End of content Octet */
  public int FORM_MASK = 0x20;        /* Bit 6 Mask */
  public int PRIMITIVE = 0x00;        /* Bit 6: 0 = primitive */
  public int CONSTRUCTED = 0x20;      /* Bit 6: 1 = constructed */

  public Asn1Class() {

  }

 /**
  * Tells us if the tag is the header of a constructed element.
  * 
  * @return True if the element is a constructed class header
  */
  public boolean isConstructed() {
    return ((this.id & FORM_MASK) == CONSTRUCTED);
  }

 /**
  * Set the length of the tag
  * 
  * @param length The length to set
  */
  public void setLength(int length) {
    this.length = length;
  }

  public void setTag(int tag) {
    this.tag = tag;
  }

 /**
  * Set the raw (unprocessed) tag value. This is useful for some ASN.1 types
  * which make unusual use of the tag values (e.g. Huawei).
  * 
  * @param tag The tag to get the value from
  */
  public void setRawTag(String tag) {
    this.rawTag = tag;
  }

 /**
  * Get the raw (unprocessed) tag value as a Hex value. This is useful for some 
  * ASN.1 types which make unusual use of the tag values (e.g. Huawei).
  * 
  * @return tag The tag to get the value from
  */
  public String getRawTag() {
    return this.rawTag;
  }
  
 /**
  * Set the value as a byte array.
  * 
  * @param the value in the original byte form
  */
  public void setValue(byte[] value) {
    this.value = value;
  }

 /**
  * Get the value as the original byte array.
  * 
  * @return the value in the original byte form
  */
  public byte[] getOrigValue() {
    return this.value;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public int getLength() {
    return this.length;
  }

  public int getTag() {
    return this.tag;
  }

  public String getValue() {
    if (this.value != null ) {
      return parseIA5String(this.value);
    } else {
      return "";
    }
  }

  private String parseIA5String(byte[] value) {
    String output = "";
    int i,len = value.length;
    for (i = 0; i < len; ++i) {
        char c = (char) ( value[i] & 0xFF );
        if ((c < 0) || (c > 127)) return "";
        output += (char)( value[i] & 0xFF );
    }
    return output;
  }

  private int setTagByteArray(byte[] newTag) throws Exception {
    byte readByte;
    int myTag;
    myTag = newTag[0];
    setId(myTag & ~TAG_MASK);
    myTag &= TAG_MASK;

    if (myTag == TAG_MASK) {
      /* Long tag encoded as sequence of 7-bit values.  This doesn't try to
      handle tags > INT_MAX, it'd be pretty peculiar ASN.1 if it had to
      use tags this large */
      myTag = 0;
      int index = 1;
      do {
        readByte = newTag[index];
        myTag = (myTag << 7) | (readByte & 0x7F);
        index++;
      } while (((readByte & LEN_XTND) != 0) && (index < 5));
      if (index == 5) {
        throw new Exception("Tag has illegal length.");
      }
    }
    return myTag;
  }

  public void setTagFromByteArray(byte[] header) throws Exception {
    setTag(setTagByteArray(header));
  }

 /**
  * Get the dump information for this record
  *
  * @return Error Dump Information
  */
  public ArrayList<String> getDumpInfo() {

    ArrayList<String> tmpDumpList;
    tmpDumpList = new ArrayList<>();

    // Format the fields
    tmpDumpList.add("  " + this.getTagname() + " = <" + this.getValue() + ">");

    return tmpDumpList;
  }

  /**
   * Get the tag name of the tag
   * 
   * @return the tag name
   */
  public String getTagname() {
    return tagname;
  }

  /**
   * Set the tag name of the tag.
   * 
   * @param tagname the tag name to set
   */
  public void setTagname(String tagname) {
    this.tagname = tagname;
  }

  /**
   * @return the nullTag
   */
  public boolean isNullTag() {
    return nullTag;
  }

  /**
   * @param nullTag the nullTag to set
   */
  public void setNullTag(boolean nullTag) {
    this.nullTag = nullTag;
  }

  }