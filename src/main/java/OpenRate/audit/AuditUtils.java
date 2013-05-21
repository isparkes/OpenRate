/* ====================================================================
 * Limited Evaluation License:
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

package OpenRate.audit;

import OpenRate.logging.AstractLogger;
import OpenRate.logging.LogUtil;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Class AuditUtils
 *
 * This class implements the utility functions for controlling the auditing
 * infrastructure, which is used to provide version information in production
 * environments.
 *
 * @author ian
 */
public class AuditUtils
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: AuditUtils.java,v $, $Revision: 1.18 $, $Date: 2013-05-13 18:12:12 $";

  // Get the utilities for handling the XML configuration
  private static AuditUtils auditUtilsObj = null;

  // The key we are using to recover the CVS info
  final static String CVS_INFO_KEY="CVS_MODULE_INFO";
  
  // Whether we log custom classes as well
  private static boolean auditCoreOnly = false;

  // Whether we do logging at all (used for test)
  private static boolean auditLogging = true;

 /**
  * Access to the Framework AstractLogger. All non-pipeline specific messages (e.g.
  * from resources or caches) should go into this log, as well as startup
  * and shutdown messages. Normally the messages will be application driven,
  * not stack traces, which should go into the error log.
  */
  protected static AstractLogger FWLog = LogUtil.getStaticLogger("Framework");

  // The internal map of the version information
  private static HashMap<String,AuditInfoRecord> versionMap;

  private class AuditInfoRecord
  {
    String elementName;
    String version;
    String releaseDate;
    String packageName;
  }

 /**
  * Audit utils provides utility functions for logging and tracing the internal
  * version numbers of key modules in the OpenRate core framework. This allows
  * us to know the CVS version number of the setup that a framework is using
  * and therefore aids in the management of defects.
  *
  * The audit map information is written to the Framework log on startup, and
  * can therefore be easily retrieved.
  */
  public AuditUtils()
  {
    // Nothing to do
  }

 /**
  * Initialse the version map, in readiness for receiving information to log
  */
  private void initVersionMap()
  {
    versionMap = new HashMap<>();

    // Add ourselves
    buildVersionMap(CVS_MODULE_INFO, this.getClass());
  }

  /**
   * Add information to the version map, removing any duplicates as we go. It
   * is normal that we can have multiple copies of a plug in in a framework,
   * and we only want to have each module reported once.
   *
   * @param myVersion the version information passed from the module
   * @param myClass The class that we are adding
   */
  public void buildVersionMap(String myVersion, Class myClass)
  {
    // split up the information
    String formatString = myVersion.replaceAll("OpenRate, \\$RCSfile: ", "");
    formatString = formatString.replaceAll(",v \\$, \\$Revision: ", ";");
    formatString = formatString.replaceAll("\\$, \\$Date: ", ";");
    formatString = formatString.replaceAll("\\$", "");

    // Split the fields
    String[] versionFields = formatString.split(";");

    if (versionFields.length != 3)
    {
      FWLog.error("Unable to parse version info <" + myVersion + ">");
      return;
    }

    // See if we already have this element in the map
    if (versionMap.containsKey(versionFields[0].trim()))
    {
      // we already have this logged, exit
      return;
    }

    // prepare the version info
    AuditInfoRecord tmpRecord = new AuditInfoRecord();
    tmpRecord.elementName = versionFields[0].replace(".java", "").trim();
    tmpRecord.version = versionFields[1].trim();
    tmpRecord.releaseDate = versionFields[2].trim();
    
    // Prepare the package name to remove the class name
    tmpRecord.packageName = myClass.getCanonicalName().replace("."+tmpRecord.elementName, "");

    // add it to the map
    versionMap.put(tmpRecord.elementName,tmpRecord);
  }

  /**
   * Dump the version map to a logger.
   */
  public static void logVersionMap()
  {
    String formatString;

    FWLog.info("-----------------");
    FWLog.info("Start version map");
    // Loop through the keys
    SortedSet<String> sortedSet= new TreeSet<>(versionMap.keySet());
    Iterator<String> mapIter = sortedSet.iterator();
    while (mapIter.hasNext())
    {
      AuditInfoRecord tmpRecord = versionMap.get(mapIter.next());

      formatString = "  " + (tmpRecord.elementName + "                                                            ").substring(0, 60) +
                     (tmpRecord.version + "          ").substring(0, 10) +
                     tmpRecord.releaseDate + "  (" +
                     tmpRecord.packageName + ")";

      FWLog.info(formatString);
    }
    FWLog.info("End version map");
    FWLog.info("-----------------");
  }

 /**
  * Get the description of the hierarchy of the instantiated subclass. This is
  * to be used for auditing the module versions in the Framework version map.
  *
  * @param ourClass The class we need to find the hierarchy of
  */
  public void buildHierarchyVersionMap(Class ourClass)
  {
    Class sup=ourClass;
    String class_name = "";

    if (auditLogging == false)
    {
      return;
    }
    
    // get the information for the base class
    try
    {
      String info = (String)sup.getField(CVS_INFO_KEY).get(this.getClass());
      class_name=sup.getSimpleName();

      // detect the case that we inherited a super CVS_INFO entry
      if (!info.contains(class_name))
      {
        throw new NoSuchFieldException();
      }

      // Add the field to be audited
      buildVersionMap(info,sup);
    }
    catch (NoSuchFieldException ex)
    {
      if (class_name == null)
      {
        FWLog.error("Class <" + sup.getSimpleName() + "> is null");
      }
      else
      {
        String className = ourClass.toString().toUpperCase();
        if ((auditCoreOnly == false) || className.startsWith("CLASS OPENRATE"))
        {
          // Log core classes only
          FWLog.warning("Class <" + class_name + "> does not have version information");
        }    
      }
    }
    catch (IllegalAccessException ex)
    {
      FWLog.error("Access error getting version information for Class <" + class_name + ">");
    }

    // now get the info for all the superclasses
    while ((sup=sup.getSuperclass()) != null)
    {
      String name=sup.getName();

      // we don't care about Object
      if (!name.equals("java.lang.Object"))
      {
        try
        {
          String info = (String)sup.getField(CVS_INFO_KEY).get(this.getClass());
          class_name=sup.getSimpleName();
          if (!info.contains(class_name))
            throw new NoSuchFieldException();

          // Add the field to be audited
          buildVersionMap(info,sup);
        }
        catch (NoSuchFieldException ex)
        {
          String className = ourClass.toString().toUpperCase();
          if ((auditCoreOnly == false) || className.startsWith("CLASS OPENRATE"))
          {
            // Log core classes only
            FWLog.warning("Class <" + class_name + "> does not have version information");
          }    
        }
        catch (IllegalAccessException ex)
        {
          FWLog.error("Access error getting version information for Class <" + class_name + ">");
        }
      }
    }
  }

 /**
  * This utility function returns the singleton instance of AuditUtils
  *
  * @return    the instance of AuditUtils
  */
  public static AuditUtils getAuditUtils()
  {
    if(auditUtilsObj == null)
    {
      auditUtilsObj = new AuditUtils();
      auditUtilsObj.initVersionMap();
    }

    return auditUtilsObj;
  }
  
  /**
   * Get the status of the setting to audit module only to audit OpenRate core 
   * classes (no custom classes)
   * 
   * @return the current value
   */
  public boolean getAuditCoreOnly() 
  {
    return auditCoreOnly;
  }

  /**
   * Set the audit module only to audit OpenRate core classes (no custom classes)
   * 
   * @param newValue the new value to use
   */
  public void setAuditCoreOnly(boolean newValue) 
  {
    auditCoreOnly = newValue;
  }
  
  /**
   * Set the audit module to audit or not. (Used for testing)
   * 
   * @param newValue the new value to use
   */
  public void setAuditLogging(boolean newValue) 
  {
    auditLogging = newValue;
  }
}
