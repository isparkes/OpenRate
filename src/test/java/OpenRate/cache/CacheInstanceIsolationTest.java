/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package OpenRate.cache;

import OpenRate.OpenRate;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import OpenRate.record.RateMapEntry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Two caches of the same class must not share their configuration.
 *
 * Configuring the same cache class twice is ordinary: a pipeline that rates
 * both retail and wholesale traffic has a price cache for each. The select
 * statements, the data file names and the prepared statements were all held
 * in static fields, so the two instances were writing over each other during
 * startup. Whichever cache the CacheFactory initialised last decided the SQL,
 * and both then loaded from that one place. Nothing was logged, because each
 * cache reported the name it had been given while reading the other one's
 * rates.
 *
 * @author ian
 */
public class CacheInstanceIsolationTest {

  private static URL FQConfigFileName;

  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestCacheIsolation.properties.xml");

    OpenRate.getApplicationInstance();
    FrameworkUtils.loadProperties(FQConfigFileName);
    FrameworkUtils.startupLoggers();
    FrameworkUtils.startupDataSources();

    Connection connection = FrameworkUtils.getDBConnection("RetailRateCache");

    // Two price models, in two tables, with deliberately different rates
    createPriceModel(connection, "ISO_RETAIL_PRICE_MODEL", "RetailModel", 0.10);
    createPriceModel(connection, "ISO_WHOLESALE_PRICE_MODEL", "WholesaleModel", 0.02);

    createRUMMap(connection, "ISO_RETAIL_RUM_MAP", "RetailGroup", "RetailModel");
    createRUMMap(connection, "ISO_WHOLESALE_RUM_MAP", "WholesaleGroup", "WholesaleModel");

    connection.close();
  }

  @AfterClass
  public static void tearDownClass() {
    OpenRate.getApplicationInstance().cleanup();
  }

  /**
   * Load both caches the way the CacheFactory does, one after the other, and
   * check each one ends up with its own rates.
   *
   * Before the configuration was made per instance, the second loadCache()
   * overwrote the first one's select statements. Both caches then held
   * WholesaleModel and getPriceModel("RetailModel") returned null.
   */
  @Test
  public void testTwoCachesOfTheSameClassKeepTheirOwnConfiguration() throws Exception {
    System.out.println("two caches of the same class keep their own configuration");

    RUMRateCache retail = new RUMRateCache();
    retail.setSymbolicName("RetailRateCache");
    retail.loadCache("CacheFactory", "RetailRateCache");

    RUMRateCache wholesale = new RUMRateCache();
    wholesale.setSymbolicName("WholesaleRateCache");
    wholesale.loadCache("CacheFactory", "WholesaleRateCache");

    // Each cache has the model from its own table
    assertRate(retail, "RetailModel", 0.10);
    assertRate(wholesale, "WholesaleModel", 0.02);

    // and does not have the other one's
    Assert.assertNull("the retail cache picked up the wholesale price model",
            retail.getPriceModel("WholesaleModel"));
    Assert.assertNull("the wholesale cache picked up the retail price model",
            wholesale.getPriceModel("RetailModel"));

    // The RUM maps are loaded by a second statement, which was shared the
    // same way, so check those too
    Assert.assertNotNull("the retail cache has no RUM map of its own",
            retail.getRUMMap("RetailGroup"));
    Assert.assertNull("the retail cache picked up the wholesale RUM map",
            retail.getRUMMap("WholesaleGroup"));
    Assert.assertNotNull("the wholesale cache has no RUM map of its own",
            wholesale.getRUMMap("WholesaleGroup"));
    Assert.assertNull("the wholesale cache picked up the retail RUM map",
            wholesale.getRUMMap("RetailGroup"));

    // Reloading one must not disturb the other, which is what the admin
    // console's CacheFactory:Reload does on a running framework
    retail.ReloadData();

    assertRate(retail, "RetailModel", 0.10);
    assertRate(wholesale, "WholesaleModel", 0.02);
  }

  private void assertRate(RUMRateCache cache, String modelName, double expectedFactor) {
    ArrayList<RateMapEntry> model = cache.getPriceModel(modelName);

    Assert.assertNotNull("cache <" + cache.getSymbolicName() + "> has no model <"
            + modelName + ">", model);
    Assert.assertEquals("cache <" + cache.getSymbolicName() + "> loaded the wrong rate",
            expectedFactor, model.get(0).getFactor(), 0.0001);
  }

  private static void createPriceModel(Connection connection, String table,
          String modelName, double factor) throws SQLException {
    dropIfPresent(connection, table);

    connection.prepareStatement("CREATE TABLE " + table
            + " (ID int,PRICE_MODEL varchar(64) NOT NULL,STEP int DEFAULT 0 NOT NULL,"
            + "TIER_FROM int,TIER_TO int,BEAT int,FACTOR double,CHARGE_BASE int,"
            + "VALID_FROM DATE)").execute();

    connection.prepareStatement("INSERT INTO " + table
            + " (ID,PRICE_MODEL,STEP,TIER_FROM,TIER_TO,BEAT,FACTOR,CHARGE_BASE,VALID_FROM)"
            + " VALUES (1,'" + modelName + "',1,0,999999,1," + factor + ",1,'2000-01-01')").execute();
  }

  private static void createRUMMap(Connection connection, String table,
          String groupName, String modelName) throws SQLException {
    dropIfPresent(connection, table);

    connection.prepareStatement("CREATE TABLE " + table
            + " (ID int, PRICE_GROUP varchar(24), PRICE_MODEL varchar(24), RUM varchar(24),"
            + " RESOURCE varchar(24), RUM_TYPE varchar(24), CONSUME_FLAG int)").execute();

    connection.prepareStatement("INSERT INTO " + table
            + " (ID,PRICE_GROUP,PRICE_MODEL,RUM,RESOURCE,RUM_TYPE,CONSUME_FLAG)"
            + " VALUES (1,'" + groupName + "','" + modelName + "','DUR','EUR','TIERED',0)").execute();
  }

  private static void dropIfPresent(Connection connection, String table) throws SQLException {
    try {
      connection.prepareStatement("DROP TABLE " + table).execute();
    } catch (SQLException ex) {
      if ((ex.getMessage().startsWith("Unknown table") == false)
              && (ex.getMessage().startsWith("user lacks") == false)) {
        throw ex;
      }
    }
  }
}
