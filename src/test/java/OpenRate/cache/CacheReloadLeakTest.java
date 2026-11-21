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
import OpenRate.db.DBUtil;
import OpenRate.exception.InitializationException;
import TestUtils.FrameworkUtils;
import java.net.URL;
import java.sql.Connection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A cache load that fails part way through must give its connection back.
 *
 * Every DB cache loader has a set of paths that throw InitializationException
 * between getting the connection and closing it, and those used to leave the
 * connection checked out of the pool. At startup that costs nothing, because
 * the process is about to die. ReloadData() is the problem: it runs on a live
 * framework, and it is what the admin console's CacheFactory:Reload does, so
 * a reference data problem that failed the reload cost a pooled connection
 * every time somebody retried it, until the pool was empty.
 *
 * @author ian
 */
public class CacheReloadLeakTest {

  private static URL FQConfigFileName;

  // the data source defined by the test configuration we borrow here
  private static final String DATA_SOURCE = "TestDB";

  @BeforeClass
  public static void setUpClass() throws Exception {
    FQConfigFileName = new URL("File:src/test/resources/TestRegexDB.properties.xml");

    OpenRate.getApplicationInstance();
    FrameworkUtils.loadProperties(FQConfigFileName);
    FrameworkUtils.startupLoggers();
    FrameworkUtils.startupDataSources();
  }

  @AfterClass
  public static void tearDownClass() {
    OpenRate.getApplicationInstance().cleanup();
  }

  /**
   * Stands in for a real loader that gets as far as opening the connection
   * and then finds something wrong with the data.
   */
  private static class FailingCache extends AbstractSyncLoaderCache {

    // the connection handed out by the most recent attempt
    private Connection lastConnection;

    FailingCache() {
      setSymbolicName("FailingCache");
      CacheDataSourceType = "DB";
      cacheDataSourceName = DATA_SOURCE;
    }

    @Override
    public void loadDataFromDB() throws InitializationException {
      JDBCcon = DBUtil.getConnection(cacheDataSourceName);
      lastConnection = JDBCcon;

      throw new InitializationException("simulated reference data problem", getSymbolicName());
    }

    @Override
    public void loadDataFromFile() throws InitializationException {
      // not used by this test
    }

    @Override
    public void loadDataFromMethod() throws InitializationException {
      // not used by this test
    }

    @Override
    public void clearCacheObjects() {
      // nothing cached
    }
  }

  /**
   * Fail the same reload several times over and check that nothing is held on
   * to. Before the loaders were wrapped, every iteration kept its connection
   * and lastConnection.isClosed() was false on the first one.
   */
  @Test
  public void testFailedReloadReturnsTheConnection() throws Exception {
    System.out.println("failed reload returns the connection");

    FailingCache cache = new FailingCache();

    for (int attempt = 1; attempt <= 5; attempt++) {
      try {
        cache.ReloadData();
        Assert.fail("attempt " + attempt + ": the reload was supposed to fail");
      } catch (InitializationException expected) {
        // this is what a bad select or bad reference data looks like
      }

      Assert.assertNull("attempt " + attempt
              + ": the connection reference was left set after a failed reload",
              cache.JDBCcon);

      Assert.assertTrue("attempt " + attempt
              + ": the connection was not given back after a failed reload",
              cache.lastConnection.isClosed());
    }

    // and the pool still has something to hand out
    Connection stillWorks = DBUtil.getConnection(DATA_SOURCE);
    Assert.assertNotNull(stillWorks);
    stillWorks.close();
  }
}
