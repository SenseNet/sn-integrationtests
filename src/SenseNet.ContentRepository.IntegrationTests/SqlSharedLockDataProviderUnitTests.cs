using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.ContentRepository.Storage.Security;

namespace SenseNet.ContentRepository.IntegrationTests
{
    [TestClass]
    public class SqlSharedLockDataProviderUnitTests : ContentRepositoryIntegrationTestBase
    {
        [TestInitialize]
        public void InitializeTest()
        {
            // set default implementation directly to avoid repository start
            Providers.Instance.DataProvider2 = new MsSqlDataProvider();
            DataStore.DataProvider.SetExtension(typeof(ISharedLockDataProviderExtension), new SqlSharedLockDataProvider());

            DataStore.Enabled = true;

            SharedLock.RemoveAllLocks();
        }

        [ClassCleanup]
        public static void CleanupClass()
        {
            TearDown();
        }

        private SqlSharedLockDataProvider Provider => (SqlSharedLockDataProvider)DataStore.GetDataProviderExtension<ISharedLockDataProviderExtension>();

        /* ====================================================================== */

        [TestMethod]
        public void SharedLock_LockAndGetLock()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var expectedLockValue = Guid.NewGuid().ToString();

            // ACTION
            Provider.CreateSharedLock(nodeId, expectedLockValue);

            // ASSERT
            var actualLockValue = Provider.GetSharedLock(nodeId);
            Assert.AreEqual(expectedLockValue, actualLockValue);
        }
        [TestMethod]
        public void SharedLock_GetTimedOut()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var expectedLockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, expectedLockValue);

            // ACTION
            var timeout = Provider.SharedLockTimeout;
            Provider.SetCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-timeout.TotalMinutes - 1));

            // ASSERT
            Assert.IsNull(SharedLock.GetLock(nodeId));
        }
        [TestMethod]
        public void SharedLock_Lock_Same()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var expectedLockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, expectedLockValue);
            SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

            // ACTION
            Provider.CreateSharedLock(nodeId, expectedLockValue);

            // ASSERT
            // Equivalent to the refresh lock
            var now = DateTime.UtcNow;
            var actualDate = GetSharedLockCreationDate(nodeId);
            var delta = (now - actualDate).TotalSeconds;
            Assert.IsTrue(delta < 1);
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public void SharedLock_Lock_Different()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var oldLockValue = Guid.NewGuid().ToString();
            var newLockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, oldLockValue);
            SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

            // ACTION
            Provider.CreateSharedLock(nodeId, newLockValue);
        }
        [TestMethod]
        public void SharedLock_Lock_DifferentTimedOut()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var oldLockValue = Guid.NewGuid().ToString();
            var newLockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, oldLockValue);
            SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

            // ACTION
            Provider.CreateSharedLock(nodeId, newLockValue);

            var actualLockValue = Provider.GetSharedLock(nodeId);
            Assert.AreEqual(newLockValue, actualLockValue);
        }


        [TestMethod]
        public void SharedLock_ModifyLock()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var oldLockValue = Guid.NewGuid().ToString();
            var newLockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, oldLockValue);
            Assert.AreEqual(oldLockValue, Provider.GetSharedLock(nodeId));

            // ACTION
            Provider.ModifySharedLock(nodeId, oldLockValue, newLockValue);

            Assert.AreEqual(newLockValue, Provider.GetSharedLock(nodeId));
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public void SharedLock_ModifyLockDifferent()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var oldLockValue = Guid.NewGuid().ToString();
            var newLockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, oldLockValue);
            Assert.AreEqual(oldLockValue, Provider.GetSharedLock(nodeId));

            // ACTION
            var actualLock = Provider.ModifySharedLock(nodeId, "DifferentLock", newLockValue);

            Assert.AreEqual(oldLockValue, actualLock);
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public void SharedLock_ModifyLock_Missing()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var oldLockValue = Guid.NewGuid().ToString();
            var newLockValue = Guid.NewGuid().ToString();

            // ACTION
            Provider.ModifySharedLock(nodeId, oldLockValue, newLockValue);
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public void SharedLock_ModifyLock_TimedOut()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var oldLockValue = Guid.NewGuid().ToString();
            var newLockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, oldLockValue);
            SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

            // ACTION
            Provider.ModifySharedLock(nodeId, oldLockValue, newLockValue);
        }


        [TestMethod]
        public void SharedLock_RefreshLock()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var lockValue = "LCK_" + Guid.NewGuid();
            Provider.CreateSharedLock(nodeId, lockValue);
            SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

            // ACTION
            Provider.RefreshSharedLock(nodeId, lockValue);

            Assert.IsTrue((DateTime.UtcNow - GetSharedLockCreationDate(nodeId)).TotalSeconds < 1);
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public void SharedLock_RefreshLock_Different()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var lockValue = "LCK_" + Guid.NewGuid();
            Provider.CreateSharedLock(nodeId, lockValue);

            // ACTION
            var actualLock = Provider.RefreshSharedLock(nodeId, "DifferentLock");

            Assert.AreEqual(lockValue, actualLock);
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public void SharedLock_RefreshLock_Missing()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var lockValue = "LCK_" + Guid.NewGuid();

            // ACTION
            Provider.RefreshSharedLock(nodeId, lockValue);
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public void SharedLock_RefreshLock_TimedOut()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var lockValue = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, lockValue);
            SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

            // ACTION
            Provider.RefreshSharedLock(nodeId, lockValue);
        }


        [TestMethod]
        public void SharedLock_Unlock()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var existingLock = "LCK_" + Guid.NewGuid();
            Provider.CreateSharedLock(nodeId, existingLock);

            // ACTION
            Provider.DeleteSharedLock(nodeId, existingLock);

            Assert.IsNull(Provider.GetSharedLock(nodeId));
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public void SharedLock_Unlock_Different()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var existingLock = "LCK_" + Guid.NewGuid();
            Provider.CreateSharedLock(nodeId, existingLock);

            // ACTION
            var actualLock = Provider.DeleteSharedLock(nodeId, "DifferentLock");

            Assert.AreEqual(existingLock, actualLock);
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public void SharedLock_Unlock_Missing()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var existingLock = "LCK_" + Guid.NewGuid();

            // ACTION
            Provider.DeleteSharedLock(nodeId, existingLock);
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public void SharedLock_Unlock_TimedOut()
        {
            const int nodeId = 42;
            Assert.IsNull(Provider.GetSharedLock(nodeId));
            var existingLock = Guid.NewGuid().ToString();
            Provider.CreateSharedLock(nodeId, existingLock);
            SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

            // ACTION
            Provider.DeleteSharedLock(nodeId, existingLock);
        }

        /* ====================================================================== Tools */

        private void SetSharedLockCreationDate(int nodeId, DateTime value)
        {
            if (!(Provider is SqlSharedLockDataProvider dataProvider))
                throw new InvalidOperationException("InMemorySharedLockDataProvider not configured.");

            dataProvider.SetCreationDate(nodeId, value);
        }
        private DateTime GetSharedLockCreationDate(int nodeId)
        {
            if (!(Provider is SqlSharedLockDataProvider dataProvider))
                throw new InvalidOperationException("InMemorySharedLockDataProvider not configured.");

            return dataProvider.GetCreationDate(nodeId);
        }

    }
}
