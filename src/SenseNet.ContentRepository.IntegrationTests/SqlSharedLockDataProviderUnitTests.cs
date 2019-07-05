using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.IntegrationTests.Common;
using STT = System.Threading.Tasks;

namespace SenseNet.ContentRepository.IntegrationTests
{
    [TestClass]
    public class SqlSharedLockDataProviderUnitTests : MsSqlIntegrationTestBase
    {
        private MsSqlSharedLockDataProvider Provider => (MsSqlSharedLockDataProvider)DataStore.GetDataProviderExtension<ISharedLockDataProviderExtension>();

        /* ====================================================================== */

        [TestMethod]
        public async STT.Task SharedLock_LockAndGetLock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var expectedLockValue = Guid.NewGuid().ToString();

                // ACTION
                Provider.CreateSharedLock(nodeId, expectedLockValue);

                // ASSERT
                var actualLockValue = Provider.GetSharedLock(nodeId);
                Assert.AreEqual(expectedLockValue, actualLockValue);
            });
        }
        [TestMethod]
        public async STT.Task SharedLock_GetTimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var expectedLockValue = Guid.NewGuid().ToString();
                Provider.CreateSharedLock(nodeId, expectedLockValue);

                // ACTION
                var timeout = Provider.SharedLockTimeout;
                Provider.SetCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-timeout.TotalMinutes - 1));

                // ASSERT
                Assert.IsNull(SharedLock.GetLock(nodeId));
            });
        }
        [TestMethod]
        public async STT.Task SharedLock_Lock_Same()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
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
            });
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public async STT.Task SharedLock_Lock_Different()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                Provider.CreateSharedLock(nodeId, oldLockValue);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

                // ACTION
                Provider.CreateSharedLock(nodeId, newLockValue);
            });
        }
        [TestMethod]
        public async STT.Task SharedLock_Lock_DifferentTimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
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
            });
        }


        [TestMethod]
        public async STT.Task SharedLock_ModifyLock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                Provider.CreateSharedLock(nodeId, oldLockValue);
                Assert.AreEqual(oldLockValue, Provider.GetSharedLock(nodeId));

                // ACTION
                Provider.ModifySharedLock(nodeId, oldLockValue, newLockValue);

                Assert.AreEqual(newLockValue, Provider.GetSharedLock(nodeId));
            });
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public async STT.Task SharedLock_ModifyLockDifferent()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                Provider.CreateSharedLock(nodeId, oldLockValue);
                Assert.AreEqual(oldLockValue, Provider.GetSharedLock(nodeId));

                // ACTION
                var actualLock = Provider.ModifySharedLock(nodeId, "DifferentLock", newLockValue);

                Assert.AreEqual(oldLockValue, actualLock);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_ModifyLock_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();

                // ACTION
                Provider.ModifySharedLock(nodeId, oldLockValue, newLockValue);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_ModifyLock_TimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                Provider.CreateSharedLock(nodeId, oldLockValue);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

                // ACTION
                Provider.ModifySharedLock(nodeId, oldLockValue, newLockValue);
            });
        }


        [TestMethod]
        public async STT.Task SharedLock_RefreshLock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var lockValue = "LCK_" + Guid.NewGuid();
                Provider.CreateSharedLock(nodeId, lockValue);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

                // ACTION
                Provider.RefreshSharedLock(nodeId, lockValue);

                Assert.IsTrue((DateTime.UtcNow - GetSharedLockCreationDate(nodeId)).TotalSeconds < 1);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public async STT.Task SharedLock_RefreshLock_Different()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var lockValue = "LCK_" + Guid.NewGuid();
                Provider.CreateSharedLock(nodeId, lockValue);

                // ACTION
                var actualLock = Provider.RefreshSharedLock(nodeId, "DifferentLock");

                Assert.AreEqual(lockValue, actualLock);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_RefreshLock_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var lockValue = "LCK_" + Guid.NewGuid();

                // ACTION
                Provider.RefreshSharedLock(nodeId, lockValue);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_RefreshLock_TimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var lockValue = Guid.NewGuid().ToString();
                Provider.CreateSharedLock(nodeId, lockValue);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

                // ACTION
                Provider.RefreshSharedLock(nodeId, lockValue);
            });
        }


        [TestMethod]
        public async STT.Task SharedLock_Unlock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var existingLock = "LCK_" + Guid.NewGuid();
                Provider.CreateSharedLock(nodeId, existingLock);

                // ACTION
                Provider.DeleteSharedLock(nodeId, existingLock);

                Assert.IsNull(Provider.GetSharedLock(nodeId));
            });
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public async STT.Task SharedLock_Unlock_Different()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var existingLock = "LCK_" + Guid.NewGuid();
                Provider.CreateSharedLock(nodeId, existingLock);

                // ACTION
                var actualLock = Provider.DeleteSharedLock(nodeId, "DifferentLock");

                Assert.AreEqual(existingLock, actualLock);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_Unlock_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var existingLock = "LCK_" + Guid.NewGuid();

                // ACTION
                Provider.DeleteSharedLock(nodeId, existingLock);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_Unlock_TimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks();
                const int nodeId = 42;
                Assert.IsNull(Provider.GetSharedLock(nodeId));
                var existingLock = Guid.NewGuid().ToString();
                Provider.CreateSharedLock(nodeId, existingLock);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

                // ACTION
                Provider.DeleteSharedLock(nodeId, existingLock);
            });
        }

        /* ====================================================================== Tools */

        private void SetSharedLockCreationDate(int nodeId, DateTime value)
        {
            if (!(Provider is MsSqlSharedLockDataProvider dataProvider))
                throw new InvalidOperationException("InMemorySharedLockDataProvider not configured.");

            dataProvider.SetCreationDate(nodeId, value);
        }
        private DateTime GetSharedLockCreationDate(int nodeId)
        {
            if (!(Provider is MsSqlSharedLockDataProvider dataProvider))
                throw new InvalidOperationException("InMemorySharedLockDataProvider not configured.");

            return dataProvider.GetCreationDate(nodeId);
        }

    }
}
