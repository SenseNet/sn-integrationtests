using System;
using System.Threading;
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
        private MsSqlSharedLockDataProvider Provider => (MsSqlSharedLockDataProvider)Providers.Instance.DataProvider.GetExtension<ISharedLockDataProviderExtension>();

        /* ====================================================================== */

        [TestMethod]
        public async STT.Task SharedLock_LockAndGetLock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var expectedLockValue = Guid.NewGuid().ToString();

                // ACTION
                await Provider.CreateSharedLockAsync(nodeId, expectedLockValue, CancellationToken.None);

                // ASSERT
                var actualLockValue = await Provider.GetSharedLockAsync(nodeId, CancellationToken.None);
                Assert.AreEqual(expectedLockValue, actualLockValue);
            });
        }
        [TestMethod]
        public async STT.Task SharedLock_GetTimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var expectedLockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, expectedLockValue, CancellationToken.None);

                // ACTION
                var timeout = Provider.SharedLockTimeout;
                TDP.SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-timeout.TotalMinutes - 1));

                // ASSERT
                Assert.IsNull(SharedLock.GetLock(nodeId, CancellationToken.None));
            });
        }
        [TestMethod]
        public async STT.Task SharedLock_Lock_Same()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var expectedLockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, expectedLockValue, CancellationToken.None);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

                // ACTION
                await Provider.CreateSharedLockAsync(nodeId, expectedLockValue, CancellationToken.None);

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
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, oldLockValue, CancellationToken.None);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

                // ACTION
                await Provider.CreateSharedLockAsync(nodeId, newLockValue, CancellationToken.None);
            });
        }
        [TestMethod]
        public async STT.Task SharedLock_Lock_DifferentTimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, oldLockValue, CancellationToken.None);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

                // ACTION
                await Provider.CreateSharedLockAsync(nodeId, newLockValue, CancellationToken.None);

                var actualLockValue = await Provider.GetSharedLockAsync(nodeId, CancellationToken.None);
                Assert.AreEqual(newLockValue, actualLockValue);
            });
        }


        [TestMethod]
        public async STT.Task SharedLock_ModifyLock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, oldLockValue, CancellationToken.None);
                Assert.AreEqual(oldLockValue, await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));

                // ACTION
                await Provider.ModifySharedLockAsync(nodeId, oldLockValue, newLockValue, CancellationToken.None);

                Assert.AreEqual(newLockValue, await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
            });
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public async STT.Task SharedLock_ModifyLockDifferent()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, oldLockValue, CancellationToken.None);
                Assert.AreEqual(oldLockValue, await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));

                // ACTION
                var actualLock = await Provider.ModifySharedLockAsync(nodeId, "DifferentLock", newLockValue, CancellationToken.None);

                Assert.AreEqual(oldLockValue, actualLock);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_ModifyLock_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();

                // ACTION
                await Provider.ModifySharedLockAsync(nodeId, oldLockValue, newLockValue, CancellationToken.None);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_ModifyLock_TimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var oldLockValue = Guid.NewGuid().ToString();
                var newLockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, oldLockValue, CancellationToken.None);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

                // ACTION
                await Provider.ModifySharedLockAsync(nodeId, oldLockValue, newLockValue, CancellationToken.None);
            });
        }


        [TestMethod]
        public async STT.Task SharedLock_RefreshLock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var lockValue = "LCK_" + Guid.NewGuid();
                await Provider.CreateSharedLockAsync(nodeId, lockValue, CancellationToken.None);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddMinutes(-10.0d));

                // ACTION
                await Provider.RefreshSharedLockAsync(nodeId, lockValue, CancellationToken.None);

                Assert.IsTrue((DateTime.UtcNow - GetSharedLockCreationDate(nodeId)).TotalSeconds < 1);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public async STT.Task SharedLock_RefreshLock_Different()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var lockValue = "LCK_" + Guid.NewGuid();
                await Provider.CreateSharedLockAsync(nodeId, lockValue, CancellationToken.None);

                // ACTION
                var actualLock = await Provider.RefreshSharedLockAsync(nodeId, "DifferentLock", CancellationToken.None);

                Assert.AreEqual(lockValue, actualLock);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_RefreshLock_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var lockValue = "LCK_" + Guid.NewGuid();

                // ACTION
                await Provider.RefreshSharedLockAsync(nodeId, lockValue, CancellationToken.None);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_RefreshLock_TimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var lockValue = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, lockValue, CancellationToken.None);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

                // ACTION
                await Provider.RefreshSharedLockAsync(nodeId, lockValue, CancellationToken.None);
            });
        }


        [TestMethod]
        public async STT.Task SharedLock_Unlock()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var existingLock = "LCK_" + Guid.NewGuid();
                await Provider.CreateSharedLockAsync(nodeId, existingLock, CancellationToken.None);

                // ACTION
                await Provider.DeleteSharedLockAsync(nodeId, existingLock, CancellationToken.None);

                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
            });
        }
        [TestMethod]
        [ExpectedException(typeof(LockedNodeException))]
        public async STT.Task SharedLock_Unlock_Different()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var existingLock = "LCK_" + Guid.NewGuid();
                await Provider.CreateSharedLockAsync(nodeId, existingLock, CancellationToken.None);

                // ACTION
                var actualLock = await Provider.DeleteSharedLockAsync(nodeId, "DifferentLock", CancellationToken.None);

                Assert.AreEqual(existingLock, actualLock);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_Unlock_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var existingLock = "LCK_" + Guid.NewGuid();

                // ACTION
                await Provider.DeleteSharedLockAsync(nodeId, existingLock, CancellationToken.None);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(SharedLockNotFoundException))]
        public async STT.Task SharedLock_Unlock_TimedOut()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                SharedLock.RemoveAllLocks(CancellationToken.None);
                const int nodeId = 42;
                Assert.IsNull(await Provider.GetSharedLockAsync(nodeId, CancellationToken.None));
                var existingLock = Guid.NewGuid().ToString();
                await Provider.CreateSharedLockAsync(nodeId, existingLock, CancellationToken.None);
                SetSharedLockCreationDate(nodeId, DateTime.UtcNow.AddHours(-1.0d));

                // ACTION
                await Provider.DeleteSharedLockAsync(nodeId, existingLock, CancellationToken.None);
            });
        }

        /* ====================================================================== Tools */

        private void SetSharedLockCreationDate(int nodeId, DateTime value)
        {
            TDP.SetSharedLockCreationDate(nodeId, value);
        }
        private DateTime GetSharedLockCreationDate(int nodeId)
        {
            return TDP.GetSharedLockCreationDate(nodeId);
        }

    }
}
