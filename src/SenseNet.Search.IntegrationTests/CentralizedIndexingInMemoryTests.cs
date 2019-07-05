using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data;
using System.Threading.Tasks;
using SenseNet.Tests.Implementations;
using SenseNet.Tests.Implementations2;

namespace SenseNet.Search.IntegrationTests
{
    [TestClass]
    public class CentralizedIndexingInMemorylTests : CentralizedIndexingTestCases
    {
        protected override DataProvider DataProvider => new InMemoryDataProvider();
        protected override ISharedLockDataProviderExtension SharedLockDataProvider => new InMemorySharedLockDataProvider2();
        protected override IAccessTokenDataProviderExtension AccessTokenDataProvider => new InMemoryAccessTokenDataProvider2();
        protected override IBlobStorageMetaDataProvider BlobStorageMetaDataProvider => new InMemoryBlobStorageMetaDataProvider2();
        protected override ITestingDataProviderExtension TestingDataProvider => new InMemoryTestingDataProvider2();

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_RegisterAndReload()
        {
            await Indexing_Centralized_RegisterAndReload();
        }

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_UpdateStateToDone()
        {
            await Indexing_Centralized_UpdateStateToDone();
        }

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate01_SelectWaiting()
        {
            await Indexing_Centralized_Allocate01_SelectWaiting();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate02_IdDependency()
        {
            await Indexing_Centralized_Allocate02_IdDependency();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate02_IdDependency_VersionId0()
        {
            await Indexing_Centralized_Allocate02_IdDependency_VersionId0();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate03_InactiveDependency()
        {
            await Indexing_Centralized_Allocate03_InactiveDependency();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate04_SelectMore()
        {
            await Indexing_Centralized_Allocate04_SelectMore();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate05_PathDependency()
        {
            await Indexing_Centralized_Allocate05_PathDependency();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate06_Timeout()
        {
            await Indexing_Centralized_Allocate06_Timeout();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate07_MaxRecords()
        {
            await Indexing_Centralized_Allocate07_MaxRecords();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_Allocate08_StateUpdated()
        {
            await Indexing_Centralized_Allocate08_StateUpdated();
        }

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_AllocateAndState()
        {
            await Indexing_Centralized_AllocateAndState();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_RefreshLock()
        {
            await Indexing_Centralized_RefreshLock();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_InMemory_DeleteFinished()
        {
            await Indexing_Centralized_DeleteFinished();
        }
    }
}
