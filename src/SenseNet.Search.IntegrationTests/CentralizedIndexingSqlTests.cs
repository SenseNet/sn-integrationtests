using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data;
using System.Threading.Tasks;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.IntegrationTests.Common.Implementations;
using SenseNet.Tests.Implementations;

namespace SenseNet.Search.IntegrationTests
{
    [TestClass]
    public class CentralizedIndexingSqlTests : CentralizedIndexingTestCases
    {
        protected override DataProvider DataProvider => new MsSqlDataProvider();
        protected override ISharedLockDataProviderExtension SharedLockDataProvider => new MsSqlSharedLockDataProvider();
        protected override IAccessTokenDataProviderExtension AccessTokenDataProvider => new MsSqlAccessTokenDataProvider();
        protected override IBlobStorageMetaDataProvider BlobStorageMetaDataProvider => new MsSqlBlobMetaDataProvider();
        protected override ITestingDataProviderExtension TestingDataProvider => new MsSqlTestingDataProvider();

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_RegisterAndReload()
        {
            await Indexing_Centralized_RegisterAndReload();
        }

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_UpdateStateToDone()
        {
            await Indexing_Centralized_UpdateStateToDone();
        }

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate01_SelectWaiting()
        {
            await Indexing_Centralized_Allocate01_SelectWaiting();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate02_IdDependency()
        {
            await Indexing_Centralized_Allocate02_IdDependency();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate02_IdDependency_VersionId0()
        {
            await Indexing_Centralized_Allocate02_IdDependency_VersionId0();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate03_InactiveDependency()
        {
            await Indexing_Centralized_Allocate03_InactiveDependency();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate04_SelectMore()
        {
            await Indexing_Centralized_Allocate04_SelectMore();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate05_PathDependency()
        {
            await Indexing_Centralized_Allocate05_PathDependency();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate06_Timeout()
        {
            await Indexing_Centralized_Allocate06_Timeout();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate07_MaxRecords()
        {
            await Indexing_Centralized_Allocate07_MaxRecords();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_Allocate08_StateUpdated()
        {
            await Indexing_Centralized_Allocate08_StateUpdated();
        }

        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_AllocateAndState()
        {
            await Indexing_Centralized_AllocateAndState();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_RefreshLock()
        {
            await Indexing_Centralized_RefreshLock();
        }
        [TestMethod, TestCategory("IR")]
        public async Task Indexing_Centralized_MsSql_DeleteFinished()
        {
            await Indexing_Centralized_DeleteFinished();
        }
    }
}
