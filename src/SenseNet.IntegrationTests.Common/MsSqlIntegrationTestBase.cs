using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.IntegrationTests.Common.Implementations;
using SenseNet.Tests.Implementations;

namespace SenseNet.IntegrationTests.Common
{
    public abstract class MsSqlIntegrationTestBase : IntegrationTestBase
    {
        protected override DataProvider DataProvider => new MsSqlDataProvider();
        protected override ISharedLockDataProviderExtension SharedLockDataProvider => new SqlSharedLockDataProvider();
        protected override IAccessTokenDataProviderExtension AccessTokenDataProvider => new SqlAccessTokenDataProvider();
        protected override IBlobStorageMetaDataProvider BlobStorageMetaDataProvider => new MsSqlBlobMetaDataProvider();
        protected override ITestingDataProviderExtension TestingDataProvider => new SqlTestingDataProvider();

        // ReSharper disable once InconsistentNaming
        protected MsSqlDataProvider DP => (MsSqlDataProvider)DataStore.DataProvider;
        // ReSharper disable once InconsistentNaming
        protected SqlTestingDataProvider TDP => (SqlTestingDataProvider)DataStore.GetDataProviderExtension<ITestingDataProviderExtension>();

    }
}
