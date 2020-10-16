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
        protected override ISharedLockDataProviderExtension SharedLockDataProvider => new MsSqlSharedLockDataProvider();
        protected override IAccessTokenDataProviderExtension AccessTokenDataProvider => new MsSqlAccessTokenDataProvider();
        protected override IBlobStorageMetaDataProvider BlobStorageMetaDataProvider => new MsSqlBlobMetaDataProvider();
        protected override ITestingDataProviderExtension TestingDataProvider => new MsSqlTestingDataProvider();

        // ReSharper disable once InconsistentNaming
        protected MsSqlDataProvider DP => (MsSqlDataProvider)DataStore.DataProvider;
        // ReSharper disable once InconsistentNaming
        protected MsSqlTestingDataProvider TDP => (MsSqlTestingDataProvider)DataStore.GetDataProviderExtension<ITestingDataProviderExtension>();

    }
}
