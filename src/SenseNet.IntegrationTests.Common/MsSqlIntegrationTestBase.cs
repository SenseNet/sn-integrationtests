using Microsoft.Extensions.Options;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.IntegrationTests.Common.Implementations;
using SenseNet.Tests.Implementations;

namespace SenseNet.IntegrationTests.Common
{
    public abstract class MsSqlIntegrationTestBase : IntegrationTestBase
    {
        protected override DataProvider DataProvider => new MsSqlDataProvider(Options.Create(ConnectionStringOptions.GetLegacyConnectionStrings()));
        protected override ISharedLockDataProviderExtension SharedLockDataProvider => new MsSqlSharedLockDataProvider();
        protected override IAccessTokenDataProviderExtension AccessTokenDataProvider => new MsSqlAccessTokenDataProvider();
        protected override IBlobStorageMetaDataProvider BlobStorageMetaDataProvider => new MsSqlBlobMetaDataProvider(
            Providers.Instance.BlobProviders,
            Options.Create(DataOptions.GetLegacyConfiguration()),
            Options.Create(BlobStorageOptions.GetLegacyConfiguration()),
            Options.Create(ConnectionStringOptions.GetLegacyConnectionStrings()));
        protected override ITestingDataProviderExtension TestingDataProvider => new MsSqlTestingDataProvider();

        // ReSharper disable once InconsistentNaming
        protected MsSqlDataProvider DP => (MsSqlDataProvider)Providers.Instance.DataStore.DataProvider;
        // ReSharper disable once InconsistentNaming
        protected MsSqlTestingDataProvider TDP => (MsSqlTestingDataProvider)DataProvider.GetExtension<ITestingDataProviderExtension>();

    }
}
