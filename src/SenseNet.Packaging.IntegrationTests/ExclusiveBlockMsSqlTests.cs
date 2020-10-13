using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using ConnectionStrings = SenseNet.Configuration.ConnectionStrings;

namespace SenseNet.Packaging.IntegrationTests
{
    [TestClass]
    public class ExclusiveBlockMsSqlTests : ExclusiveBlockTestCases
    {
        protected override DataProvider GetMainDataProvider()
        {
            ConnectionStrings.ConnectionString =
                SenseNet.IntegrationTests.Common.ConnectionStrings.ForContentRepositoryTests;
            return new MsSqlDataProvider();
        }
        protected override IExclusiveLockDataProviderExtension GetDataProviderExtension()
        {
            return new MsSqlExclusiveLockDataProvider();
        }

        [TestMethod]
        public void ExclusiveBlock_MsSql_SkipIfLocked()
        {
            TestCase_SkipIfLocked();
        }
        [TestMethod]
        public void ExclusiveBlock_MsSql_WaitForReleased()
        {
            TestCase_WaitForReleased();
        }
        [TestMethod]
        public void ExclusiveBlock_MsSql_WaitAndAcquire()
        {
            TestCase_WaitAndAcquire();
        }
    }
}
