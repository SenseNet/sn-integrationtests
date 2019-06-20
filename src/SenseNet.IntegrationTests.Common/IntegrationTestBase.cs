using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Security;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.ContentRepository.Storage.DataModel;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.Diagnostics;
using SenseNet.IntegrationTests.Common.Implementations;
using SenseNet.Search;
using SenseNet.Security;
using SenseNet.Security.Data;
using SenseNet.Tests;
using SenseNet.Tests.Implementations;

namespace SenseNet.IntegrationTests.Common
{
    public abstract class IntegrationTestBase
    {
        #region Infrastructure

        public TestContext TestContext { get; set; }

        private static IntegrationTestBase _instance;
        private static RepositoryInstance _repositoryInstance;

        private string _databaseName;
        private string _connectionString;
        private string _connectionStringBackup;
        private string _securityConnectionStringBackup;

        public System.Threading.Tasks.Task StorageTest(Func<System.Threading.Tasks.Task> callback)
        {
            return StorageTest(false, callback);
        }
        public System.Threading.Tasks.Task IsolatedStorageTest(Func<System.Threading.Tasks.Task> callback)
        {
            return StorageTest(true, callback);
        }

        private async System.Threading.Tasks.Task StorageTest(bool isolated, Func<System.Threading.Tasks.Task> callback)
        {
            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("START test: {0}", TestContext.TestName);
            //if(SnTrace.SnTracers.Count == 1)
            //    SnTrace.SnTracers.Add(new SnDebugViewTracer());

            DataStore.Enabled = true;

            if (isolated)
                _instance = null;

            var brandNew = EnsureDatabase();

            DistributedApplication.Cache.Reset();
            ContentTypeManager.Reset();
            Providers.Instance.NodeTypeManeger = null;


            if (isolated || brandNew)
            {
                _repositoryInstance?.Dispose();
                _repositoryInstance = null;

                //var portalContextAcc = new PrivateType(typeof(PortalContext));
                //portalContextAcc.SetStaticField("_sites", new Dictionary<string, Site>());

                var builder = CreateRepositoryBuilderForStorageTest();

                Indexing.IsOuterSearchEngineEnabled = true;

                DistributedApplication.Cache.Reset();
                ContentTypeManager.Reset();
                _repositoryInstance = Repository.Start(builder);
                PrepareRepository();
            }

            try
            {
                using (new SystemAccount())
                    await callback();
            }
            finally
            {
                if (isolated)
                {
                    _repositoryInstance?.Dispose();
                    _instance = null;
                }
            }
        }
        public async System.Threading.Tasks.Task NoRepositoryIntegrtionTest(Func<System.Threading.Tasks.Task> callback)
        {
            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("START test: {0}", TestContext.TestName);
            //if(SnTrace.SnTracers.Count == 1)
            //    SnTrace.SnTracers.Add(new SnDebugViewTracer());

            DataStore.Enabled = true;

            var brandNew = EnsureDatabase();
            if (brandNew)
            {
                var builder = CreateRepositoryBuilderForStorageTest();
            }

            await callback();
        }

        private bool EnsureDatabase()
        {
            var brandNew = _instance == null;
            if (brandNew)
            {
                using (var op = SnTrace.Test.StartOperation("Install database."))
                {
                    _connectionStringBackup = Configuration.ConnectionStrings.ConnectionString;
                    _securityConnectionStringBackup = Configuration.ConnectionStrings.SecurityDatabaseConnectionString;
                    _connectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForStorageTests;
                    _databaseName = GetDatabaseName(_connectionString);

                    Configuration.ConnectionStrings.ConnectionString = _connectionString;
                    Configuration.ConnectionStrings.SecurityDatabaseConnectionString = _connectionString;

                    PrepareDatabase();

                    new SnMaintenance().Shutdown();

                    _instance = this;

                    op.Successful = true;
                }
            }
            return brandNew;
        }
        private void PrepareRepository()
        {
            SecurityHandler.SecurityInstaller.InstallDefaultSecurityStructure();
            //ContentTypeInstaller.InstallContentType(LoadCtds());
            //SaveInitialIndexDocuments();
            //RebuildIndex();
        }

        protected abstract DataProvider2 DataProvider { get; }
        protected abstract ISharedLockDataProviderExtension SharedLockDataProvider { get; }
        protected abstract IAccessTokenDataProviderExtension AccessTokenDataProvider { get; }
        protected abstract IBlobStorageMetaDataProvider BlobStorageMetaDataProvider { get; }
        protected abstract ITestingDataProviderExtension TestingDataProvider { get; }

        protected RepositoryBuilder CreateRepositoryBuilderForStorageTest()
        {
            var dp2 = DataProvider;
            Providers.Instance.DataProvider2 = dp2;

            using (var op = SnTrace.Test.StartOperation("Install initial data."))
            {
                var backup = DataStore.Enabled;
                DataStore.Enabled = true;
                DataStore.InstallInitialDataAsync(GetInitialData()).Wait();
                DataStore.Enabled = backup;
                op.Successful = true;
            }
            var inMemoryIndex = GetInitialIndex();

            dp2.SetExtension(typeof(ISharedLockDataProviderExtension), SharedLockDataProvider);
            dp2.SetExtension(typeof(IAccessTokenDataProviderExtension), AccessTokenDataProvider);

            Providers.Instance.BlobMetaDataProvider2 = BlobStorageMetaDataProvider;

            var dataProvider = new InMemoryDataProvider();
            var securityDataProvider = GetSecurityDataProvider(dataProvider);

            return new RepositoryBuilder()
                .UseAccessProvider(new DesktopAccessProvider())
                .UseDataProvider(dataProvider)
                .UseSharedLockDataProviderExtension(new InMemorySharedLockDataProvider())
                .UseBlobMetaDataProvider(DataStore.Enabled
                    ? BlobStorageMetaDataProvider
                    : new InMemoryBlobStorageMetaDataProvider(dataProvider))
                .UseBlobProviderSelector(new InMemoryBlobProviderSelector())
                .UseAccessTokenDataProviderExtension(AccessTokenDataProvider)
                .UseSearchEngine(new InMemorySearchEngine(inMemoryIndex))
                .UseSecurityDataProvider(securityDataProvider)
                .UseTestingDataProviderExtension(DataStore.Enabled
                    ? TestingDataProvider
                    : new InMemoryTestingDataProvider()) //DB:ok
                .UseElevatedModificationVisibilityRuleProvider(new ElevatedModificationVisibilityRule())
                .StartWorkflowEngine(false)
                .DisableNodeObservers()
                .EnableNodeObservers(typeof(SettingsCache))
                .UseTraceCategories("Test", "Event", "Custom") as RepositoryBuilder;
        }
        protected static ISecurityDataProvider GetSecurityDataProvider(InMemoryDataProvider repo)
        {
            return new MemoryDataProvider(new DatabaseStorage
            {
                Aces = new List<StoredAce>
                {
                    new StoredAce {EntityId = 2, IdentityId = 1, LocalOnly = false, AllowBits = 0x0EF, DenyBits = 0x000}
                },
                Entities = repo.GetSecurityEntities().ToDictionary(e => e.Id, e => e),
                Memberships = new List<Membership>
                {
                    new Membership
                    {
                        GroupId = Identifiers.AdministratorsGroupId,
                        MemberId = Identifiers.AdministratorUserId,
                        IsUser = true
                    }
                },
                Messages = new List<Tuple<int, DateTime, byte[]>>()
            });
        }


        [TestCleanup]
        public void CleanupTest()
        {
            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("END test: {0}", TestContext.TestName);
            SnTrace.Flush();
        }

        protected static void TearDown()
        {
            _instance?.TearDownPrivate();
        }
        private void TearDownPrivate()
        {
            if (_connectionStringBackup != null)
                Configuration.ConnectionStrings.ConnectionString = _connectionStringBackup;
            if (_securityConnectionStringBackup != null)
                Configuration.ConnectionStrings.SecurityDatabaseConnectionString = _securityConnectionStringBackup;
        }

        protected void PrepareDatabase()
        {
            var scriptRootPath = System.IO.Path.GetFullPath(System.IO.Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\..\..\sensenet\src\Storage\Data\MsSqlClient\Scripts"));

            var dbid = ExecuteSqlScalarNative<int?>($"SELECT database_id FROM sys.databases WHERE Name = '{_databaseName}'", "master");
            if (dbid == null)
            {
                // create database
                var sqlPath = System.IO.Path.Combine(scriptRootPath, "Create_SenseNet_Database_Templated.sql");
                string sql;
                using (var reader = new System.IO.StreamReader(sqlPath))
                    sql = reader.ReadToEnd();
                sql = sql.Replace("{_databaseName}", _databaseName);
                ExecuteSqlCommandNative(sql, "master");
            }
            // prepare database
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"MsSqlInstall_Security.sql"), _databaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"MsSqlInstall_01_Schema.sql"), _databaseName);
            //ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"MsSqlInstall_02_Procs.sql"), _databaseName);
            //ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"MsSqlInstall_03_Data_Phase1.sql"), _databaseName);
            //ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"MsSqlInstall_04_Data_Phase2.sql"), _databaseName);
        }
        private void ExecuteSqlScriptNative(string scriptPath, string databaseName)
        {
            string sql;
            using (var reader = new System.IO.StreamReader(scriptPath))
                sql = reader.ReadToEnd();
            ExecuteSqlCommandNative(sql, databaseName);
        }

        private void ExecuteSqlCommandNative(string sql, string databaseName)
        {
            var scripts = sql.Split(new[] { "\r\nGO" }, StringSplitOptions.RemoveEmptyEntries);

            using (var cn = new SqlConnection(_connectionString))
            {
                cn.Open();
                foreach (var script in scripts)
                {
                    using (var proc = new SqlCommand(script, cn))
                    {
                        proc.CommandType = CommandType.Text;
                        proc.ExecuteNonQuery();
                    }
                }
            }
        }

        private T ExecuteSqlScalarNative<T>(string sql, string databaseName)
        {
            using (var cn = new SqlConnection(_connectionString))
            {
                cn.Open();
                using (var proc = new SqlCommand(sql, cn))
                {
                    proc.CommandType = CommandType.Text;
                    return (T)proc.ExecuteScalar();
                }
            }
        }

        private string GetDatabaseName(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            return builder.InitialCatalog;
        }




        private static InitialData _initialData;
        protected static InitialData GetInitialData()
        {
            if (_initialData == null)
            {
                InitialData initialData;
                using (var ptr = new StringReader(InitialTestData.PropertyTypes))
                using (var ntr = new StringReader(InitialTestData.NodeTypes))
                using (var nr = new StringReader(InitialTestData.Nodes))
                using (var vr = new StringReader(InitialTestData.Versions))
                using (var dr = new StringReader(InitialTestData.DynamicData))
                    initialData = InitialData.Load(ptr, ntr, nr, vr, dr);
                initialData.ContentTypeDefinitions = InitialTestData.ContentTypeDefinitions;
                initialData.Blobs = InitialTestData.GeneralBlobs;
                _initialData = initialData;
            }
            return _initialData;
        }

        private static InMemoryIndex _initialIndex;
        protected static InMemoryIndex GetInitialIndex()
        {
            if (_initialIndex == null)
            {
                var index = new InMemoryIndex();
                index.Load(new StringReader(InitialTestIndex.Index));
                _initialIndex = index;
            }
            return _initialIndex.Clone();
        }
        #endregion

        protected static ContentQuery CreateSafeContentQuery(string qtext, QuerySettings settings = null)
        {
            var cquery = ContentQuery.CreateQuery(qtext, settings ?? QuerySettings.AdminSettings);
            var cqueryAcc = new PrivateObject(cquery);
            cqueryAcc.SetFieldOrProperty("IsSafe", true);
            return cquery;
        }

        protected void AssertSequenceEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            var e = string.Join(", ", expected.Select(x => x.ToString()));
            var a = string.Join(", ", actual.Select(x => x.ToString()));
            Assert.AreEqual(e, a);
        }
    }
}
