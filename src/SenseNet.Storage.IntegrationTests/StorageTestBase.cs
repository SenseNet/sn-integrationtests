using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using STT = System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Security;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.ContentRepository.Storage.DataModel;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.Diagnostics;
using SenseNet.IntegrationTests.Common.Implementations;
using SenseNet.Security;
using SenseNet.Security.Data;
using SenseNet.Storage.Data.MsSqlClient;
using SenseNet.Tests;
using SenseNet.Tests.Implementations;

namespace SenseNet.Storage.IntegrationTests
{
    public class StorageTestBase
    {
        #region Infrastructure

        public TestContext TestContext { get; set; }

        private static StorageTestBase _instance;

        private string _databaseName;
        private string _connectionString;
        private string _connectionStringBackup;
        private string _securityConnectionStringBackup;

        public STT.Task StorageTest(Func<STT.Task> callback)
        {
            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("START test: {0}", TestContext.TestName);

            DataStore.Enabled = true;

            EnsureDatabase();

            DistributedApplication.Cache.Reset();
            ContentTypeManager.Reset();
            Providers.Instance.NodeTypeManeger = null;

            //var portalContextAcc = new PrivateType(typeof(PortalContext));
            //portalContextAcc.SetStaticField("_sites", new Dictionary<string, Site>());

            var builder = CreateRepositoryBuilderForStorageTest();

            Indexing.IsOuterSearchEngineEnabled = true;

            DistributedApplication.Cache.Reset();
            ContentTypeManager.Reset();

            using (Repository.Start(builder))
            {
                //PrepareRepository();

                //if (useCurrentUser)
                //    callback();
                //else
                    using (new SystemAccount())
                        return callback();
            }


        }

        private void EnsureDatabase()
        {
            if (_instance == null)
            {
                using (var op = SnTrace.Test.StartOperation("Initialize database"))
                {
                    _connectionStringBackup = ConnectionStrings.ConnectionString;
                    _securityConnectionStringBackup = ConnectionStrings.SecurityDatabaseConnectionString;
                    _connectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForStorageTests;
                    _databaseName = GetDatabaseName(_connectionString);

                    ConnectionStrings.ConnectionString = _connectionString;
                    ConnectionStrings.SecurityDatabaseConnectionString = _connectionString;

                    PrepareDatabase();

                    new SnMaintenance().Shutdown();

                    _instance = this;

                    op.Successful = true;
                }
            }
        }


        protected static RepositoryBuilder CreateRepositoryBuilderForStorageTest()
        {
            var dp2 = new MsSqlDataProvider();
            Providers.Instance.DataProvider2 = dp2;
var backup = DataStore.Enabled;
DataStore.Enabled = true;
DataStore.InstallInitialDataAsync(GetInitialData()).Wait();
DataStore.Enabled = backup;

            dp2.SetExtension(typeof(ISharedLockDataProviderExtension), new SqlSharedLockDataProvider());
            dp2.SetExtension(typeof(IAccessTokenDataProviderExtension), new SqlAccessTokenDataProvider());

            Providers.Instance.BlobMetaDataProvider2 = new MsSqlBlobMetaDataProvider();

            var dataProvider = new InMemoryDataProvider();
            var securityDataProvider = GetSecurityDataProvider(dataProvider);
            SecurityHandler.SecurityInstaller.InstallDefaultSecurityStructure();

            return new RepositoryBuilder()
                .UseAccessProvider(new DesktopAccessProvider())
                .UseDataProvider(dataProvider)
                .UseTestingDataProviderExtension(new SqlTestingDataProvider())
                .UseSharedLockDataProviderExtension(new InMemorySharedLockDataProvider())
                .UseBlobMetaDataProvider(DataStore.Enabled
                    ? (IBlobStorageMetaDataProvider)new MsSqlBlobMetaDataProvider()
                    : new InMemoryBlobStorageMetaDataProvider(dataProvider))
                .UseBlobProviderSelector(new InMemoryBlobProviderSelector())
                .UseAccessTokenDataProviderExtension(new SqlAccessTokenDataProvider())
                .UseSearchEngine(new InMemorySearchEngine(new InMemoryIndex()))
                .UseSecurityDataProvider(securityDataProvider)
                .UseTestingDataProviderExtension(DataStore.Enabled
                    ? (ITestingDataProviderExtension)new SqlTestingDataProvider()
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
                ConnectionStrings.ConnectionString = _connectionStringBackup;
            if (_securityConnectionStringBackup != null)
                ConnectionStrings.SecurityDatabaseConnectionString = _securityConnectionStringBackup;
        }

        protected void PrepareDatabase()
        {
            var scriptRootPath = System.IO.Path.GetFullPath(System.IO.Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\..\..\sensenet\src\Storage\Data\SqlClient\Scripts"));

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
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_Security.sql"), _databaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_01_Schema.sql"), _databaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_02_Procs.sql"), _databaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_03_Data_Phase1.sql"), _databaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_04_Data_Phase2.sql"), _databaseName);
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

        private DbDataReader ExecuteSqlReader(string sql)
        {
            var proc = DataProvider.Instance.CreateDataProcedure(sql);
            proc.CommandType = CommandType.Text;
            return proc.ExecuteReader();
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
                using (var ptr = new StringReader(InitialTestData.PropertyTypes))
                using (var ntr = new StringReader(InitialTestData.NodeTypes))
                using (var nr = new StringReader(InitialTestData.Nodes))
                using (var vr = new StringReader(InitialTestData.Versions))
                using (var dr = new StringReader(InitialTestData.DynamicData))
                    _initialData = InitialData.Load(ptr, ntr, nr, vr, dr);
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
    }
}
