using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.BackgroundOperations;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.InMemory;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Security;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.ContentRepository.Storage.DataModel;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.Diagnostics;
using SenseNet.Extensions.DependencyInjection;
using SenseNet.IntegrationTests.Common.Implementations;
using SenseNet.Search;
using SenseNet.Security;
using SenseNet.Security.Data;
using SenseNet.Tests;
using SenseNet.Tests.Implementations;

namespace SenseNet.IntegrationTests.Common
{
    /// <summary>
    /// Base IntegrationTests class for the various features of the ContentRepository and Storage
    /// This class installs SQL database once but does not start a Repository.
    /// The right cleanup needs a static cleanup method in the overwritten instance:
    /// [ClassCleanup] public static void CleanupClass() { TearDown(); }
    /// </summary>
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

            if (isolated)
                _instance = null;

            var brandNew = EnsureDatabase();

            Cache.Reset();
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

                Cache.Reset();
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

                    //UNDONE: new SnMaintenance().Shutdown();
                    //new SnMaintenance().Shutdown();

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

        protected abstract DataProvider DataProvider { get; }
        protected abstract ISharedLockDataProviderExtension SharedLockDataProvider { get; }
        protected abstract IAccessTokenDataProviderExtension AccessTokenDataProvider { get; }
        protected abstract IBlobStorageMetaDataProvider BlobStorageMetaDataProvider { get; }
        protected abstract ITestingDataProviderExtension TestingDataProvider { get; }

        protected RepositoryBuilder CreateRepositoryBuilderForStorageTest()
        {
            var dataProvider2 = DataProvider;
            Providers.Instance.DataProvider = dataProvider2;

            using (var op = SnTrace.Test.StartOperation("Install initial data."))
            {
                DataStore.InstallInitialDataAsync(GetInitialData(), CancellationToken.None).GetAwaiter().GetResult();
                op.Successful = true;
            }
            var inMemoryIndex = GetInitialIndex();

            dataProvider2.SetExtension(typeof(ISharedLockDataProviderExtension), SharedLockDataProvider);
            dataProvider2.SetExtension(typeof(IAccessTokenDataProviderExtension), AccessTokenDataProvider);

            var securityDataProvider = GetSecurityDataProvider(dataProvider2);

            return new RepositoryBuilder()
                .UseAccessProvider(new DesktopAccessProvider())
                .UseDataProvider(dataProvider2)
                .UseBlobMetaDataProvider(BlobStorageMetaDataProvider)
                .UseBlobProviderSelector(new InMemoryBlobProviderSelector())
                .UseAccessTokenDataProviderExtension(AccessTokenDataProvider)
                .UseSearchEngine(new InMemorySearchEngine(inMemoryIndex))
                .UseSecurityDataProvider(securityDataProvider)
                .UseTestingDataProviderExtension(TestingDataProvider)
                .UseElevatedModificationVisibilityRuleProvider(new ElevatedModificationVisibilityRule())
                .StartWorkflowEngine(false)
                .DisableNodeObservers()
                .EnableNodeObservers(typeof(SettingsCache))
                .UseTraceCategories("Test", "Event", "Custom") as RepositoryBuilder;
        }

        protected static ISecurityDataProvider GetSecurityDataProvider(DataProvider repo)
        {
            return new MemoryDataProvider(new DatabaseStorage
            {
                Aces = new List<StoredAce>
                {
                    new StoredAce {EntityId = 2, IdentityId = 1, LocalOnly = false, AllowBits = 0x0EF, DenyBits = 0x000}
                },
                Entities = repo.LoadEntityTreeAsync(CancellationToken.None).GetAwaiter().GetResult()
                    .ToDictionary(x => x.Id, x => new StoredSecurityEntity
                {
                    Id = x.Id,
                    OwnerId = x.OwnerId,
                    ParentId = x.ParentId,
                    IsInherited = true,
                    HasExplicitEntry = x.Id == 2
                }),
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
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"MsSqlInstall_Schema.sql"), _databaseName);
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
                _initialData = InitialData.Load(InMemoryTestData.Instance, null);
            return _initialData;
        }

        private static InMemoryIndex _initialIndex;
        protected static InMemoryIndex GetInitialIndex()
        {
            if (_initialIndex == null)
            {
                var index = new InMemoryIndex();
                index.Load(new StringReader(InMemoryTestIndex.Index));
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

        protected static readonly string CarContentTypeWithoutColor = @"<?xml version='1.0' encoding='utf-8'?>
<ContentType name='Car' parentType='ListItem' handler='SenseNet.ContentRepository.GenericContent' xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentTypeDefinition'>
  <DisplayName>Car,DisplayName</DisplayName>
  <Description>Car,Description</Description>
  <Icon>Car</Icon>
  <AllowIncrementalNaming>true</AllowIncrementalNaming>
  <Fields>
    <Field name='Name' type='ShortText'/>
    <Field name='Make' type='ShortText'/>
    <Field name='Model' type='ShortText'/>
    <Field name='Style' type='Choice'>
      <Configuration>
        <AllowMultiple>false</AllowMultiple>
        <AllowExtraValue>true</AllowExtraValue>
        <Options>
          <Option value='Sedan' selected='true'>Sedan</Option>
          <Option value='Coupe'>Coupe</Option>
          <Option value='Cabrio'>Cabrio</Option>
          <Option value='Roadster'>Roadster</Option>
          <Option value='SUV'>SUV</Option>
          <Option value='Van'>Van</Option>
        </Options>
      </Configuration>
    </Field>
    <Field name='StartingDate' type='DateTime'/>
    <!--
    <Field name='Color' type='Color'>
      <Configuration>
        <DefaultValue>#ff0000</DefaultValue>
        <Palette>#ff0000;#f0d0c9;#e2a293;#d4735e;#65281a</Palette>
      </Configuration>
    </Field>
    -->
    <Field name='EngineSize' type='ShortText'/>
    <Field name='Power' type='ShortText'/>
    <Field name='Price' type='Number'/>
    <Field name='Description' type='LongText'/>
  </Fields>
</ContentType>
";
        protected static void InstallCarContentType()
        {
            ContentTypeInstaller.InstallContentType(CarContentTypeWithoutColor);
        }
    }
}
