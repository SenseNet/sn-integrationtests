using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.DataModel;
using SenseNet.Diagnostics;
using SenseNet.Security;
using SenseNet.Security.Data;
using SenseNet.Tests;
using SenseNet.Tests.Implementations;
using Task = System.Threading.Tasks.Task;

namespace SenseNet.Storage.IntegrationTests
{
    [TestClass]
    public class MsSqlDataInstallerTests
    {
        #region Infrastructure

        public TestContext TestContext { get; set; }

        private string _databaseName;
        private string _connectionString;
        private string _connectionStringBackup;
        private string _securityConnectionStringBackup;

        public async Task MsSqlDataInstallerTest(Func<Task> callback)
        {
            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("START test: {0}", TestContext.TestName);
            //if(SnTrace.SnTracers.Count == 1)
            //    SnTrace.SnTracers.Add(new SnDebugViewTracer());

            EnsureDatabase();

            var builder = CreateRepositoryBuilderForStorageTest();

            PrepareRepository();

            await callback();
        }

        public async System.Threading.Tasks.Task NoRepositoryIntegrtionTest(Func<System.Threading.Tasks.Task> callback)
        {
            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("START test: {0}", TestContext.TestName);
            //if(SnTrace.SnTracers.Count == 1)
            //    SnTrace.SnTracers.Add(new SnDebugViewTracer());

            var builder = CreateRepositoryBuilderForStorageTest();

            await callback();
        }

        private void EnsureDatabase()
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

                op.Successful = true;
            }
        }

        private void PrepareRepository()
        {
            //SecurityHandler.SecurityInstaller.InstallDefaultSecurityStructure();
            //ContentTypeInstaller.InstallContentType(LoadCtds());
            //SaveInitialIndexDocuments();
            //RebuildIndex();
        }

        protected RepositoryBuilder CreateRepositoryBuilderForStorageTest()
        {
            var dataProvider = new MsSqlDataProvider();

            //// Requires installed Administrator
            //var securityDataProvider = GetSecurityDataProvider(dataProvider);

            var builder = new RepositoryBuilder()
                    //.UseAccessProvider(new DesktopAccessProvider())
                    .UseDataProvider(dataProvider)
                    .UseBlobMetaDataProvider(new MsSqlBlobMetaDataProvider())
                    .UseBlobProviderSelector(new InMemoryBlobProviderSelector())
                    //.UseSharedLockDataProviderExtension(new MsSqlSharedLockDataProvider())
                    //.UseAccessTokenDataProviderExtension(new MsSqlAccessTokenDataProvider())
                    //.UseSecurityDataProvider(securityDataProvider)
                    //.UseTestingDataProviderExtension(new MsSqlTestingDataProvider())
                    //.UseElevatedModificationVisibilityRuleProvider(new ElevatedModificationVisibilityRule())
                    .StartWorkflowEngine(false)
                    .DisableNodeObservers()
                //.EnableNodeObservers(typeof(SettingsCache))
                //.UseTraceCategories("Test", "Event", "Custom") as RepositoryBuilder
                ;
            //using (var op = SnTrace.Test.StartOperation("Install initial data."))
            //{
            //    DataStore.InstallInitialDataAsync(GetInitialData()).Wait();
            //    op.Successful = true;
            //}
            //var inMemoryIndex = GetInitialIndex();

            //builder.UseSearchEngine(new InMemorySearchEngine(inMemoryIndex));

            return (RepositoryBuilder)builder;
        }

        protected static ISecurityDataProvider GetSecurityDataProvider(DataProvider repo)
        {
            return new MemoryDataProvider(new DatabaseStorage
            {
                Aces = new List<StoredAce>
                {
                    new StoredAce {EntityId = 2, IdentityId = 1, LocalOnly = false, AllowBits = 0x0EF, DenyBits = 0x000}
                },
                Entities = repo.LoadEntityTreeAsync().Result.ToDictionary(x => x.Id, x => new StoredSecurityEntity
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
            if (_connectionStringBackup != null)
                Configuration.ConnectionStrings.ConnectionString = _connectionStringBackup;
            if (_securityConnectionStringBackup != null)
                Configuration.ConnectionStrings.SecurityDatabaseConnectionString = _securityConnectionStringBackup;

            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("END test: {0}", TestContext.TestName);
            SnTrace.Flush();
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




        //private static InitialData GetInitialTestData()
        //{
        //    InitialData initialData;

        //    using (var ptr = new StringReader(InitialTestData.PropertyTypes))
        //    using (var ntr = new StringReader(InitialTestData.NodeTypes))
        //    using (var nr = new StringReader(InitialTestData.Nodes))
        //    using (var vr = new StringReader(InitialTestData.Versions))
        //    using (var dr = new StringReader(InitialTestData.DynamicData))
        //        initialData = InitialData.Load(ptr, ntr, nr, vr, dr);

        //    initialData.ContentTypeDefinitions = InitialTestData.ContentTypeDefinitions;
        //    initialData.Blobs = InitialTestData.GeneralBlobs;

        //    return initialData;
        //}

        private static InitialData GetInitialData(IRepositoryDataFile dataFile)
        {
            InitialData initialData;

            using (var ptr = new StringReader(dataFile.PropertyTypes))
            using (var ntr = new StringReader(dataFile.NodeTypes))
            using (var nr = new StringReader(dataFile.Nodes))
            using (var vr = new StringReader(dataFile.Versions))
            using (var dr = new StringReader(dataFile.DynamicData))
                initialData = InitialData.Load(ptr, ntr, nr, vr, dr);

            initialData.ContentTypeDefinitions = dataFile.ContentTypeDefinitions;
            initialData.Blobs = dataFile.Blobs;

            return initialData;
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

        [TestMethod]
        public async Task MsSqlDataInstaller_TestData()
        {
            var data = GetInitialData(new InitialTestData());
            await InstallInitialDataTest(data);
        }
        [TestMethod]
        public async Task MsSqlDataInstaller_SensenetServices()
        {
            var data = GetInitialData(new SenseNetServicesInitialData());
            await InstallInitialDataTest(data);
        }

        public async Task InstallInitialDataTest(InitialData data)
        {
            await MsSqlDataInstallerTest(async () =>
            {
                // PRECONCEPTION
                var counts = await GetTablesAndCountsAsync();
                Assert.AreEqual(0, counts["Nodes"]);
                Assert.AreEqual(0, counts["Versions"]);
                Assert.AreEqual(0, counts["PropertyTypes"]);
                Assert.AreEqual(0, counts["NodeTypes"]);

                // ACTION
                await DataStore.InstallInitialDataAsync(data);

                // ASSERT
                counts = await GetTablesAndCountsAsync();
                Assert.AreEqual(data.Nodes.Count(), counts["Nodes"]);
                Assert.AreEqual(data.Versions.Count(), counts["Versions"]);
                Assert.AreEqual(data.Schema.PropertyTypes.Count, counts["PropertyTypes"]);
                Assert.AreEqual(data.Schema.NodeTypes.Count, counts["NodeTypes"]);

                var dynProps = data.DynamicProperties.ToArray();
                Assert.AreEqual(dynProps.SelectMany(x => x.LongTextProperties).Count(), counts["LongTextProperties"]);
                Assert.AreEqual(dynProps.SelectMany(x => x.ReferenceProperties).Count(), counts["ReferenceProperties"]);
                Assert.AreEqual(dynProps.SelectMany(x => x.BinaryProperties).Count(), counts["BinaryProperties"]);
                Assert.AreEqual(counts["BinaryProperties"], counts["Files"]);
            });
        }

        private async Task<Dictionary<string, long>> GetTablesAndCountsAsync()
        {
            var sql = @"SELECT TableName = t.name, RowCounts = p.rows
FROM sys.tables t
    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
WHERE t.is_ms_shipped = 0 -- Means: user defines
GROUP BY t.NAME, p.Rows
ORDER BY t.Name";

            using (var ctx = new MsSqlDataContext())
            {
                return await ctx.ExecuteReaderAsync(sql, async (reader, cancel) =>
                {
                    var result = new Dictionary<string, long>();
                    while (await reader.ReadAsync(cancel))
                        result.Add(reader.GetString(0), reader.GetInt64(1));
                    return result;
                });
            }
        }
    }
}
