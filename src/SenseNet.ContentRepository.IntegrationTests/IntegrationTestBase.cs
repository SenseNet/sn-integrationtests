using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Search;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.Diagnostics;

namespace SenseNet.ContentRepository.IntegrationTests
{
    /// <summary>
    /// Base IntegrationTests class for the various features of the ContentRepository and Storage
    /// This class installs SQL database once but does not start a Repository.
    /// The right cleanup needs a static cleanup method in the overwritten instance:
    /// [ClassCleanup] public static void CleanupClass() { TearDown(); }
    /// </summary>
    [TestClass]
    public abstract class IntegrationTestBase
    {
        #region Infrastructure

        public TestContext TestContext { get; set; }

        private static IntegrationTestBase _instance;

        private string _databaseName;
        private string _connectionString;
        private string _connectionStringBackup;
        private string _securityConnectionStringBackup;

        [TestInitialize]
        public void Initialize()
        {
            // Test class initialization problem: the test framework
            // uses brand new instance for each test method.
            // The right cleanup requires a static ClassCleanup method
            // That needs to be something like this:
            //    [ClassCleanup]
            //    public static void CleanupClass()
            //    {
            //        TearDown();
            //    }


            SnTrace.Test.Enabled = true;
            SnTrace.Test.Write("START test: {0}", TestContext.TestName);

            if (_instance == null)
            {
                using (var op = SnTrace.Test.StartOperation("Initialize database"))
                {
                    _connectionStringBackup = ConnectionStrings.ConnectionString;
                    _securityConnectionStringBackup = ConnectionStrings.SecurityDatabaseConnectionString;
                    _connectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForContentRepositoryTests;
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
            var proc = DataProvider.CreateDataProcedure(sql);
            proc.CommandType = CommandType.Text;
            return proc.ExecuteReader();
        }

        private string GetDatabaseName(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            return builder.InitialCatalog;
        }

        #endregion


        [TestMethod]
        public void TestMethod1()
        {
        }
    }
}
