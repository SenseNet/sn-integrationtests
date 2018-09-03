using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Security;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Schema;
using SenseNet.ContentRepository.Storage.Search;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.ContentRepository.Versioning;
using SenseNet.Preview.Packaging.Steps;
using SenseNet.Security.EFCSecurityStore;
using SenseNet.Tests;
using SenseNet.Tests.Implementations;

namespace SenseNet.Preview.IntegrationTests
{
    [TestClass]
    public class CleanupMsSqlTests : TestBase
    {
        protected string DatabaseName { get; set; } = "sn7tests";

        [TestMethod]
        public void Cleanup_SQL_MultiClean()
        {
            using (Repository.Start(CreateRepositoryBuilder()))
            {
                Content root = null;

                try
                {
                    root = CreateStructure();

                    // 3x5
                    AssertSystemFolders(root.Path, 15);
                    // 3x15
                    AssertPreviewImages(root.Path, 45);

                    // delete only empty folders but not images
                    var pc = new PreviewCleaner(cleanupMode:CleanupMode.EmptyFoldersOnly);
                    pc.Execute();

                    // 3x4
                    AssertSystemFolders(root.Path, 12);
                    // 3x15
                    AssertPreviewImages(root.Path, 45);

                    // delete images with a big (>3) index
                    pc = new PreviewCleaner(maxIndex:3);
                    pc.Execute();

                    // 3x4
                    AssertSystemFolders(root.Path, 12);
                    // 3x9
                    AssertPreviewImages(root.Path, 27);

                    // delete previews of old versions of a single document and keep only 2 images
                    var f1Path = root.Path + "/doc1.docx";
                    pc = new PreviewCleaner(f1Path, CleanupMode.KeepLastVersions, 2);
                    pc.Execute();

                    AssertSystemFolders(f1Path, 3);
                    AssertPreviewImages(f1Path, 4);

                    // delete previews of old versions of all documents
                    pc = new PreviewCleaner(cleanupMode:CleanupMode.KeepLastVersions);
                    pc.Execute();

                    AssertSystemFolders(root.Path, 9);
                    AssertPreviewImages(root.Path, 16);

                    // delete all previews
                    pc = new PreviewCleaner();
                    pc.Execute();

                    AssertSystemFolders(root.Path, 0);
                    AssertPreviewImages(root.Path, 0);
                }
                finally 
                {
                    if (root != null && Node.Exists(root.Path))
                        Content.DeletePhysical(root.Path);
                }
            }
        }
        
        [TestInitialize]
        public void Initialize()
        {
            var connectionString = GetConnectionString(DatabaseName);
            ConnectionStrings.ConnectionString = connectionString;

            PrepareDatabase();

            using (Repository.Start(CreateRepositoryBuilder()))
            {
                using (new SystemAccount())
                    PrepareRepository();

                // this is necessary for the admin user to have full access to the repo
                using (new SystemAccount())
                {
                    Group.Administrators.AddMember(User.Administrator);
                }
            }
        }

        private static Content CreateStructure()
        {
            /*
             root
	            doc1.docx
		            Previews
			            V1.0.A [empty folder]
			            V1.1.D [5 images]
			            V2.0.A [5 images]
			            V2.1.D [5 images]
	            doc2.docx
		            Previews
			            V1.0.A [empty folder]
			            V1.1.D [5 images]
			            V2.0.A [5 images]
			            V2.1.D [5 images]
	            doc3.docx
		            Previews
			            V1.0.A [empty folder]
			            V1.1.D [5 images]
			            V2.0.A [5 images]
			            V2.1.D [5 images]
             */

            var rootName = Guid.NewGuid().ToString();
            var rootPath = $"/Root/{rootName}";
            var root = RepositoryTools.CreateStructure(rootPath, "SystemFolder") ?? Content.Load(rootPath);

            CreateFileAndPreviews(root, "doc1.docx");
            CreateFileAndPreviews(root, "doc2.docx");
            CreateFileAndPreviews(root, "doc3.docx");

            void CreateFileAndPreviews(Content parent, string name)
            {
                var file = CreateFile(parent, name);
                var previews = Content.CreateNew("SystemFolder", file, "Previews");
                previews.Save();

                var version = Content.CreateNew("SystemFolder", previews.ContentHandler, "V1.0.A");
                version.Save();
                // Deliberately leave the first folder empty.
                // CreatePreviews(version);

                version = Content.CreateNew("SystemFolder", previews.ContentHandler, "V1.1.D");
                version.Save();
                CreatePreviews(version);
                version = Content.CreateNew("SystemFolder", previews.ContentHandler, "V2.0.A");
                version.Save();
                CreatePreviews(version);
                version = Content.CreateNew("SystemFolder", previews.ContentHandler, "V2.1.D");
                version.Save();
                CreatePreviews(version);
            }

            Node CreateFile(Content parent, string name)
            {
                var file = new File(parent.ContentHandler)
                {
                    VersioningMode = VersioningType.MajorAndMinor,
                    Name = name
                };
                file.Save();

                file.Publish();     // --> 1.0.A
                file.CheckOut();
                file.CheckIn();     // --> 1.1.D
                file.CheckOut();
                file.CheckIn();
                file.Publish();     // --> 2.0.A
                file.CheckOut();
                file.CheckIn();     // --> 2.1.D

                return file;
            }

            void CreatePreviews(Content versionFolder)
            {
                var image = Content.CreateNew("PreviewImage", versionFolder.ContentHandler, "preview1.png");
                image.Index = 1;
                image.Save();
                image = Content.CreateNew("PreviewImage", versionFolder.ContentHandler, "preview2.png");
                image.Index = 2;
                image.Save();
                image = Content.CreateNew("PreviewImage", versionFolder.ContentHandler, "preview3.png");
                image.Index = 3;
                image.Save();
                image = Content.CreateNew("PreviewImage", versionFolder.ContentHandler, "preview4.png");
                image.Index = 4;
                image.Save();
                image = Content.CreateNew("PreviewImage", versionFolder.ContentHandler, "preview5.png");
                image.Index = 5;
                image.Save();
            }

            return root;
        }

        private static void AssertSystemFolders(string path, int expectedCount)
        {
            Assert.AreEqual(expectedCount, Content.All.DisableAutofilters().Count(c => c.InTree(path) && c.Path != path && c.TypeIs("SystemFolder")));
            Assert.AreEqual(expectedCount, NodeQuery.QueryNodesByTypeAndPath(NodeType.GetByName("SystemFolder"), true, path, false).Count);
        }
        private static void AssertPreviewImages(string path, int expectedCount)
        {
            Assert.AreEqual(expectedCount, NodeQuery.QueryNodesByTypeAndPath(NodeType.GetByName("PreviewImage"), false, path, false).Count);
        }

        //TODO: Copied from BLOB tests. Generalize this.
        #region Prepare database

        protected void PrepareDatabase()
        {
            var scriptRootPath = System.IO.Path.GetFullPath(System.IO.Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\..\..\sensenet\src\Storage\Data\SqlClient\Scripts"));

            var dbid = ExecuteSqlScalarNative<int?>($"SELECT database_id FROM sys.databases WHERE Name = '{DatabaseName}'", "master");
            if (dbid == null)
            {
                // create database
                var sqlPath = System.IO.Path.Combine(scriptRootPath, "Create_SenseNet_Database_Templated.sql");
                string sql;
                using (var reader = new System.IO.StreamReader(sqlPath))
                    sql = reader.ReadToEnd();
                sql = sql.Replace("{DatabaseName}", DatabaseName);
                ExecuteSqlCommandNative(sql, "master");
            }
            // prepare database
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_Security.sql"), DatabaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_01_Schema.sql"), DatabaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_02_Procs.sql"), DatabaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_03_Data_Phase1.sql"), DatabaseName);
            ExecuteSqlScriptNative(System.IO.Path.Combine(scriptRootPath, @"Install_04_Data_Phase2.sql"), DatabaseName);

            //DataProvider.InitializeForTests();
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
            var cnstr = GetConnectionString(databaseName);
            var scripts = sql.Split(new[] { "\r\nGO" }, StringSplitOptions.RemoveEmptyEntries);

            using (var cn = new SqlConnection(cnstr))
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
            var cnstr = GetConnectionString(databaseName);
            using (var cn = new SqlConnection(cnstr))
            {
                cn.Open();
                using (var proc = new SqlCommand(sql, cn))
                {
                    proc.CommandType = CommandType.Text;
                    return (T)proc.ExecuteScalar();
                }
            }
        }
        private string GetConnectionString(string databaseName = null)
        {
            //UNDONE: move this to the common class
            return $"Initial Catalog={databaseName ?? DatabaseName};Data Source=.\\SQL2016;Integrated Security=SSPI;Persist Security Info=False";
        }

        #endregion

        #region Prepare repository

        private void PrepareRepository()
        {
            SecurityHandler.SecurityInstaller.InstallDefaultSecurityStructure();
            ContentTypeInstaller.InstallContentType(LoadCtds());
            SaveInitialIndexDocuments();
            RebuildIndex();
        }
        protected RepositoryBuilder CreateRepositoryBuilder()
        {
            // If we do not set the data provider here, it will fall back to the default SQL provider.
            // ...but we do have to set the security sb provider because the default implementation
            // looks for a connection string in the config file, which we do not want to use here.
            var sdp = new EFCSecurityDataProvider(
                Configuration.Security.SecurityDatabaseCommandTimeoutInSeconds,
                GetConnectionString());

            return new RepositoryBuilder()
                .UseAccessProvider(new DesktopAccessProvider())
                .UseSecurityDataProvider(sdp)
                .UseSearchEngine(new InMemorySearchEngine())
                .UseElevatedModificationVisibilityRuleProvider(new ElevatedModificationVisibilityRule())
                .StartWorkflowEngine(false)
                .DisableNodeObservers()
                //.EnableNodeObservers(typeof(SettingsCache))
                .UseTraceCategories("Test", "Event", "Custom");
        }
        private static string[] LoadCtds()
        {
            var ctdRootPath = System.IO.Path.GetFullPath(System.IO.Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\..\..\sensenet\src\nuget\snadmin\install-services\import\System\Schema\ContentTypes"));
            var xmlSources = System.IO.Directory.GetFiles(ctdRootPath, "*.xml")
                .Select(p =>
                {
                    using (var r = new System.IO.StreamReader(p))
                        return r.ReadToEnd();
                })
                .ToArray();
            return xmlSources;
        }

        #endregion
    }
}
