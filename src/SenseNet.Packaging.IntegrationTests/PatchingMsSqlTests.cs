using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.Extensions.DependencyInjection;
using SenseNet.Packaging.IntegrationTests.Implementations;
using SenseNet.Tests;

namespace SenseNet.Packaging.IntegrationTests
{
    [TestClass]
    public class PatchingMsSqlTests : TestBase
    {
        private static StringBuilder _log;
        private static MsSqlDataProvider Db => (MsSqlDataProvider)Providers.Instance.DataProvider;

        [ClassInitialize]
        public static void InitializeDatabase(TestContext context)
        {
            //DropPackagesTable();
            //InstallPackagesTable();
        }
        [TestInitialize]
        public void InitializePackagingTest()
        {
            // preparing logger
            _log = new StringBuilder();
            var loggers = new[] { new PackagingTestLogger(_log) };
            var loggerAcc = new PrivateType(typeof(Logger));
            loggerAcc.SetStaticField("_loggers", loggers);

            // set default implementation directly
            var sqlDb = new MsSqlDataProvider();
            Providers.Instance.DataProvider = sqlDb;

            // build database
            var builder = new RepositoryBuilder();
            builder.UsePackagingDataProviderExtension(new MsSqlPackagingDataProvider());

            // preparing database
            ConnectionStrings.ConnectionString = SenseNet.IntegrationTests.Common.ConnectionStrings.ForPackagingTests;

            using (var ctx = new MsSqlDataContext(CancellationToken.None))
            {
                DropPackagesTable(ctx);
                InstallPackagesTable(ctx);
            }

            RepositoryVersionInfo.Reset();
        }

        [TestMethod]
        public void PatchingSystem_InstalledComponents()
        {
            var installer1 = new ComponentInstaller
            {
                ComponentId = "C1",
                Version = new Version(1, 0),
                Description = "C1 description",
                ReleaseDate = new DateTime(2020, 07, 30),
                Dependencies = null
            };
            var installer2 = new ComponentInstaller
            {
                ComponentId = "C2",
                Version = new Version(2, 0),
                Description = "C2 description",
                ReleaseDate = new DateTime(2020, 07, 31),
                Dependencies = new[]
                {
                    Dep("C1", "1.0 <= v <= 1.0"),
                }
            };
            PackageManager.SavePackage(Manifest.Create(installer1), null, true, null);
            PackageManager.SavePackage(Manifest.Create(installer2), null, true, null);

            var verInfo = RepositoryVersionInfo.Create(CancellationToken.None);

            var components = verInfo.Components.ToArray();
            Assert.AreEqual(2, components.Length);

            Assert.AreEqual("C1", components[0].ComponentId);
            Assert.AreEqual("C2", components[1].ComponentId);

            Assert.AreEqual("1.0", components[0].Version.ToString());
            Assert.AreEqual("2.0", components[1].Version.ToString());

            Assert.AreEqual("C1 description", components[0].Description);
            Assert.AreEqual("C2 description", components[1].Description);

            Assert.AreEqual(0, components[0].Dependencies.Length);
            Assert.AreEqual(1, components[1].Dependencies.Length);
            Assert.AreEqual("C1: 1.0 <= v <= 1.0", components[1].Dependencies[0].ToString());
        }
        [TestMethod]
        public void PatchingSystem_InstalledComponents_Descriptions()
        {
            var installer = new ComponentInstaller
            {
                ComponentId = "C1",
                Version = new Version(1, 0),
                Description = "C1 component",
                ReleaseDate = new DateTime(2020, 07, 30),
                Dependencies = null
            };
            var patch = new SnPatch
            {
                ComponentId = "C1",
                Version = new Version(2, 0),
                Description = "C1 patch",
                ReleaseDate = new DateTime(2020, 07, 31),
                Boundary = new VersionBoundary
                {
                    MinVersion = new Version(1, 0)
                },
                Dependencies = new[]
                {
                    Dep("C2", "1.0 <= v <= 1.0"),
                }
            };

            PackageManager.SavePackage(Manifest.Create(installer), null, true, null);
            PackageManager.SavePackage(Manifest.Create(patch), null, true, null);

            var verInfo = RepositoryVersionInfo.Create(CancellationToken.None);

            var components = verInfo.Components.ToArray();
            Assert.AreEqual(1, components.Length);
            Assert.AreEqual("C1", components[0].ComponentId);
            Assert.AreEqual("2.0", components[0].Version.ToString());
            Assert.AreEqual("C1 component", components[0].Description);
            Assert.AreEqual(1, components[0].Dependencies.Length);
            Assert.AreEqual("C2: 1.0 <= v <= 1.0", components[0].Dependencies[0].ToString());
        }

        [TestMethod]
        public void PatchingExec_InstallOne_Success()
        {
            var packages = new List<Package[]>();
            var log = new List<PatchExecutionLogRecord>();
            void LogMessage(PatchExecutionLogRecord record)
            {
                packages.Add(LoadPackages());
                log.Add(record);
            }

            var executed = new List<ISnPatch>();
            void Execute(ISnPatch patch)
            {
                executed.Add(patch);
            }

            var installed = new SnComponentDescriptor[0];
            var patches = new ISnPatch[]
            {
                //Patch("C1", "1.0 <= v < 2.0", "v2.0", null, 
                //    ctx => ExecutionResult.Successful),
                Inst("C1", "v1.0", null,
                    ctx => { Execute(ctx.CurrentPatch); }),
            };

            // ACTION
            var context = new PatchExecutionContext();
            context.LogMessage = LogMessage;
            var pm = new PatchManager();
            pm.ExecuteRelevantPatches(patches, installed, context);

            // ASSERT
            Assert.AreEqual(0, context.Errors.Length);
            Assert.AreEqual(2, log.Count);
            Assert.AreEqual("C1i1.0", PatchesToString(executed.ToArray()));
            Assert.AreEqual("[C1: 1.0] ExecutionStart.", log[0].ToString());
            Assert.AreEqual("[C1: 1.0] ExecutionFinished. Successful", log[1].ToString());
            Assert.AreEqual("1, C1: Install Unfinished, 1.0", PackagesToString(packages[0]));
            Assert.AreEqual("1, C1: Install Successful, 1.0", PackagesToString(packages[1]));
        }
        [TestMethod]
        public void PatchingExec_InstallOne_Faulty()
        {
            var packages = new List<Package[]>();
            var log = new List<PatchExecutionLogRecord>();
            void LogMessage(PatchExecutionLogRecord record)
            {
                packages.Add(LoadPackages());
                log.Add(record);
            }

            var executed = new List<ISnPatch>();
            void Execute(ISnPatch patch)
            {
                executed.Add(patch);
            }

            var installed = new SnComponentDescriptor[0];
            var patches = new ISnPatch[]
            {
                //Patch("C1", "1.0 <= v < 2.0", "v2.0", null, 
                //    ctx => ExecutionResult.Successful),
                Inst("C1", "v1.0", null,
                    ctx => { throw new Exception("Error inda patch."); }),
            };

            // ACTION
            var context = new PatchExecutionContext();
            context.LogMessage = LogMessage;
            var pm = new PatchManager();
            pm.ExecuteRelevantPatches(patches, installed, context);

            // ASSERT
            Assert.AreEqual(0, context.Errors.Length);
            Assert.AreEqual(2, log.Count);
            Assert.AreEqual("", PatchesToString(executed.ToArray()));
            Assert.AreEqual("[C1: 1.0] ExecutionStart.", log[0].ToString());
            Assert.AreEqual("[C1: 1.0] ExecutionFinished. Faulty", log[1].ToString());
            Assert.AreEqual("1, C1: Install Unfinished, 1.0", PackagesToString(packages[0]));
            Assert.AreEqual("1, C1: Install Faulty, 1.0", PackagesToString(packages[1]));
        }
        [TestMethod]
        public void PatchingExec_PatchOne_Success()
        {
            var packages = new List<Package[]>();
            var log = new List<PatchExecutionLogRecord>();
            void LogMessage(PatchExecutionLogRecord record)
            {
                packages.Add(LoadPackages());
                log.Add(record);
            }

            var executed = new List<ISnPatch>();
            void Execute(ISnPatch patch)
            {
                executed.Add(patch);
            }

            var installed = new SnComponentDescriptor[0];
            var patches = new ISnPatch[]
            {
                Patch("C1", "1.0 <= v < 2.0", "v2.0", null,
                    ctx => { Execute(ctx.CurrentPatch); }),
                Inst("C1", "v1.0", null,
                    ctx => { Execute(ctx.CurrentPatch); }),
            };

            // ACTION
            var context = new PatchExecutionContext();
            context.LogMessage = LogMessage;
            var pm = new PatchManager();
            pm.ExecuteRelevantPatches(patches, installed, context);

            // ASSERT
            Assert.AreEqual(0, context.Errors.Length);
            Assert.AreEqual(4, log.Count);
            Assert.AreEqual("C1i1.0 C1p2.0", PatchesToString(executed.ToArray()));
            Assert.AreEqual("[C1: 1.0] ExecutionStart.", log[0].ToString());
            Assert.AreEqual("[C1: 1.0] ExecutionFinished. Successful", log[1].ToString());
            Assert.AreEqual("[C1: 1.0 <= v < 2.0 --> 2.0] ExecutionStart.", log[2].ToString());
            Assert.AreEqual("[C1: 1.0 <= v < 2.0 --> 2.0] ExecutionFinished. Successful", log[3].ToString());
            Assert.AreEqual(4, packages.Count);
            Assert.AreEqual("1, C1: Install Unfinished, 1.0", PackagesToString(packages[0]));
            Assert.AreEqual("1, C1: Install Successful, 1.0", PackagesToString(packages[1]));
            Assert.AreEqual("1, C1: Install Successful, 1.0|2, C1: Patch Unfinished, 2.0", PackagesToString(packages[2]));
            Assert.AreEqual("1, C1: Install Successful, 1.0|2, C1: Patch Successful, 2.0", PackagesToString(packages[3]));
        }
        [TestMethod]
        public void PatchingExec_PatchOne_Faulty()
        {
            var packages = new List<Package[]>();
            var log = new List<PatchExecutionLogRecord>();
            void LogMessage(PatchExecutionLogRecord record)
            {
                packages.Add(LoadPackages());
                log.Add(record);
            }

            var executed = new List<ISnPatch>();
            void Execute(ISnPatch patch)
            {
                executed.Add(patch);
            }

            var installed = new SnComponentDescriptor[0];
            var patches = new ISnPatch[]
            {
                Patch("C1", "1.0 <= v < 2.0", "v2.0", null,
                    ctx => { throw new Exception("Error inda patch."); }),
                Inst("C1", "v1.0", null,
                    ctx => { Execute(ctx.CurrentPatch); }),
            };

            // ACTION
            var context = new PatchExecutionContext();
            context.LogMessage = LogMessage;
            var pm = new PatchManager();
            pm.ExecuteRelevantPatches(patches, installed, context);

            // ASSERT
            Assert.AreEqual(0, context.Errors.Length);
            Assert.AreEqual(4, log.Count);
            Assert.AreEqual("C1i1.0", PatchesToString(executed.ToArray()));
            Assert.AreEqual("[C1: 1.0] ExecutionStart.", log[0].ToString());
            Assert.AreEqual("[C1: 1.0] ExecutionFinished. Successful", log[1].ToString());
            Assert.AreEqual("[C1: 1.0 <= v < 2.0 --> 2.0] ExecutionStart.", log[2].ToString());
            Assert.AreEqual("[C1: 1.0 <= v < 2.0 --> 2.0] ExecutionFinished. Faulty", log[3].ToString());
            Assert.AreEqual(4, packages.Count);
            Assert.AreEqual("1, C1: Install Unfinished, 1.0", PackagesToString(packages[0]));
            Assert.AreEqual("1, C1: Install Successful, 1.0", PackagesToString(packages[1]));
            Assert.AreEqual("1, C1: Install Successful, 1.0|2, C1: Patch Unfinished, 2.0", PackagesToString(packages[2]));
            Assert.AreEqual("1, C1: Install Successful, 1.0|2, C1: Patch Faulty, 2.0", PackagesToString(packages[3]));
        }

        /* ===================================================================== TOOLS */

        internal static void DropPackagesTable(SnDataContext ctx)
        {
            var sql = @"
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Packages]') AND type in (N'U'))
DROP TABLE [dbo].[Packages]
";
            ctx.ExecuteNonQueryAsync(sql).GetAwaiter().GetResult();
        }
        internal static void InstallPackagesTable(SnDataContext ctx)
        {
            var sql = @"
CREATE TABLE [dbo].[Packages](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[PackageType] [varchar](50) NOT NULL,
	[ComponentId] [nvarchar](450) NULL,
	[ComponentVersion] [varchar](50) NULL,
	[ReleaseDate] [datetime2](7) NOT NULL,
	[ExecutionDate] [datetime2](7) NOT NULL,
	[ExecutionResult] [varchar](50) NOT NULL,
	[ExecutionError] [nvarchar](max) NULL,
	[Description] [nvarchar](1000) NULL,
	[Manifest] [nvarchar](max) NULL,
 CONSTRAINT [PK_Packages] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
";

            ctx.ExecuteNonQueryAsync(sql).GetAwaiter().GetResult();
        }

        protected Package[] LoadPackages()
        {
            var dataProvider = DataStore.GetDataProviderExtension<IPackagingDataProviderExtension>();
            return dataProvider.LoadInstalledPackagesAsync(CancellationToken.None)
                .ConfigureAwait(false).GetAwaiter().GetResult().ToArray();
        }

        /// <summary>
        /// Creates a SnComponentDescriptor for test purposes.
        /// </summary>
        /// <param name="id">ComponentId</param>
        /// <param name="version">Last saved version.</param>
        /// <returns></returns>
        protected SnComponentDescriptor Comp(string id, string version)
        {
            return new SnComponentDescriptor(id, Version.Parse(version.TrimStart('v')), "", null);
        }
        /// <summary>
        /// Creates a ComponentInstaller for test purposes
        /// </summary>
        /// <param name="id">ComponentId</param>
        /// <param name="version">Target version</param>
        /// <param name="dependencies">Dependency array. Use null if there is no dependencies.</param>
        /// <returns></returns>
        protected ComponentInstaller Inst(string id, string version, Dependency[] dependencies)
        {
            return new ComponentInstaller
            {
                ComponentId = id,
                Version = Version.Parse(version.TrimStart('v')),
                Dependencies = dependencies,
            };
        }
        /// <summary>
        /// Creates a ComponentInstaller for test purposes
        /// </summary>
        /// <param name="id">ComponentId</param>
        /// <param name="version">Target version</param>
        /// <param name="dependencies">Dependency array. Use null if there is no dependencies.</param>
        /// <param name="execute">Function of execution</param>
        /// <returns></returns>
        protected ComponentInstaller Inst(string id, string version, Dependency[] dependencies,
            Action<PatchExecutionContext> execute)
        {
            return new ComponentInstaller
            {
                ComponentId = id,
                Version = Version.Parse(version.TrimStart('v')),
                Dependencies = dependencies,
                Execute = execute
            };
        }
        /// <summary>
        /// Creates a patch for test purposes.
        /// </summary>
        /// <param name="id">ComponentId</param>
        /// <param name="version">Target version</param>
        /// <param name="boundary">Complex source version. Example: "1.1 &lt;= v &lt;= 1.1"</param>
        /// <param name="dependencies">Dependency array. Use null if there is no dependencies.</param>
        /// <returns></returns>
        protected SnPatch Patch(string id, string boundary, string version, Dependency[] dependencies)
        {
            return new SnPatch
            {
                ComponentId = id,
                Version = version == null ? null : Version.Parse(version.TrimStart('v')),
                Boundary = ParseBoundary(boundary),
                Dependencies = dependencies
            };
        }
        /// <summary>
        /// Creates a patch for test purposes.
        /// </summary>
        /// <param name="id">ComponentId</param>
        /// <param name="version">Target version</param>
        /// <param name="boundary">Complex source version. Example: "1.1 &lt;= v &lt;= 1.1"</param>
        /// <param name="dependencies">Dependency array. Use null if there is no dependencies.</param>
        /// <param name="execute">Function of execution</param>
        /// <returns></returns>
        protected SnPatch Patch(string id, string boundary, string version, Dependency[] dependencies,
            Action<PatchExecutionContext> execute)
        {
            return new SnPatch
            {
                ComponentId = id,
                Version = version == null ? null : Version.Parse(version.TrimStart('v')),
                Boundary = ParseBoundary(boundary),
                Dependencies = dependencies,
                Execute = execute
            };
        }
        /// <summary>
        /// Creates a Dependency for test purposes.
        /// </summary>
        /// <param name="id">ComponentId</param>
        /// <param name="boundary">Complex source version. Example: "1.1 &lt;= v &lt;= 1.1"</param>
        /// <returns></returns>
        protected Dependency Dep(string id, string boundary)
        {
            return new Dependency
            {
                Id = id,
                Boundary = ParseBoundary(boundary)
            };
        }
        protected VersionBoundary ParseBoundary(string src)
        {
            // "1.0 <= v <  2.0"

            var a = src.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var boundary = new VersionBoundary();

            if (a.Length == 3)
            {
                if (a[0] == "v")
                {
                    boundary.MaxVersion = Version.Parse(a[2]);
                    boundary.MaxVersionIsExclusive = a[1] == "<";

                    boundary.MinVersion = Version.Parse("0.0");
                    boundary.MinVersionIsExclusive = false;
                }
                else if (a[2] == "v")
                {
                    boundary.MinVersion = Version.Parse(a[0]);
                    boundary.MinVersionIsExclusive = a[1] == "<";

                    boundary.MaxVersion = new Version(int.MaxValue, int.MaxValue);
                    boundary.MaxVersionIsExclusive = false;
                }
                else
                {
                    throw new FormatException($"Invalid Boundary: {src}");
                }
            }
            else if (a.Length == 5 && a[2] == "v")
            {
                boundary.MinVersion = Version.Parse(a[0]);
                boundary.MinVersionIsExclusive = a[1] == "<";

                boundary.MaxVersion = Version.Parse(a[4]);
                boundary.MaxVersionIsExclusive = a[3] == "<";
            }
            else
            {
                throw new FormatException($"Invalid Boundary: {src}");
            }

            return boundary;
        }

        private string ComponentsToString(SnComponentDescriptor[] components)
        {
            return string.Join(" ", components.OrderBy(x => x.ComponentId)
                .Select(x => $"{x.ComponentId}v{x.Version}"));
        }
        private string PatchesToString(ISnPatch[] executables)
        {
            return string.Join(" ", executables.Select(x =>
                $"{x.ComponentId}{(x.Type == PackageType.Install ? "i" : "p")}{x.Version}"));
        }
        private string PackagesToString(Package[] packages)
        {
            return string.Join("|", packages.Select(p => p.ToString()));
        }


        internal static Manifest ParseManifestHead(string manifestXml)
        {
            var xml = new XmlDocument();
            xml.LoadXml(manifestXml);
            var manifest = new Manifest();
            Manifest.ParseHead(xml, manifest);
            return manifest;
        }
        internal static Manifest ParseManifest(string manifestXml, int currentPhase)
        {
            var xml = new XmlDocument();
            xml.LoadXml(manifestXml);
            return Manifest.Parse(xml, currentPhase, true, new PackageParameter[0]);
        }

    }
}
