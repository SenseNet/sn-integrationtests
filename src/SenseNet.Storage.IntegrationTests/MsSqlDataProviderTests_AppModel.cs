using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage.AppModel;

namespace SenseNet.Storage.IntegrationTests
{
    public partial class MsSqlDataProviderTests
    {
        [TestMethod]
        public async Task MsSqlDP_AppModel_ResolveFromPredefinedPaths_First()
        {
            await StorageTest(() =>
            {
                var backup = Indexing.IsOuterSearchEngineEnabled;
                Indexing.IsOuterSearchEngineEnabled = false;
                try
                {
                    var paths = new[]
                    {
                        "/Root/AA/BB/CC",
                        "/Root/AA",
                        "/Root/System",
                        "/Root",
                    };

                    // ACTION
                    var nodeHead = ApplicationResolver.ResolveFirstByPaths(paths);

                    // ASSERT
                    Assert.IsNotNull(nodeHead);
                    Assert.IsTrue(nodeHead.Path == "/Root/System", "Path does not equal the expected");
                }
                finally
                {
                    Indexing.IsOuterSearchEngineEnabled = backup;
                }

                return Task.CompletedTask;
            });
        }
        [TestMethod]
        public async Task MsSqlDP_AppModel_ResolveFromPredefinedPaths_All()
        {
            await StorageTest(() =>
            {
                var backup = Indexing.IsOuterSearchEngineEnabled;
                Indexing.IsOuterSearchEngineEnabled = false;
                try
                {
                    var paths = new[]
                    {
                        "/Root/AA/BB/CC",
                        "/Root/AA",
                        "/Root/System",
                        "/Root",
                    };

                    // ACTION
                    var nodeHeads = ApplicationResolver.ResolveAllByPaths(paths, false).ToArray();

                    // ASSERT
                    Assert.AreEqual(2, nodeHeads.Length);
                    Assert.AreEqual("/Root/System", nodeHeads[0].Path);
                    Assert.AreEqual("/Root", nodeHeads[1].Path);
                }
                finally
                {
                    Indexing.IsOuterSearchEngineEnabled = backup;
                }

                return Task.CompletedTask;
            });
        }
    }
}
