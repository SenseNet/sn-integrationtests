using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Common.Storage.Data;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Schema;
using SenseNet.ContentRepository.Versioning;
using SenseNet.Tests.Implementations;
using SenseNet.Tests.Implementations2;
using Task = System.Threading.Tasks.Task;

namespace SenseNet.Storage.IntegrationTests
{
    [TestClass]
    public class MsSqlDataProviderTests : StorageTestBase
    {
        // ReSharper disable once InconsistentNaming
        private static MsSqlDataProvider DP => (MsSqlDataProvider)DataStore.DataProvider;
        // ReSharper disable once InconsistentNaming
        private static ITestingDataProviderExtension TDP => DataStore.GetDataProviderExtension<ITestingDataProviderExtension>();

        [TestMethod]
        public async Task MsSqlDP_InsertNode()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var root = CreateTestRoot();

                // Create a file but do not save.
                var created = new File(root) { Name = "File1", Index = 42, Description = "File1 Description" };
                created.Binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content"));
                var nodeData = created.Data;
                nodeData.Path = RepositoryPath.Combine(created.ParentPath, created.Name);
                GenerateTestData(nodeData);


                // ACTION
                var nodeHeadData = nodeData.GetNodeHeadData();
                var versionData = nodeData.GetVersionData();
                var dynamicData = nodeData.GetDynamicData(false);
                var binaryProperty = dynamicData.BinaryProperties.First().Value;
                await DP.InsertNodeAsync(nodeHeadData, versionData, dynamicData);

                // ASSERT
                Assert.IsTrue(nodeHeadData.NodeId > 0);
                Assert.IsTrue(nodeHeadData.Timestamp > 0);
                Assert.IsTrue(versionData.VersionId > 0);
                Assert.IsTrue(versionData.Timestamp > 0);
                Assert.IsTrue(binaryProperty.Id > 0);
                Assert.IsTrue(binaryProperty.FileId > 0);
                Assert.IsTrue(nodeHeadData.LastMajorVersionId == versionData.VersionId);
                Assert.IsTrue(nodeHeadData.LastMajorVersionId == nodeHeadData.LastMinorVersionId);

                DistributedApplication.Cache.Reset();
                var loaded = Node.Load<File>(nodeHeadData.NodeId);
                Assert.IsNotNull(loaded);
                Assert.AreEqual("File1", loaded.Name);
                Assert.AreEqual(nodeHeadData.Path, loaded.Path);
                Assert.AreEqual(42, loaded.Index);
                Assert.AreEqual("File1 Content", RepositoryTools.GetStreamString(loaded.Binary.GetStream()));

                foreach (var propType in loaded.Data.PropertyTypes)
                    loaded.Data.GetDynamicRawData(propType);
                DataProviderChecker.Assert_DynamicPropertiesAreEqualExceptBinaries(nodeData, loaded.Data);

            });
        }
        [TestMethod]
        public async Task MsSqlDP_Update()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var root = CreateTestRoot();

                var created = new File(root) { Name = "File1", Index = 42 };
                created.Binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content"));
                created.Save();

                // Update a file but do not save
                var updated = Node.Load<File>(created.Id);
                updated.Index = 142;
                updated.Binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content UPDATED"));
                var nodeData = updated.Data;
                GenerateTestData(nodeData);

                // ACTION
                var nodeHeadData = nodeData.GetNodeHeadData();
                var versionData = nodeData.GetVersionData();
                var dynamicData = nodeData.GetDynamicData(false);
                var versionIdsToDelete = new int[0];
                //var binaryProperty = dynamicData.BinaryProperties.First().Value;
                await DP.UpdateNodeAsync(nodeHeadData, versionData, dynamicData, versionIdsToDelete);

                // ASSERT
                Assert.IsTrue(nodeHeadData.Timestamp > created.NodeTimestamp);

                DistributedApplication.Cache.Reset();
                var loaded = Node.Load<File>(nodeHeadData.NodeId);
                Assert.IsNotNull(loaded);
                Assert.AreEqual("File1", loaded.Name);
                Assert.AreEqual(nodeHeadData.Path, loaded.Path);
                Assert.AreEqual(142, loaded.Index);
                Assert.AreEqual("File1 Content UPDATED", RepositoryTools.GetStreamString(loaded.Binary.GetStream()));

                foreach (var propType in loaded.Data.PropertyTypes)
                    loaded.Data.GetDynamicRawData(propType);
                DataProviderChecker.Assert_DynamicPropertiesAreEqualExceptBinaries(nodeData, loaded.Data);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_CopyAndUpdate_NewVersion()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var root = CreateTestRoot();
                root.Save();
                var created = new File(root) { Name = "File1", VersioningMode = VersioningType.MajorAndMinor };
                created.Binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content"));
                created.Save();

                // Update a file but do not save
                var updated = Node.Load<File>(created.Id);
                var binary = updated.Binary;
                binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content UPDATED"));
                updated.Binary = binary;
                var nodeData = updated.Data;

                // Patch version because the NodeSaveSetting logic is skipped.
                nodeData.Version = VersionNumber.Parse("V0.2.D");

                // Update dynamic properties
                GenerateTestData(nodeData);
                var versionIdBefore = nodeData.VersionId;
                var modificationDateBefore = nodeData.ModificationDate;
                var nodeTimestampBefore = nodeData.NodeTimestamp;

                // ACTION
                nodeData.ModificationDate = DateTime.UtcNow;
                var nodeHeadData = nodeData.GetNodeHeadData();
                var versionData = nodeData.GetVersionData();
                var dynamicData = nodeData.GetDynamicData(false);
                var versionIdsToDelete = new int[0];
                await DP.CopyAndUpdateNodeAsync(nodeHeadData, versionData, dynamicData, versionIdsToDelete);

                // ASSERT
                Assert.AreNotEqual(versionIdBefore, versionData.VersionId);

                DistributedApplication.Cache.Reset();
                var loaded = Node.Load<File>(nodeHeadData.NodeId);
                Assert.IsNotNull(loaded);
                Assert.AreEqual("File1", loaded.Name);
                Assert.AreEqual(nodeHeadData.Path, loaded.Path);
                Assert.AreNotEqual(nodeTimestampBefore, loaded.NodeTimestamp);
                Assert.AreNotEqual(modificationDateBefore, loaded.ModificationDate);
                Assert.AreEqual("File1 Content UPDATED", RepositoryTools.GetStreamString(loaded.Binary.GetStream()));

                foreach (var propType in loaded.Data.PropertyTypes)
                    loaded.Data.GetDynamicRawData(propType);
                DataProviderChecker.Assert_DynamicPropertiesAreEqualExceptBinaries(nodeData, loaded.Data);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_CopyAndUpdate_ExpectedVersion()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var root = CreateTestRoot();
                root.Save();
                var created = new File(root) { Name = "File1" };
                created.Binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content"));
                created.Save();
                var versionIdBefore = created.VersionId;

                created.CheckOut();

                // Update a file but do not save
                var updated = Node.Load<File>(created.Id);
                var binary = updated.Binary;
                binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content UPDATED"));
                updated.Binary = binary;
                var nodeData = updated.Data;

                // Patch version because the NodeSaveSetting logic is skipped.
                nodeData.Version = VersionNumber.Parse("V1.0.A");

                // Update dynamic properties
                GenerateTestData(nodeData);
                var modificationDateBefore = nodeData.ModificationDate;
                var nodeTimestampBefore = nodeData.NodeTimestamp;

                // ACTION
                nodeData.ModificationDate = DateTime.UtcNow;
                var nodeHeadData = nodeData.GetNodeHeadData();
                var versionData = nodeData.GetVersionData();
                var dynamicData = nodeData.GetDynamicData(false);
                var versionIdsToDelete = new int[] { versionData.VersionId };
                var expectedVersionId = versionIdBefore;
                await DP.CopyAndUpdateNodeAsync(nodeHeadData, versionData, dynamicData, versionIdsToDelete, expectedVersionId);

                // ASSERT
                Assert.AreEqual(versionIdBefore, versionData.VersionId);

                DistributedApplication.Cache.Reset();
                var loaded = Node.Load<File>(nodeHeadData.NodeId);
                Assert.IsNotNull(loaded);
                Assert.AreEqual("File1", loaded.Name);
                Assert.AreEqual(nodeHeadData.Path, loaded.Path);
                Assert.AreNotEqual(nodeTimestampBefore, loaded.NodeTimestamp);
                Assert.AreNotEqual(modificationDateBefore, loaded.ModificationDate);
                Assert.AreEqual(VersionNumber.Parse("V1.0.A"), loaded.Version);
                Assert.AreEqual(versionIdBefore, loaded.VersionId);
                Assert.AreEqual("File1 Content UPDATED", RepositoryTools.GetStreamString(loaded.Binary.GetStream()));

                foreach (var propType in loaded.Data.PropertyTypes)
                    loaded.Data.GetDynamicRawData(propType);
                DataProviderChecker.Assert_DynamicPropertiesAreEqualExceptBinaries(nodeData, loaded.Data);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_UpdateNodeHead()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                // Create a file under the test root
                var root = CreateTestRoot();
                root.Save();
                var created = new File(root) { Name = "File1" };
                created.Binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content"));
                created.Save();

                // Memorize final expectations
                var expectedVersion = created.Version;
                var expectedVersionId = created.VersionId;
                var createdHead = NodeHead.Get(created.Id);
                var expectedLastMajor = createdHead.LastMajorVersionId;
                var expectedLastMinor = createdHead.LastMinorVersionId;

                // Make a new version.
                created.CheckOut();

                // Modify the new version.
                var checkedOut = Node.Load<File>(created.Id);
                var binary = checkedOut.Binary;
                binary.SetStream(RepositoryTools.GetStreamFromString("File1 Content UPDATED"));
                checkedOut.Binary = binary;
                checkedOut.Save();

                // PREPARE THE LAST ACTION: simulate UndoCheckOut
                var modified = Node.Load<File>(created.Id);
                var oldTimestamp = modified.NodeTimestamp;
                // Get the editable NodeData
                modified.Index = modified.Index;
                var nodeData = modified.Data;

                nodeData.LastLockUpdate = DP.DateTimeMinValue;
                nodeData.LockDate = DP.DateTimeMinValue;
                nodeData.Locked = false;
                nodeData.LockedById = 0;
                nodeData.ModificationDate = DateTime.UtcNow;
                var nodeHeadData = nodeData.GetNodeHeadData();
                var deletedVersionId = nodeData.VersionId;
                var versionIdsToDelete = new int[] { deletedVersionId };

                // ACTION: Simulate UndoCheckOut
                await DP.UpdateNodeHeadAsync(nodeHeadData, versionIdsToDelete);

                // ASSERT: the original state is restored after the UndoCheckOut operation
                Assert.IsTrue(oldTimestamp < nodeHeadData.Timestamp);
                DistributedApplication.Cache.Reset();
                var reloaded = Node.Load<File>(created.Id);
                Assert.AreEqual(expectedVersion, reloaded.Version);
                Assert.AreEqual(expectedVersionId, reloaded.VersionId);
                var reloadedHead = NodeHead.Get(created.Id);
                Assert.AreEqual(expectedLastMajor, reloadedHead.LastMajorVersionId);
                Assert.AreEqual(expectedLastMinor, reloadedHead.LastMinorVersionId);
                Assert.AreEqual("File1 Content", RepositoryTools.GetStreamString(reloaded.Binary.GetStream()));
                Assert.IsNull(Node.LoadNodeByVersionId(deletedVersionId));
            });
        }

        [TestMethod]
        public async Task MsSqlDP_HandleAllDynamicProps()
        {
            var contentTypeName = "TestContent";
            var ctd = $"<ContentType name='{contentTypeName}' parentType='GenericContent'" + @"
             handler='SenseNet.ContentRepository.GenericContent'
             xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentTypeDefinition'>
  <Fields>
    <Field name='ShortText1' type='ShortText'/>
    <Field name='LongText1' type='LongText'/>
    <Field name='Integer1' type='Integer'/>
    <Field name='Number1' type='Number'/>
    <Field name='DateTime1' type='DateTime'/>
    <Field name='Reference1' type='Reference'/>
  </Fields>
</ContentType>
";
            await IsolatedStorageTest(async () =>
            {
                Node node = null;
                try
                {
                    DataStore.Enabled = true;

                    ContentTypeInstaller.InstallContentType(ctd);
                    var unused = ContentType.GetByName(contentTypeName); // preload schema

                    var root = new SystemFolder(Repository.Root) { Name = "TestRoot" };
                    root.Save();

                    // ACTION-1 CREATE
                    // Create all kind of dynamic properties
                    node = new GenericContent(root, contentTypeName)
                    {
                        Name = $"{contentTypeName}1",
                        ["ShortText1"] = "ShortText value 1",
                        ["LongText1"] = "LongText value 1",
                        ["Integer1"] = 42,
                        ["Number1"] = 42.56m,
                        ["DateTime1"] = new DateTime(1111, 11, 11)
                    };
                    node.AddReference("Reference1", Repository.Root);
                    node.AddReference("Reference1", root);
                    node.Save();

                    // ASSERT-1
                    Assert.AreEqual("ShortText value 1", await TDP.GetPropertyValueAsync(node.VersionId, "ShortText1"));
                    Assert.AreEqual("LongText value 1", await TDP.GetPropertyValueAsync(node.VersionId, "LongText1"));
                    Assert.AreEqual(42, await TDP.GetPropertyValueAsync(node.VersionId, "Integer1"));
                    Assert.AreEqual(42.56m, await TDP.GetPropertyValueAsync(node.VersionId, "Number1"));
                    Assert.AreEqual(new DateTime(1111, 11, 11), await TDP.GetPropertyValueAsync(node.VersionId, "DateTime1"));
                    Assert.AreEqual($"{Repository.Root.Id},{root.Id}", ArrayToString((int[])await TDP.GetPropertyValueAsync(node.VersionId, "Reference1")));

                    // ACTION-2 UPDATE-1
                    node = Node.Load<GenericContent>(node.Id);
                    // Update all kind of dynamic properties
                    node["ShortText1"] = "ShortText value 2";
                    node["LongText1"] = "LongText value 2";
                    node["Integer1"] = 43;
                    node["Number1"] = 42.099m;
                    node["DateTime1"] = new DateTime(1111, 11, 22);
                    node.RemoveReference("Reference1", Repository.Root);
                    node.Save();

                    // ASSERT-2
                    Assert.AreEqual("ShortText value 2", await TDP.GetPropertyValueAsync(node.VersionId, "ShortText1"));
                    Assert.AreEqual("LongText value 2", await TDP.GetPropertyValueAsync(node.VersionId, "LongText1"));
                    Assert.AreEqual(43, await TDP.GetPropertyValueAsync(node.VersionId, "Integer1"));
                    Assert.AreEqual(42.099m, await TDP.GetPropertyValueAsync(node.VersionId, "Number1"));
                    Assert.AreEqual(new DateTime(1111, 11, 22), await TDP.GetPropertyValueAsync(node.VersionId, "DateTime1"));
                    Assert.AreEqual($"{root.Id}", ArrayToString((int[])await TDP.GetPropertyValueAsync(node.VersionId, "Reference1")));

                    // ACTION-3 UPDATE-2
                    node = Node.Load<GenericContent>(node.Id);
                    // Remove existing references
                    node.RemoveReference("Reference1", root);
                    node.Save();

                    // ASSERT-3
                    Assert.AreEqual("ShortText value 2", await TDP.GetPropertyValueAsync(node.VersionId, "ShortText1"));
                    Assert.AreEqual("LongText value 2", await TDP.GetPropertyValueAsync(node.VersionId, "LongText1"));
                    Assert.AreEqual(43, await TDP.GetPropertyValueAsync(node.VersionId, "Integer1"));
                    Assert.AreEqual(42.099m, await TDP.GetPropertyValueAsync(node.VersionId, "Number1"));
                    Assert.AreEqual(new DateTime(1111, 11, 22), await TDP.GetPropertyValueAsync(node.VersionId, "DateTime1"));
                    Assert.IsNull(await TDP.GetPropertyValueAsync(node.VersionId, "Reference1"));
                }
                finally
                {
                    node?.ForceDelete();
                    ContentTypeInstaller.RemoveContentType(contentTypeName);
                }
            });
        }


        [TestMethod]
        public async Task MsSqlDP_Rename()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                // Create a small subtree
                var root = CreateTestRoot(); root.Save();
                var f1 = new SystemFolder(root) {Name = "F1"}; f1.Save();
                var f2 = new SystemFolder(root) {Name = "F2"}; f2.Save();
                var f3 = new SystemFolder(f1) {Name = "F3"}; f3.Save();
                var f4 = new SystemFolder(f1) {Name = "F4"}; f4.Save();

                // ACTION: Rename root
                root = Node.Load<SystemFolder>(root.Id);
                var originalPath = root.Path;
                var newName = Guid.NewGuid() + "-RENAMED";
                root.Name = newName;
                var nodeData = root.Data;
                nodeData.Path = RepositoryPath.Combine(root.ParentPath, root.Name); // ApplySettings
                var nodeHeadData = nodeData.GetNodeHeadData();
                var versionData = nodeData.GetVersionData();
                var dynamicData = nodeData.GetDynamicData(false);
                var versionIdsToDelete = new int[0];
                await DP.UpdateNodeAsync(nodeHeadData, versionData, dynamicData, versionIdsToDelete, originalPath);

                // ASSERT
                DistributedApplication.Cache.Reset();
                f1 = Node.Load<SystemFolder>(f1.Id);
                f2 = Node.Load<SystemFolder>(f2.Id);
                f3 = Node.Load<SystemFolder>(f3.Id);
                f4 = Node.Load<SystemFolder>(f4.Id);
                Assert.AreEqual("/Root/" + newName, root.Path);
                Assert.AreEqual("/Root/" + newName + "/F1", f1.Path);
                Assert.AreEqual("/Root/" + newName + "/F2", f2.Path);
                Assert.AreEqual("/Root/" + newName + "/F1/F3", f3.Path);
                Assert.AreEqual("/Root/" + newName + "/F1/F4", f4.Path);
            });
        }

        [TestMethod]
        public async Task MsSqlDP_LoadChildren()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_Move()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_RefreshCacheAfterSave()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_LazyLoadedBigText()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_LazyLoadedBigTextVsCache()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_LoadChildTypesToAllow()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_ContentListTypesInTree()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_ForceDelete()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var countsBefore = await GetDbObjectCountsAsync(null, DP, TDP);

                // Create a small subtree
                var root = CreateTestRoot();
                root.Save();
                var f1 = new SystemFolder(root) { Name = "F1" };
                f1.Save();
                var f2 = new File(root) { Name = "F2" };
                f2.Binary.SetStream(RepositoryTools.GetStreamFromString("filecontent"));
                f2.Save();
                var f3 = new SystemFolder(f1) { Name = "F3" };
                f3.Save();
                var f4 = new File(root) { Name = "F4" };
                f4.Binary.SetStream(RepositoryTools.GetStreamFromString("filecontent"));
                f4.Save();

                // ACTION
                Node.ForceDelete(root.Path);

                // ASSERT
                Assert.IsNull(Node.Load<SystemFolder>(root.Id));
                Assert.IsNull(Node.Load<SystemFolder>(f1.Id));
                Assert.IsNull(Node.Load<File>(f2.Id));
                Assert.IsNull(Node.Load<SystemFolder>(f3.Id));
                Assert.IsNull(Node.Load<File>(f4.Id));
                var countsAfter = await GetDbObjectCountsAsync(null, DP, TDP);
                Assert.AreEqual(countsBefore.AllCountsExceptFiles, countsAfter.AllCountsExceptFiles);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_DeleteDeleted()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var folder = new SystemFolder(Repository.Root) { Name = "Folder1" };
                folder.Save();
                folder = Node.Load<SystemFolder>(folder.Id);
                var nodeHeadData = folder.Data.GetNodeHeadData();
                Node.ForceDelete(folder.Path);

                // ACTION
                await DP.DeleteNodeAsync(nodeHeadData);

                // ASSERT
                // Expectation: no exception was thrown
            });
        }

        [TestMethod]
        public async Task MsSqlDP_GetVersionNumbers()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_GetVersionNumbers_MissingNode()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_LoadBinaryPropertyValues()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_NodeEnumerator()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_NameSuffix()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_TreeSize_Root()
        {
            await StorageTest(async () =>
            {
                // ACTION
                var size = await DP.GetTreeSizeAsync("/Root", true);

                // ASSERT
                var expectedSize = (long)await ExecuteScalarAsync("SELECT SUM(Size) FROM Files");
                Assert.AreEqual(expectedSize, size);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_TreeSize_Subtree()
        {
            await StorageTest(async () =>
            {
                // ACTION
                var size = await DP.GetTreeSizeAsync("/Root/System/Schema/ContentTypes/GenericContent/Folder", true);

                // ASSERT
                var sql = @"SELECT SUM(Size) FROM Files f
    JOIN BinaryProperties b ON b.FileId = f.FileId
    JOIN Versions v ON v.VersionId = b.VersionId
    JOIN Nodes n on n.NodeId = v.NodeId
WHERE Path LIKE '/Root/System/Schema/ContentTypes/GenericContent/Folder%'";
                var expectedSize = (long) await ExecuteScalarAsync(sql);
                Assert.AreEqual(expectedSize, size);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_TreeSize_Item()
        {
            await StorageTest(async () =>
            {
                // ACTION
                var size = await DP.GetTreeSizeAsync("/Root/System/Schema/ContentTypes/GenericContent/Folder", false);

                // ASSERT
                var sql = @"SELECT SUM(Size) FROM Files f
    JOIN BinaryProperties b ON b.FileId = f.FileId
    JOIN Versions v ON v.VersionId = b.VersionId
    JOIN Nodes n on n.NodeId = v.NodeId
WHERE Path = '/Root/System/Schema/ContentTypes/GenericContent/Folder'";
                var expectedSize = (long) await ExecuteScalarAsync(sql);
                Assert.AreEqual(expectedSize, size);
            });
        }

        /* ================================================================================================== NodeQuery */

        [TestMethod]
        public async Task MsSqlDP_NodeQuery_InstanceCount()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_NodeQuery_ChildrenIdentfiers()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_NodeQuery_QueryNodesByTypeAndPathAndName()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_NodeQuery_QueryNodesByTypeAndPathAndProperty()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_NodeQuery_QueryNodesByReferenceAndType()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== TreeLock */

        [TestMethod]
        public async Task MsSqlDP_LoadEntityTree()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== TreeLock */

        [TestMethod]
        public async Task MsSqlDP_TreeLock()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== IndexDocument */

        [TestMethod]
        public async Task MsSqlDP_LoadIndexDocuments()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_SaveIndexDocumentById()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== IndexingActivities */

        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_GetLastIndexingActivityId()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_LoadIndexingActivities_Page()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_LoadIndexingActivities_PageUnprocessed()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_LoadIndexingActivities_Gaps()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_LoadIndexingActivities_Executable()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_UpdateRunningState()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_RefreshLockTime()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_DeleteFinished()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_IndexingActivity_LoadFull()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== Nodes */

        [TestMethod]
        public async Task MsSqlDP_CopyAndUpdateNode_Rename()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_LoadNodes()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_LoadNodeHeadByVersionId_Missing()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_NodeAndVersion_CountsAndTimestamps()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== Errors */

        [TestMethod]
        public async Task MsSqlDP_Error_InsertNode_AlreadyExists()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNode_Deleted()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNode_MissingVersion()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNode_OutOfDate()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_Error_CopyAndUpdateNode_Deleted()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Error_CopyAndUpdateNode_MissingVersion()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Error_CopyAndUpdateNode_OutOfDate()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNodeHead_Deleted()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNodeHead_OutOfDate()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_Error_DeleteNode()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_Error_MoveNode_MissingSource()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Error_MoveNode_MissingTarget()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Error_MoveNode_OutOfDate()
        {
            Assert.Inconclusive();
        }

        [TestMethod]
        public async Task MsSqlDP_Error_QueryNodesByReferenceAndTypeAsync()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== Transaction */

        [TestMethod]
        public async Task MsSqlDP_Transaction_InsertNode()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Transaction_UpdateNode()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Transaction_CopyAndUpdateNode()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Transaction_UpdateNodeHead()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Transaction_MoveNode()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Transaction_RenameNode()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_Transaction_DeleteNode()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== Schema */

        [TestMethod]
        public async Task MsSqlDP_Schema_()
        {
            Assert.Inconclusive();
        }

        /* ================================================================================================== */

        private void GenerateTestData(NodeData nodeData)
        {
            foreach (var propType in nodeData.PropertyTypes)
            {
                var data = GetTestData(propType);
                if (data != null)
                    nodeData.SetDynamicRawData(propType, data);
            }
        }
        private object GetTestData(PropertyType propType)
        {
            if (propType.Name == "AspectData")
                return "<AspectData />";
            switch (propType.DataType)
            {
                case DataType.String: return "String " + Guid.NewGuid();
                case DataType.Text: return "Text value" + Guid.NewGuid();
                case DataType.Int: return Rng();
                case DataType.Currency: return (decimal)Rng();
                case DataType.DateTime: return DateTime.UtcNow;
                case DataType.Reference: return new List<int> { Rng(), Rng() };
                case DataType.Binary:
                default:
                    return null;
            }
        }
        private Random _random = new Random();
        private int Rng()
        {
            return _random.Next(1, int.MaxValue);
        }

        private SystemFolder CreateTestRoot()
        {
            return CreateFolder(Repository.Root, "TestRoot" + Guid.NewGuid());
        }
        private SystemFolder CreateFolder(Node parent, string name)
        {
            var folder = new SystemFolder(parent) { Name = name };
            folder.Save();
            return folder;
        }
        private File CreateFile(Node parent, string name, string fileContent)
        {
            var file = new File(parent) { Name = name };
            file.Binary.SetStream(RepositoryTools.GetStreamFromString(fileContent));
            file.Save();
            return file;
        }

        private async Task<(int Nodes, int Versions, int Binaries, int Files, int LongTexts, string AllCounts, string AllCountsExceptFiles)> GetDbObjectCountsAsync(string path, DataProvider2 DP, ITestingDataProviderExtension tdp)
        {
            var nodes = await DP.GetNodeCountAsync(path);
            var versions = await DP.GetVersionCountAsync(path);
            var binaries = await TDP.GetBinaryPropertyCountAsync(path);
            var files = await TDP.GetFileCountAsync(path);
            var longTexts = await TDP.GetLongTextCountAsync(path);
            var all = $"{nodes},{versions},{binaries},{files},{longTexts}";
            var allExceptFiles = $"{nodes},{versions},{binaries},{longTexts}";

            var result = (Nodes: nodes, Versions: versions, Binaries: binaries, Files: files, LongTexts: longTexts, AllCounts: all, AllCountsExceptFiles: allExceptFiles);
            return await Task.FromResult(result);
        }

        private async Task<object> ExecuteScalarAsync(string sql)
        {
            using (var ctx = new SnDataContext(DP))
                return await ctx.ExecuteScalarAsync(sql);
        }

        protected string ArrayToString(int[] array)
        {
            return string.Join(",", array.Select(x => x.ToString()));
        }
        protected string ArrayToString(List<int> array)
        {
            return string.Join(",", array.Select(x => x.ToString()));
        }
        protected string ArrayToString(IEnumerable<object> array)
        {
            return string.Join(",", array.Select(x => x.ToString()));
        }

    }
}
