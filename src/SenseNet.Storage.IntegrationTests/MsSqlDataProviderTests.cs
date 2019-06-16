using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Common.Storage.Data;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Search.Querying;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.DataModel;
using SenseNet.ContentRepository.Storage.Schema;
using SenseNet.ContentRepository.Versioning;
using SenseNet.Search.Querying;
using SenseNet.Tests.Implementations;
using SenseNet.Tests.Implementations2;
using Task = System.Threading.Tasks.Task;

namespace SenseNet.Storage.IntegrationTests
{
    [TestClass]
    public partial class MsSqlDataProviderTests : StorageTestBase
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
                var root = CreateTestRoot();
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
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                DistributedApplication.Cache.Reset();
                var loaded = Repository.Root.Children.Select(x => x.Id.ToString()).ToArray();

                int[] expected;
                using (var ctx = new SnDataContext(DP))
                    expected = await ctx.ExecuteReaderAsync(
                        "SELECT * FROM Nodes WHERE ParentNodeId = " + Identifiers.PortalRootId,
                        cmd => { }, async reader =>
                        {
                            var result = new List<int>();
                            while (await reader.ReadAsync())
                                result.Add(reader.GetInt32(0));
                            return result.ToArray();
                        });

                Assert.AreEqual(string.Join(",", expected), string.Join(",", loaded));
            });
        }

        [TestMethod]
        public async Task MsSqlDP_Move()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                // Create a small subtree
                var root = CreateTestRoot();
                var rootPath = root.Path;
                var source = new SystemFolder(root) { Name = "Source" }; source.Save();
                var target = new SystemFolder(root) { Name = "Target" }; target.Save();
                var f1 = new SystemFolder(source) { Name = "F1" }; f1.Save();
                var f2 = new SystemFolder(source) { Name = "F2" }; f2.Save();
                var f3 = new SystemFolder(f1) { Name = "F3" }; f3.Save();
                var f4 = new SystemFolder(f1) { Name = "F4" }; f4.Save();

                // ACTION: Node.Move(source.Path, target.Path);
                var srcNodeHeadData = source.Data.GetNodeHeadData();
                await DP.MoveNodeAsync(srcNodeHeadData, target.Id, target.NodeTimestamp);

                // ASSERT
                DistributedApplication.Cache.Reset(); //UNDONE:DB: Need to work without explicite clear.
                target = Node.Load<SystemFolder>(target.Id);
                source = Node.Load<SystemFolder>(source.Id);
                f1 = Node.Load<SystemFolder>(f1.Id);
                f2 = Node.Load<SystemFolder>(f2.Id);
                f3 = Node.Load<SystemFolder>(f3.Id);
                f4 = Node.Load<SystemFolder>(f4.Id);
                Assert.AreEqual(rootPath, root.Path);
                Assert.AreEqual(rootPath + "/Target", target.Path);
                Assert.AreEqual(rootPath + "/Target/Source", source.Path);
                Assert.AreEqual(rootPath + "/Target/Source/F1", f1.Path);
                Assert.AreEqual(rootPath + "/Target/Source/F2", f2.Path);
                Assert.AreEqual(rootPath + "/Target/Source/F1/F3", f3.Path);
                Assert.AreEqual(rootPath + "/Target/Source/F1/F4", f4.Path);
            });
        }

        [TestMethod]
        public async Task MsSqlDP_RefreshCacheAfterSave()
        {
            await StorageTest(() =>
            {
                DataStore.Enabled = true;

                var root = new SystemFolder(Repository.Root) { Name = Guid.NewGuid().ToString() };

                // ACTION-1: Create
                root.Save();
                var nodeTimestamp1 = root.NodeTimestamp;
                var versionTimestamp1 = root.VersionTimestamp;

                // ASSERT-1: NodeData is in cache after creation
                var cacheKey1 = DataStore.GenerateNodeDataVersionIdCacheKey(root.VersionId);
                var item1 = DistributedApplication.Cache[cacheKey1];
                Assert.IsNotNull(item1);
                var cachedNodeData1 = item1 as NodeData;
                Assert.IsNotNull(cachedNodeData1);
                Assert.AreEqual(nodeTimestamp1, cachedNodeData1.NodeTimestamp);
                Assert.AreEqual(versionTimestamp1, cachedNodeData1.VersionTimestamp);

                // ACTION-2: Update
                root.Index++;
                root.Save();
                var nodeTimestamp2 = root.NodeTimestamp;
                var versionTimestamp2 = root.VersionTimestamp;

                // ASSERT-2: NodeData is refreshed in the cache after update,
                Assert.AreNotEqual(nodeTimestamp1, nodeTimestamp2);
                Assert.AreNotEqual(versionTimestamp1, versionTimestamp2);
                var cacheKey2 = DataStore.GenerateNodeDataVersionIdCacheKey(root.VersionId);
                if (cacheKey1 != cacheKey2)
                    Assert.Inconclusive("The test is invalid because the cache keys are not equal.");
                var item2 = DistributedApplication.Cache[cacheKey2];
                Assert.IsNotNull(item2);
                var cachedNodeData2 = item2 as NodeData;
                Assert.IsNotNull(cachedNodeData2);
                Assert.AreEqual(nodeTimestamp2, cachedNodeData2.NodeTimestamp);
                Assert.AreEqual(versionTimestamp2, cachedNodeData2.VersionTimestamp);

                return Task.FromResult(true);
            });
        }

        [TestMethod]
        public async Task MsSqlDP_LazyLoadedBigText()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var nearlyLongText = new string('a', DataStore.TextAlternationSizeLimit - 10);
                var longText = new string('c', DataStore.TextAlternationSizeLimit + 10);
                var descriptionPropertyType = ActiveSchema.PropertyTypes["Description"];

                // ACTION-1a: Creation with text that shorter than the magic limit
                var root = new SystemFolder(Repository.Root)
                    { Name = Guid.NewGuid().ToString(), Description = nearlyLongText };
                root.Save();
                // ACTION-1b: Load the node
                var loaded = (await DP.LoadNodesAsync(new[] { root.VersionId })).First();
                var longTextProps = loaded.GetDynamicData(false).LongTextProperties;
                var longTextPropType = longTextProps.First().Key;

                // ASSERT-1
                Assert.AreEqual("Description", longTextPropType.Name);

                // ACTION-2a: Update text property value in the database over the magic limit
                await TDP.UpdateDynamicPropertyAsync(loaded.VersionId, "Description", longText);
                // ACTION-2b: Load the node
                loaded = (await DP.LoadNodesAsync(new[] { root.VersionId })).First();
                longTextProps = loaded.GetDynamicData(false).LongTextProperties;

                // ASSERT-2
                Assert.AreEqual(0, longTextProps.Count);

                // ACTION-3: Load the property value
                DistributedApplication.Cache.Reset();
                root = Node.Load<SystemFolder>(root.Id);
                var lazyLoadedDescription = root.Description; // Loads the property value

                // ASSERT-3
                Assert.AreEqual(longText, lazyLoadedDescription);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_LazyLoadedBigTextVsCache()
        {
            await StorageTest(() =>
            {
                DataStore.Enabled = true;
                var nearlyLongText1 = new string('a', DataStore.TextAlternationSizeLimit - 10);
                var nearlyLongText2 = new string('b', DataStore.TextAlternationSizeLimit - 10);
                var longText = new string('c', DataStore.TextAlternationSizeLimit + 10);
                var descriptionPropertyType = ActiveSchema.PropertyTypes["Description"];

                // ACTION-1: Creation with text that shorter than the magic limit
                var root = new SystemFolder(Repository.Root)
                {
                    Name = Guid.NewGuid().ToString(),
                    Description = nearlyLongText1
                };
                root.Save();
                var cacheKey = DataStore.GenerateNodeDataVersionIdCacheKey(root.VersionId);

                // ASSERT-1: text property is in cache
                var cachedNodeData = (NodeData)DistributedApplication.Cache[cacheKey];
                Assert.IsTrue(cachedNodeData.IsShared);
                var longTextProperties = cachedNodeData.GetDynamicData(false).LongTextProperties;
                Assert.IsTrue(longTextProperties.ContainsKey(descriptionPropertyType));
                Assert.AreEqual(nearlyLongText1, (string)longTextProperties[descriptionPropertyType]);

                // ACTION-2: Update with text that shorter than the magic limit
                root = Node.Load<SystemFolder>(root.Id);
                root.Description = nearlyLongText2;
                root.Save();

                // ASSERT-2: text property is in cache
                cachedNodeData = (NodeData)DistributedApplication.Cache[cacheKey];
                Assert.IsTrue(cachedNodeData.IsShared);
                longTextProperties = cachedNodeData.GetDynamicData(false).LongTextProperties;
                Assert.IsTrue(longTextProperties.ContainsKey(descriptionPropertyType));
                Assert.AreEqual(nearlyLongText2, (string)longTextProperties[descriptionPropertyType]);

                // ACTION-3: Update with text that longer than the magic limit
                root = Node.Load<SystemFolder>(root.Id);
                root.Description = longText;
                root.Save();

                // ASSERT-3: text property is not in the cache
                cachedNodeData = (NodeData)DistributedApplication.Cache[cacheKey];
                Assert.IsTrue(cachedNodeData.IsShared);
                longTextProperties = cachedNodeData.GetDynamicData(false).LongTextProperties;
                Assert.IsFalse(longTextProperties.ContainsKey(descriptionPropertyType));

                // ACTION-4: Load the text property
                var loadedValue = root.Description;

                // ASSERT-4: Property is loaded and is in cache
                Assert.AreEqual(longText, loadedValue);
                cachedNodeData = (NodeData)DistributedApplication.Cache[cacheKey];
                Assert.IsTrue(cachedNodeData.IsShared);
                longTextProperties = cachedNodeData.GetDynamicData(false).LongTextProperties;
                Assert.IsTrue(longTextProperties.ContainsKey(descriptionPropertyType));

                return Task.CompletedTask;
            });
        }

        [TestMethod]
        public async Task MsSqlDP_LoadChildTypesToAllow()
        {
            Assert.Inconclusive();
        }
        [TestMethod]
        public async Task MsSqlDP_ContentListTypesInTree()
        {
            await StorageTest(async () =>
            {
                // ALIGN-1
                DataStore.Enabled = true;
                ActiveSchema.Reset();
                var contentLlistTypeCountBefore = ActiveSchema.ContentListTypes.Count;
                var root = CreateTestRoot();

                // ACTION-1
                var result1 = await DP.GetContentListTypesInTreeAsync(root.Path);

                // ASSERT-1
                Assert.IsNotNull(result1);
                Assert.AreEqual(0, result1.Count);
                Assert.AreEqual(contentLlistTypeCountBefore, ActiveSchema.ContentListTypes.Count);

                // ALIGN-2
                // Creation
                var node = new ContentList(root) { Name = "Survey-1" };
                node.Save();

                // ACTION-2
                var result2 = await DP.GetContentListTypesInTreeAsync(root.Path);

                // ASSERT
                Assert.AreEqual(contentLlistTypeCountBefore + 1, ActiveSchema.ContentListTypes.Count);
                Assert.IsNotNull(result2);
                Assert.AreEqual(1, result2.Count);
                Assert.AreEqual(ActiveSchema.ContentListTypes.Last().Id, result2[0].Id);
            });
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
            await StorageTest(() =>
            {
                DataStore.Enabled = true;

                var folderB = new SystemFolder(Repository.Root)
                {
                    Name = Guid.NewGuid().ToString(),
                    VersioningMode = VersioningType.MajorAndMinor
                };
                folderB.Save();
                for (int j = 0; j < 3; j++)
                {
                    for (int i = 0; i < 3; i++)
                    {
                        folderB.CheckOut();
                        folderB.Index++;
                        folderB.Save();
                        folderB.CheckIn();
                    }
                    folderB.Publish();
                }
                var allVersinsById = Node.GetVersionNumbers(folderB.Id);
                var allVersinsByPath = Node.GetVersionNumbers(folderB.Path);

                // Check
                var expected = new[] { "V0.1.D", "V0.2.D", "V0.3.D", "V1.0.A", "V1.1.D",
                        "V1.2.D", "V2.0.A", "V2.1.D", "V2.2.D", "V3.0.A" }
                    .Select(VersionNumber.Parse).ToArray();
                AssertSequenceEqual(expected, allVersinsById);
                AssertSequenceEqual(expected, allVersinsByPath);

                return Task.CompletedTask;
            });
        }
        [TestMethod]
        public async Task MsSqlDP_GetVersionNumbers_MissingNode()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                // ACTION
                var result = await DP.GetVersionNumbersAsync("/Root/Deleted");

                // ASSERT
                Assert.IsFalse(result.Any());
            });
        }

        [TestMethod]
        public async Task MsSqlDP_LoadBinaryPropertyValues()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                var root = CreateFolder(Repository.Root, "TestRoot");
                var file = CreateFile(root, "File-1.txt", "File content.");

                var versionId = file.VersionId;
                var fileId = file.Binary.FileId;
                var propertyTypeId = file.Binary.PropertyType.Id;

                // ACTION-1: Load existing
                var result = await DP.LoadBinaryPropertyValueAsync(versionId, propertyTypeId);
                // ASSERT-1
                Assert.IsNotNull(result);
                Assert.AreEqual("File-1", result.FileName.FileNameWithoutExtension);
                Assert.AreEqual(".txt", result.FileName.Extension);
                Assert.AreEqual(3L + "File content.".Length, result.Size); // +UTF-8 BOM
                Assert.AreEqual("text/plain", result.ContentType);

                // ACTION-2: Missing Binary
                result = await DP.LoadBinaryPropertyValueAsync(versionId, 999999);
                // ASSERT-2 (not loaded and no exceptin was thrown)
                Assert.IsNull(result);

                // ACTION-3: Staging
                await TDP.SetFileStagingAsync(fileId, true);
                result = await DP.LoadBinaryPropertyValueAsync(versionId, propertyTypeId);
                // ASSERT-3 (not loaded and no exceptin was thrown)
                Assert.IsNull(result);

                // ACTION-4: Missing File (inconsistent but need to be handled)
                await TDP.DeleteFileAsync(fileId);

                result = await DP.LoadBinaryPropertyValueAsync(versionId, propertyTypeId);
                // ASSERT-4 (not loaded and no exceptin was thrown)
                Assert.IsNull(result);
            });
        }

        [TestMethod]
        public async Task MsSqlDP_NodeEnumerator()
        {
            await StorageTest(() =>
            {
                DataStore.Enabled = true;

                // Create a small subtree
                var root = CreateTestRoot();
                var f1 = new SystemFolder(root) { Name = "F1" }; f1.Save();
                var f2 = new SystemFolder(root) { Name = "F2" }; f2.Save();
                var f3 = new SystemFolder(f1) { Name = "F3" }; f3.Save();
                var f4 = new SystemFolder(f1) { Name = "F4" }; f4.Save();
                var f5 = new SystemFolder(f3) { Name = "F5" }; f5.Save();
                var f6 = new SystemFolder(f3) { Name = "F6" }; f6.Save();

                // ACTION
                // Use ExecutionHint.ForceRelationalEngine for a valid dataprovider test case.
                var result = NodeEnumerator.GetNodes(root.Path, ExecutionHint.ForceRelationalEngine);

                // ASSERT
                var names = string.Join(", ", result.Select(n => n.Name));
                // preorder tree-walking
                Assert.AreEqual(root.Name + ", F1, F3, F5, F6, F4, F2", names);
                
                return Task.CompletedTask;
            });
        }

        [TestMethod]
        public async Task MsSqlDP_NameSuffix()
        {
            await StorageTest(() =>
            {
                DataStore.Enabled = true;

                // Create a small subtree
                var root = CreateTestRoot();
                var f1 = new SystemFolder(root) { Name = "folder(42)" }; f1.Save();

                // ACTION
                var newName = ContentNamingProvider.IncrementNameSuffixToLastName("folder(11)", f1.ParentId);

                // ASSERT
                Assert.AreEqual("folder(43)", newName);

                return Task.CompletedTask;
            });
        }

        [TestMethod]
        public async Task MsSqlDP_TreeSize_Root()
        {
            await IsolatedStorageTest(async () =>
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
            await IsolatedStorageTest(async () =>
            {
                DataStore.Enabled = true;

                var expectedFolderCount = CreateSafeContentQuery("+Type:Folder .COUNTONLY").Execute().Count;
                var expectedSystemFolderCount = CreateSafeContentQuery("+Type:SystemFolder .COUNTONLY").Execute().Count;
                var expectedAggregated = expectedFolderCount + expectedSystemFolderCount;

                var folderTypeTypeId = ActiveSchema.NodeTypes["Folder"].Id;
                var systemFolderTypeTypeId = ActiveSchema.NodeTypes["SystemFolder"].Id;

                // ACTION-1
                var actualFolderCount1 = await DP.InstanceCountAsync(new[] { folderTypeTypeId });
                var actualSystemFolderCount1 = await DP.InstanceCountAsync(new[] { systemFolderTypeTypeId });
                var actualAggregated1 = await DP.InstanceCountAsync(new[] { folderTypeTypeId, systemFolderTypeTypeId });

                // ASSERT
                Assert.AreEqual(expectedFolderCount, actualFolderCount1);
                Assert.AreEqual(expectedSystemFolderCount, actualSystemFolderCount1);
                Assert.AreEqual(expectedAggregated, actualAggregated1);

                // add a systemFolder to check inheritance in counts
                var folder = new SystemFolder(Repository.Root) { Name = "Folder-1" };
                folder.Save();

                // ACTION-1
                var actualFolderCount2 = await DP.InstanceCountAsync(new[] { folderTypeTypeId });
                var actualSystemFolderCount2 = await DP.InstanceCountAsync(new[] { systemFolderTypeTypeId });
                var actualAggregated2 = await DP.InstanceCountAsync(new[] { folderTypeTypeId, systemFolderTypeTypeId });

                // ASSERT
                Assert.AreEqual(expectedFolderCount, actualFolderCount2);
                Assert.AreEqual(expectedSystemFolderCount + 1, actualSystemFolderCount2);
                Assert.AreEqual(expectedAggregated + 1, actualAggregated2);

            });
        }
        [TestMethod]
        public async Task MsSqlDP_NodeQuery_ChildrenIdentfiers()
        {
            await IsolatedStorageTest(async () =>
            {
                DataStore.Enabled = true;

                var expected = CreateSafeContentQuery("+InFolder:/Root").Execute().Identifiers;

                // ACTION
                var result = await DP.GetChildrenIdentfiersAsync(Repository.Root.Id);

                // ASSERT
                AssertSequenceEqual(expected.OrderBy(x => x), result.OrderBy(x => x));
            });
        }

        [TestMethod]
        public async Task MsSqlDP_NodeQuery_QueryNodesByTypeAndPathAndName()
        {
            await IsolatedStorageTest(async () =>
            {
                DataStore.Enabled = true;

                var r = new SystemFolder(Repository.Root) { Name = "R" }; r.Save();
                var ra = new Folder(r) { Name = "A" }; ra.Save();
                var raf = new Folder(ra) { Name = "F" }; raf.Save();
                var rafa = new Folder(raf) { Name = "A" }; rafa.Save();
                var rafb = new Folder(raf) { Name = "B" }; rafb.Save();
                var ras = new SystemFolder(ra) { Name = "S" }; ras.Save();
                var rasa = new SystemFolder(ras) { Name = "A" }; rasa.Save();
                var rasb = new SystemFolder(ras) { Name = "B" }; rasb.Save();
                var rb = new Folder(r) { Name = "B" }; rb.Save();
                var rbf = new Folder(rb) { Name = "F" }; rbf.Save();
                var rbfa = new Folder(rbf) { Name = "A" }; rbfa.Save();
                var rbfb = new Folder(rbf) { Name = "B" }; rbfb.Save();
                var rbs = new SystemFolder(rb) { Name = "S" }; rbs.Save();
                var rbsa = new SystemFolder(rbs) { Name = "A" }; rbsa.Save();
                var rbsb = new SystemFolder(rbs) { Name = "B" }; rbsb.Save();
                var rc = new Folder(r) { Name = "C" }; rc.Save();
                var rcf = new Folder(rc) { Name = "F" }; rcf.Save();
                var rcfa = new Folder(rcf) { Name = "A" }; rcfa.Save();
                var rcfb = new Folder(rcf) { Name = "B" }; rcfb.Save();
                var rcs = new SystemFolder(rc) { Name = "S" }; rcs.Save();
                var rcsa = new SystemFolder(rcs) { Name = "A" }; rcsa.Save();
                var rcsb = new SystemFolder(rcs) { Name = "B" }; rcsb.Save();

                var typeF = ActiveSchema.NodeTypes["Folder"].Id;
                var typeS = ActiveSchema.NodeTypes["SystemFolder"].Id;

                // ACTION-1 (type: 1, path: 1, name: -)
                var nodeTypeIds = new[] { typeF };
                var pathStart = new[] { "/Root/R/A" };
                var result = await DP.QueryNodesByTypeAndPathAndNameAsync(nodeTypeIds, pathStart, true, null);
                // ASSERT-1
                var expected = CreateSafeContentQuery("+Type:Folder +InTree:/Root/R/A .SORT:Path")
                    .Execute().Identifiers.Skip(1).ToArray();
                Assert.AreEqual(3, expected.Length);
                AssertSequenceEqual(expected, result);

                // ACTION-2 (type: 2, path: 1, name: -)
                nodeTypeIds = new[] { typeF, typeS };
                pathStart = new[] { "/Root/R/A" };
                result = await DP.QueryNodesByTypeAndPathAndNameAsync(nodeTypeIds, pathStart, true, null);
                // ASSERT-2
                expected = CreateSafeContentQuery("+Type:(Folder SystemFolder) +InTree:/Root/R/A .SORT:Path")
                    .Execute().Identifiers.Skip(1).ToArray();
                Assert.AreEqual(6, expected.Length);
                AssertSequenceEqual(expected, result);

                // ACTION-3 (type: 1, path: 2, name: -)
                nodeTypeIds = new[] { typeF };
                pathStart = new[] { "/Root/R/A", "/Root/R/B" };
                result = await DP.QueryNodesByTypeAndPathAndNameAsync(nodeTypeIds, pathStart, true, null);
                // ASSERT-3
                expected = CreateSafeContentQuery("+Type:Folder +InTree:/Root/R/A .SORT:Path")
                    .Execute().Identifiers.Skip(1)
                    .Union(CreateSafeContentQuery("+Type:Folder +InTree:/Root/R/B .SORT:Path")
                        .Execute().Identifiers.Skip(1)).ToArray();
                Assert.AreEqual(6, expected.Length);
                AssertSequenceEqual(expected, result);

                // ACTION-4 (type: -, path: 1, name: A)
                pathStart = new[] { "/Root/R" };
                result = await DP.QueryNodesByTypeAndPathAndNameAsync(null, pathStart, true, "A");
                // ASSERT-4
                expected = CreateSafeContentQuery("+Name:A +InTree:/Root/R .SORT:Path").Execute().Identifiers.ToArray();
                Assert.AreEqual(7, expected.Length);
                AssertSequenceEqual(expected, result);
            });
        }
        [TestMethod]
        public async Task MsSqlDP_NodeQuery_QueryNodesByTypeAndPathAndProperty()
        {
            var contentType1 = "TestContent1";
            var contentType2 = "TestContent2";
            var ctd1 = $"<ContentType name='{contentType1}' parentType='SystemFolder'" + $@"
             handler='SenseNet.ContentRepository.SystemFolder'
             xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentTypeDefinition'>
  <AllowedChildTypes>Page,Folder,{contentType1},{contentType2}</AllowedChildTypes>
  <Fields>
    <Field name='Str' type='ShortText'/>
    <Field name='Int' type='Integer'/>
  </Fields>
</ContentType>
";
            var ctd2 = $"<ContentType name='{contentType2}' parentType='SystemFolder'" + $@"
             handler='SenseNet.ContentRepository.SystemFolder'
             xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentTypeDefinition'>
  <AllowedChildTypes>Page,Folder,{contentType1},{contentType2}</AllowedChildTypes>
  <Fields>
    <Field name='Str' type='ShortText'/>
    <Field name='Int' type='Integer'/>
  </Fields>
</ContentType>
";
            SystemFolder root = null;
            await IsolatedStorageTest(async () =>
            {
                try
                {
                    DataStore.Enabled = true;

                    ContentTypeInstaller.InstallContentType(ctd1, ctd2);
                    var unused = ContentType.GetByName(contentType1); // preload schema

                    root = new SystemFolder(Repository.Root) { Name = "R" }; root.Save();
                    var ra = new GenericContent(root, contentType1) { Name = "A", ["Int"] = 42, ["Str"] = "str1" }; ra.Save();
                    var raf = new GenericContent(ra, contentType1) { Name = "F" }; raf.Save();
                    var rafa = new GenericContent(raf, contentType1) { Name = "A", ["Int"] = 42, ["Str"] = "str1" }; rafa.Save();
                    var rafb = new GenericContent(raf, contentType1) { Name = "B", ["Int"] = 43, ["Str"] = "str2" }; rafb.Save();
                    var ras = new GenericContent(ra, contentType2) { Name = "S" }; ras.Save();
                    var rasa = new GenericContent(ras, contentType2) { Name = "A", ["Int"] = 42, ["Str"] = "str1" }; rasa.Save();
                    var rasb = new GenericContent(ras, contentType2) { Name = "B", ["Int"] = 43, ["Str"] = "str2" }; rasb.Save();
                    var rb = new GenericContent(root, contentType1) { Name = "B", ["Int"] = 43, ["Str"] = "str2" }; rb.Save();
                    var rbf = new GenericContent(rb, contentType1) { Name = "F" }; rbf.Save();
                    var rbfa = new GenericContent(rbf, contentType1) { Name = "A", ["Int"] = 42, ["Str"] = "str1" }; rbfa.Save();
                    var rbfb = new GenericContent(rbf, contentType1) { Name = "B", ["Int"] = 43, ["Str"] = "str2" }; rbfb.Save();
                    var rbs = new GenericContent(rb, contentType2) { Name = "S" }; rbs.Save();
                    var rbsa = new GenericContent(rbs, contentType2) { Name = "A", ["Int"] = 42, ["Str"] = "str1" }; rbsa.Save();
                    var rbsb = new GenericContent(rbs, contentType2) { Name = "B", ["Int"] = 43, ["Str"] = "str2" }; rbsb.Save();
                    var rc = new GenericContent(root, contentType1) { Name = "C" }; rc.Save();
                    var rcf = new GenericContent(rc, contentType1) { Name = "F" }; rcf.Save();
                    var rcfa = new GenericContent(rcf, contentType1) { Name = "A", ["Int"] = 42, ["Str"] = "str1" }; rcfa.Save();
                    var rcfb = new GenericContent(rcf, contentType1) { Name = "B", ["Int"] = 43, ["Str"] = "str2" }; rcfb.Save();
                    var rcs = new GenericContent(rc, contentType2) { Name = "S" }; rcs.Save();
                    var rcsa = new GenericContent(rcs, contentType2) { Name = "A", ["Int"] = 42, ["Str"] = "str1" }; rcsa.Save();
                    var rcsb = new GenericContent(rcs, contentType2) { Name = "B", ["Int"] = 43, ["Str"] = "str2" }; rcsb.Save();

                    var type1 = ActiveSchema.NodeTypes[contentType1].Id;
                    var type2 = ActiveSchema.NodeTypes[contentType2].Id;
                    var property1 = new List<QueryPropertyData>
                        {new QueryPropertyData {PropertyName = "Int", QueryOperator = Operator.Equal, Value = 42}};
                    var property2 = new List<QueryPropertyData>
                        {new QueryPropertyData {PropertyName = "Str", QueryOperator = Operator.Equal, Value = "str1"}};

                    // ACTION-1 (type: 1, path: 1, prop: -)
                    var result = await DP.QueryNodesByTypeAndPathAndPropertyAsync(new[] { type1 }, "/Root/R/A", true, null);
                    // ASSERT-1
                    // Skip(1) because the NodeQuery does not contein the subtree root.
                    var expected = CreateSafeContentQuery($"+Type:{contentType1} +InTree:/Root/R/A .SORT:Path")
                        .Execute().Identifiers.Skip(1).ToArray();
                    Assert.AreEqual(3, expected.Length);
                    AssertSequenceEqual(expected, result);

                    // ACTION-2 (type: 2, path: 1, prop: -)
                    result = await DP.QueryNodesByTypeAndPathAndPropertyAsync(new[] { type1, type2 }, "/Root/R/A", true, null);
                    // ASSERT-2
                    // Skip(1) because the NodeQuery does not contein the subtree root.
                    expected = CreateSafeContentQuery($"+Type:({contentType1} {contentType2}) +InTree:/Root/R/A .SORT:Path")
                        .Execute().Identifiers.Skip(1).ToArray();
                    Assert.AreEqual(6, expected.Length);
                    AssertSequenceEqual(expected, result);

                    // ACTION-3 (type: 1, path: 1, prop: Int:42)
                    result = await DP.QueryNodesByTypeAndPathAndPropertyAsync(new[] { type1 }, "/Root/R/A", true, property1);
                    // ASSERT-3
                    // Skip(1) because the NodeQuery does not contein the subtree root.
                    expected = CreateSafeContentQuery($"+Int:42 +InTree:/Root/R/A +Type:({contentType1}).SORT:Path")
                        .Execute().Identifiers.Skip(1).ToArray();
                    Assert.AreEqual(1, expected.Length);
                    AssertSequenceEqual(expected, result);

                    // ACTION-4 (type: -, path: 1,  prop: Int:42)
                    result = await DP.QueryNodesByTypeAndPathAndPropertyAsync(null, "/Root/R", true, property1);
                    // ASSERT-4
                    // Skip(1) is unnecessary because the subtree root is not a query hit.
                    expected = CreateSafeContentQuery("+Int:42 +InTree:/Root/R .SORT:Path").Execute().Identifiers.ToArray();
                    Assert.AreEqual(7, expected.Length);
                    AssertSequenceEqual(expected, result);

                    // ACTION-5 (type: 1, path: 1, prop: Str:"str1")
                    result = await DP.QueryNodesByTypeAndPathAndPropertyAsync(new[] { type1 }, "/Root/R/A", true, property2);
                    // ASSERT-5
                    // Skip(1) because the NodeQuery does not contein the subtree root.
                    expected = CreateSafeContentQuery($"+Str:str1 +InTree:/Root/R/A +Type:({contentType1}).SORT:Path")
                        .Execute().Identifiers.Skip(1).ToArray();
                    Assert.AreEqual(1, expected.Length);
                    AssertSequenceEqual(expected, result);

                    // ACTION-6 (type: -, path: 1,  prop: Str:"str2")
                    result = await DP.QueryNodesByTypeAndPathAndPropertyAsync(null, "/Root/R", true, property2);
                    // ASSERT-6
                    // Skip(1) is unnecessary because the subtree root is not a query hit.
                    expected = CreateSafeContentQuery("+Str:str1 +InTree:/Root/R .SORT:Path").Execute().Identifiers.ToArray();
                    Assert.AreEqual(7, expected.Length);
                    AssertSequenceEqual(expected, result);
                }
                finally
                {
                    root?.ForceDelete();
                    ContentTypeInstaller.RemoveContentType(contentType1);
                    ContentTypeInstaller.RemoveContentType(contentType2);
                }
            });
        }
        [TestMethod]
        public async Task MsSqlDP_NodeQuery_QueryNodesByReferenceAndType()

        {
            var contentType1 = "TestContent1";
            var contentType2 = "TestContent2";
            var ctd1 = $"<ContentType name='{contentType1}' parentType='SystemFolder'" + $@"
             handler='SenseNet.ContentRepository.SystemFolder'
             xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentTypeDefinition'>
  <AllowedChildTypes>Page,Folder,{contentType1},{contentType2}</AllowedChildTypes>
  <Fields>
    <Field name='Ref' type='Reference'/>
  </Fields>
</ContentType>
";
            var ctd2 = $"<ContentType name='{contentType2}' parentType='SystemFolder'" + $@"
             handler='SenseNet.ContentRepository.SystemFolder'
             xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentTypeDefinition'>
  <AllowedChildTypes>Page,Folder,{contentType1},{contentType2}</AllowedChildTypes>
  <Fields>
    <Field name='Ref' type='Reference'/>
  </Fields>
</ContentType>
";
            SystemFolder root = null;
            await IsolatedStorageTest(async () =>
            {
                try
                {
                    DataStore.Enabled = true;

                    ContentTypeInstaller.InstallContentType(ctd1, ctd2);
                    var unused = ContentType.GetByName(contentType1); // preload schema

                    root = new SystemFolder(Repository.Root) { Name = "TestRoot" }; root.Save();
                    var refs = new GenericContent(root, contentType1) { Name = "Refs" }; refs.Save();
                    var ref1 = new GenericContent(refs, contentType1) { Name = "R1" }; ref1.Save();
                    var ref2 = new GenericContent(refs, contentType2) { Name = "R2" }; ref2.Save();

                    var r1 = new NodeList<Node>(new[] { ref1.Id });
                    var r2 = new NodeList<Node>(new[] { ref2.Id });
                    var n1 = new GenericContent(root, contentType1) { Name = "N1", ["Ref"] = r1 }; n1.Save();
                    var n2 = new GenericContent(root, contentType1) { Name = "N2", ["Ref"] = r2 }; n2.Save();
                    var n3 = new GenericContent(root, contentType2) { Name = "N3", ["Ref"] = r1 }; n3.Save();
                    var n4 = new GenericContent(root, contentType2) { Name = "N4", ["Ref"] = r2 }; n4.Save();

                    var type1 = ActiveSchema.NodeTypes[contentType1].Id;
                    var type2 = ActiveSchema.NodeTypes[contentType2].Id;

                    // ACTION-1 (type: T1, ref: R1)
                    var result = await DP.QueryNodesByReferenceAndTypeAsync("Ref", ref1.Id, new[] { type1 });
                    // ASSERT-1
                    //((InMemorySearchEngine)Providers.Instance.SearchEngine).Index.Save("D:\\index-asdf.txt");
                    var expected = CreateSafeContentQuery($"+Type:{contentType1} +Ref:{ref1.Id} .SORT:Id")
                        .Execute().Identifiers.ToArray();
                    Assert.AreEqual(1, expected.Length);
                    AssertSequenceEqual(expected, result.OrderBy(x => x));

                    // ACTION-2 (type: T1,T2, ref: R1)
                    result = await DP.QueryNodesByReferenceAndTypeAsync("Ref", ref1.Id, new[] { type1, type2 });
                    // ASSERT-1
                    expected = CreateSafeContentQuery($"+Type:({contentType1} {contentType2}) +Ref:{ref1.Id} .SORT:Id")
                        .Execute().Identifiers.ToArray();
                    Assert.AreEqual(2, expected.Length);
                    AssertSequenceEqual(expected, result.OrderBy(x => x));

                }
                finally
                {
                    root?.ForceDelete();
                    ContentTypeInstaller.RemoveContentType(contentType1);
                    ContentTypeInstaller.RemoveContentType(contentType2);
                }
            });
        }

        /* ================================================================================================== TreeLock */

        [TestMethod]
        public async Task MsSqlDP_LoadEntityTree()
        {
            await StorageTest(async () =>
            {
                // ACTION
                var treeData = await DataStore.LoadEntityTreeAsync();

                // ASSERT check the right ordering: every node follows it's parent node.
                var tree = new Dictionary<int, EntityTreeNodeData>();
                foreach (var node in treeData)
                {
                    if (node.ParentId != 0)
                        if (!tree.ContainsKey(node.ParentId))
                            Assert.Fail($"The parent is not yet loaded. Id: {node.Id}, ParentId: {node.ParentId}");
                    tree.Add(node.Id, node);
                }
            });
        }

        /* ================================================================================================== TreeLock */

        [TestMethod]
        public async Task MsSqlDP_TreeLock()
        {
            await IsolatedStorageTest(async () =>
            {
                DataStore.Enabled = true;

                var path = "/Root/Folder-1";
                var childPath = "/root/folder-1/folder-2";
                var anotherPath = "/Root/Folder-2";
                var timeLimit = DateTime.UtcNow.AddHours(-8.0);

                // Pre check: there is no lock
                var tlocks = await DP.LoadAllTreeLocksAsync();
                Assert.AreEqual(0, tlocks.Count);

                // ACTION: create a lock
                var tlockId = await DP.AcquireTreeLockAsync(path, timeLimit);

                // Check: there is one lock ant it matches
                tlocks = await DP.LoadAllTreeLocksAsync();
                Assert.AreEqual(1, tlocks.Count);
                Assert.AreEqual(tlockId, tlocks.First().Key);
                Assert.AreEqual(path, tlocks.First().Value);

                // Check: path and subpath are locked
                Assert.IsTrue(await DP.IsTreeLockedAsync(path, timeLimit));
                Assert.IsTrue(await DP.IsTreeLockedAsync(childPath, timeLimit));

                // Check: outer path is not locked
                Assert.IsFalse(await DP.IsTreeLockedAsync(anotherPath, timeLimit));

                // ACTION: try to create a lock fot a subpath
                var childLlockId = await DP.AcquireTreeLockAsync(childPath, timeLimit);

                // Check: subPath cannot be locked
                Assert.AreEqual(0, childLlockId);

                // Check: there is still only one lock
                tlocks = await DP.LoadAllTreeLocksAsync();
                Assert.AreEqual(1, tlocks.Count);
                Assert.AreEqual(tlockId, tlocks.First().Key);
                Assert.AreEqual(path, tlocks.First().Value);

                // ACTION: Release the lock
                await DP.ReleaseTreeLockAsync(new[] { tlockId });

                // Check: there is no lock
                tlocks = await DP.LoadAllTreeLocksAsync();
                Assert.AreEqual(0, tlocks.Count);

                // Check: path and subpath are not locked
                Assert.IsFalse(await DP.IsTreeLockedAsync(path, timeLimit));
                Assert.IsFalse(await DP.IsTreeLockedAsync(childPath, timeLimit));

            });
        }

        /* ================================================================================================== IndexDocument */

        [TestMethod]
        public async Task MsSqlDP_LoadIndexDocuments()
        {
            const string fileContent = "File content.";
            const string testRootPath = "/Root/TestRoot";

            void CreateStructure()
            {
                var root = CreateFolder(Repository.Root, "TestRoot");
                var file1 = CreateFile(root, "File1", fileContent);
                var file2 = CreateFile(root, "File2", fileContent);
                var folder1 = CreateFolder(root, "Folder1");
                var file3 = CreateFile(folder1, "File3", fileContent);
                var file4 = CreateFile(folder1, "File4", fileContent);
                file4.CheckOut();
                var folder2 = CreateFolder(folder1, "Folder2");
                var fileA5 = CreateFile(folder2, "File5", fileContent);
                var fileA6 = CreateFile(folder2, "File6", fileContent);
            }

            await StorageTest(async () =>
            {
                var fileNodeType = ActiveSchema.NodeTypes["File"];
                var systemFolderType = ActiveSchema.NodeTypes["SystemFolder"];

                // ARRANGE
                DataStore.Enabled = true;
                CreateStructure();
                var testRoot = Node.Load<SystemFolder>(testRootPath);
                var testRootChildren = testRoot.Children.ToArray();

                // ACTION
                var oneVersion = await DP.LoadIndexDocumentsAsync(new[] { testRoot.VersionId });
                var moreVersions = (await DP.LoadIndexDocumentsAsync(testRootChildren.Select(x => x.VersionId).ToArray())).ToArray();
                var subTreeAll = DP.LoadIndexDocumentsAsync(testRootPath, new int[0]).ToArray();
                var onlyFiles = DP.LoadIndexDocumentsAsync(testRootPath, new[] { fileNodeType.Id }).ToArray();
                var onlySystemFolders = DP.LoadIndexDocumentsAsync(testRootPath, new[] { systemFolderType.Id }).ToArray();

                // ASSERT
                Assert.AreEqual(testRootPath, oneVersion.First().Path);
                Assert.AreEqual(3, moreVersions.Length);
                Assert.AreEqual(10, subTreeAll.Length);
                Assert.AreEqual(3, onlyFiles.Length);
                Assert.AreEqual(7, onlySystemFolders.Length);
            });
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
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;
                var newNode = CreateTestRoot();
                try
                {
                    var node = new SystemFolder(Repository.Root) { Name = newNode.Name };
                    var nodeData = node.Data;
                    nodeData.Path = RepositoryPath.Combine(node.ParentPath, node.Name);
                    var nodeHeadData = nodeData.GetNodeHeadData();
                    var versionData = nodeData.GetVersionData();
                    var dynamicData = nodeData.GetDynamicData(false);

                    // ACTION
                    await DP.InsertNodeAsync(nodeHeadData, versionData, dynamicData);
                    Assert.Fail("NodeAlreadyExistsException was not thrown.");
                }
                catch (NodeAlreadyExistsException)
                {
                    // ignored
                }
            });
        }

        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNode_Deleted()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;

                try
                {
                    var node = Node.LoadNode(Identifiers.PortalRootId);
                    node.Index++;
                    var nodeData = node.Data;
                    var nodeHeadData = nodeData.GetNodeHeadData();
                    var versionData = nodeData.GetVersionData();
                    var dynamicData = nodeData.GetDynamicData(false);
                    var versionIdsToDelete = new int[0];

                    // ACTION
                    nodeHeadData.NodeId = 99999;
                    await DP.UpdateNodeAsync(nodeHeadData, versionData, dynamicData, versionIdsToDelete);
                    Assert.Fail("ContentNotFoundException was not thrown.");
                }
                catch (ContentNotFoundException)
                {
                    // ignored
                }
            });
        }
        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNode_MissingVersion()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;
                var newNode =
                    new SystemFolder(Repository.Root) { Name = Guid.NewGuid().ToString(), Index = 42 };
                newNode.Save();

                try
                {
                    var node = Node.Load<SystemFolder>(newNode.Id);
                    node.Index++;
                    var nodeData = node.Data;
                    var nodeHeadData = nodeData.GetNodeHeadData();
                    var versionData = nodeData.GetVersionData();
                    var dynamicData = nodeData.GetDynamicData(false);
                    var versionIdsToDelete = new int[0];

                    // ACTION
                    versionData.VersionId = 99999;
                    await DP.UpdateNodeAsync(nodeHeadData, versionData, dynamicData, versionIdsToDelete);
                    Assert.Fail("ContentNotFoundException was not thrown.");
                }
                catch (ContentNotFoundException)
                {
                    // ignored
                }
            });
        }
        [TestMethod]
        public async Task MsSqlDP_Error_UpdateNode_OutOfDate()
        {
            await StorageTest(async () =>
            {
                DataStore.Enabled = true;
                var newNode =
                    new SystemFolder(Repository.Root) { Name = Guid.NewGuid().ToString(), Index = 42 };
                newNode.Save();

                try
                {
                    var node = Node.Load<SystemFolder>(newNode.Id);
                    node.Index++;
                    var nodeData = node.Data;
                    var nodeHeadData = nodeData.GetNodeHeadData();
                    var versionData = nodeData.GetVersionData();
                    var dynamicData = nodeData.GetDynamicData(false);
                    var versionIdsToDelete = new int[0];

                    // ACTION
                    nodeHeadData.Timestamp++;
                    await DP.UpdateNodeAsync(nodeHeadData, versionData, dynamicData, versionIdsToDelete);
                    Assert.Fail("NodeIsOutOfDateException was not thrown.");
                }
                catch (NodeIsOutOfDateException)
                {
                    // ignored
                }
            });
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
        private SystemFolder CreateFolder(Node parent, string name = null)
        {
            var folder = new SystemFolder(parent) { Name = name ?? Guid.NewGuid().ToString() };
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
