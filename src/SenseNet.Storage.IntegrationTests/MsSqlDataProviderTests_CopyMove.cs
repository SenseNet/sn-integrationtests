using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Storage;
using Task = System.Threading.Tasks.Task;

namespace SenseNet.Storage.IntegrationTests
{
    public partial class MsSqlDataProviderTests
    {

        #region ContentList Move Tests
        [TestMethod]
        public void MsSqlDP_Move_ContentList_LeafNodeToContentList()
        {
            MoveTest(testRoot =>
            {
                //1: MoveLeafNodeToContentList
                //Create [TestRoot]/SourceNode
                //Create [TestRoot]/TargetContentList
                //Move SourceNode, TargetContentList
                //Check: Node => Item
                //PrepareTest();
                EnsureNode(testRoot, "SourceNode");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceNode", "TargetContentList");
                CheckContentListItem2(testRoot, "TargetContentList/SourceNode");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_LeafNodeToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //2: MoveLeafNodeToContentListItem
                //Create [TestRoot]/SourceNode
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceNode, TargetItemFolder
                //Check: Node => Item
                //PrepareTest();
                EnsureNode(testRoot, "SourceNode");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceNode", "TargetContentList/TargetItemFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceNode");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_NodeTreeToContentList()
        {
            MoveTest(testRoot =>
            {
                //3: MoveNodeTreeToContentList
                //Create [TestRoot]/SourceFolder/SourceNode
                //Create [TestRoot]/TargetContentList
                //Move SourceFolder, TargetContentList
                //Check: NodeTree => ItemTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceFolder/SourceNode");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceFolder", "TargetContentList");
                CheckContentListItem2(testRoot, "TargetContentList/SourceFolder");
                CheckContentListItem2(testRoot, "TargetContentList/SourceFolder/SourceNode");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_NodeTreeToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //4: MoveNodeTreeToContentListItem
                //Create [TestRoot]/SourceFolder/SourceNode
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceFolder, TargetItemFolder
                //Check: NodeTree => ItemTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceFolder/SourceNode");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceFolder", "TargetContentList/TargetItemFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceFolder/SourceNode");
            });
        }
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_NodeWithContentListToNode()
        //{
        //    //5: MoveNodeWithContentListToNode
        //    //Create [TestRoot]/SourceFolder/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetFolder
        //    //Move SourceFolder, TargetFolder
        //    //Check: Unchanged contentlist and item
        //    PrepareTest();
        //    EnsureNode(testRoot, "SourceFolder/SourceContentList/SourceContentListItem");
        //    EnsureNode(testRoot, "TargetFolder");
        //    MoveNode(testRoot, "SourceFolder", "TargetFolder");
        //    CheckSimpleNode(testRoot, "TargetFolder/SourceFolder");
        //    CheckContentList1(testRoot, "TargetFolder/SourceFolder/SourceContentList");
        //    CheckContentListItem1(testRoot, "TargetFolder/SourceFolder/SourceContentList/SourceContentListItem");
        //}
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_NodeWithContentListToContentList()
        {
            MoveTest(testRoot =>
            {
                //6: MoveNodeWithContentListToContentList
                //Create [TestRoot]/SourceFolder/SourceContentList/SourceContentListItem
                //Create [TestRoot]/TargetContentList
                //Move SourceFolder, TargetContentList
                //Check: exception
                //PrepareTest();
                EnsureNode(testRoot, "SourceFolder/SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceFolder", "TargetContentList");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_NodeWithContentListToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //7: MoveNodeWithContentListToContentListItem
                //Create [TestRoot]/SourceFolder/SourceContentList/SourceContentListItem
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceFolder, TargetItemFolder
                //Check: exception
                //PrepareTest();
                EnsureNode(testRoot, "SourceFolder/SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceFolder", "TargetContentList/TargetItemFolder");
            });
        }
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListToNode()
        //{
        //    //8: MoveContentListToNode
        //    //Create [TestRoot]/SourceContentList
        //    //Create [TestRoot]/TargetFolder
        //    //Move SourceContentList, TargetFolder
        //    //Check: ok
        //    PrepareTest();
        //    EnsureNode(testRoot, "SourceContentList");
        //    EnsureNode(testRoot, "TargetFolder");
        //    MoveNode(testRoot, "SourceContentList", "TargetFolder");
        //    CheckContentList1(testRoot, "TargetFolder/SourceContentList");
        //}
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListToContentList()
        {
            MoveTest(testRoot =>
            {
                //9: MoveContentListToContentList
                //Create [TestRoot]/SourceContentList
                //Create [TestRoot]/TargetContentList
                //Move SourceContentList, TargetContentList
                //Check: exception
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceContentList", "TargetContentList");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //10: MoveContentListToContentListItem
                //Create [TestRoot]/SourceContentList
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceContentList, TargetItemFolder
                //Check: exception
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceContentList", "TargetContentList/TargetItemFolder");
            });
        }
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListTreeToNode()
        //{
        //    //11: MoveContentListTreeToNode
        //    //Create [TestRoot]/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetFolder
        //    //Move SourceContentList, TargetFolder
        //    //Check: ok
        //    PrepareTest();
        //    EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
        //    EnsureNode(testRoot, "TargetFolder");
        //    MoveNode(testRoot, "SourceContentList", "TargetFolder");
        //    CheckContentList1(testRoot, "TargetFolder/SourceContentList");
        //    CheckContentListItem1(testRoot, "TargetFolder/SourceContentList/SourceContentListItem");
        //}
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListTreeToContentList()
        {
            MoveTest(testRoot =>
            {
                //12: MoveContentListTreeToContentList
                //Create [TestRoot]/SourceContentList/SourceContentListItem
                //Create [TestRoot]/TargetContentList
                //Move SourceContentList, TargetContentList
                //Check: exception
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceContentList", "TargetContentList");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListTreeToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //13: MoveContentListTreeToContentListItem
                //Create [TestRoot]/SourceContentList/SourceContentListItem
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceContentList, TargetItemFolder
                //Check: exception
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceContentList", "TargetContentList/TargetItemFolder");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToNode()
        {
            MoveTest(testRoot =>
            {
                //14: MoveContentListItemToNode
                //Create [TestRoot]/SourceContentList/SourceContentListItem
                //Create [TestRoot]/TargetFolder
                //Move SourceContentListItem, TargetFolder
                //Check: Item => Node
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode(testRoot, "SourceContentList/SourceContentListItem", "TargetFolder");
                CheckSimpleNode(testRoot, "TargetFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToContentList()
        {
            MoveTest(testRoot =>
            {
                //15: MoveContentListItemToContentList
                //Create [TestRoot]/SourceContentList/SourceContentListItem
                //Create [TestRoot]/TargetContentList
                //Move SourceContentListItem, TargetContentList
                //Check: Item => Item
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceContentList/SourceContentListItem", "TargetContentList");
                CheckContentListItem2(testRoot, "TargetContentList/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //16: MoveContentListItemToContentListItem
                //Create [TestRoot]/SourceContentList/SourceContentListItem
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceContentListItem, TargetItemFolder
                //Check: Item => Item
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceContentList/SourceContentListItem", "TargetContentList/TargetItemFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTreeToNode()
        {
            MoveTest(testRoot =>
            {
                //17: MoveContentListItemTreeToNode
                //Create [TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem
                //Create [TestRoot]/TargetFolder
                //Move SourceItemFolder, TargetFolder
                //Check: ItemTree => NodeTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode(testRoot, "SourceContentList/SourceItemFolder", "TargetFolder");
                CheckSimpleNode(testRoot, "TargetFolder/SourceItemFolder");
                CheckSimpleNode(testRoot, "TargetFolder/SourceItemFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTreeToContentList()
        {
            MoveTest(testRoot =>
            {
                //18: MoveContentListItemTreeToContentList
                //Create [TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem
                //Create [TestRoot]/TargetContentList
                //Move SourceItemFolder, TargetContentList
                //Check: ItemTree => ItemTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceContentList/SourceItemFolder", "TargetContentList");
                CheckContentListItem2(testRoot, "TargetContentList/SourceItemFolder");
                CheckContentListItem2(testRoot, "TargetContentList/SourceItemFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTreeToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //19: MoveContentListItemTreeToContentListItem
                //Create [TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceItemFolder, TargetItemFolder
                //Check: ItemTree => ItemTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceContentList/SourceItemFolder", "TargetContentList/TargetItemFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceItemFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceItemFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTree2ToNode()
        {
            MoveTest(testRoot =>
            {
                //20: MoveContentListItemTree2ToNode
                //Create [TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
                //Create [TestRoot]/TargetFolder
                //Move SourceItemFolder2, TargetFolder
                //Check: ItemTree => NodeTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2", "TargetFolder");
                CheckSimpleNode(testRoot, "TargetFolder/SourceItemFolder2");
                CheckSimpleNode(testRoot, "TargetFolder/SourceItemFolder2/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTree2ToContentList()
        {
            MoveTest(testRoot =>
            {
                //21: MoveContentListItemTree2ToContentList
                //Create [TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
                //Create [TestRoot]/TargetContentList
                //Move SourceItemFolder2, TargetContentList
                //Check: ItemTree => ItemTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2", "TargetContentList");
                CheckContentListItem2(testRoot, "TargetContentList/SourceItemFolder2");
                CheckContentListItem2(testRoot, "TargetContentList/SourceItemFolder2/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTree2ToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //22: MoveContentListItemTree2ToContentListItem
                //Create [TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
                //Create [TestRoot]/TargetContentList/TargetItemFolder
                //Move SourceItemFolder2, TargetItemFolder
                //Check: ItemTree => ItemTree
                //PrepareTest();
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2", "TargetContentList/TargetItemFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceItemFolder2");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceItemFolder2/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToSameContentList()
        {
            MoveTest(testRoot =>
            {
                //23: MoveContentListItemToSameContentList
                //Create [TestRoot]/ContentList/SourceItemFolder/SourceContentListItem
                //Create [TestRoot]/ContentList
                //Move SourceContentListItem, SourceContentList
                //Check: 
                //PrepareTest();
                EnsureNode(testRoot, "ContentList/SourceItemFolder/SourceContentListItem");
                //EnsureNode(testRoot, "ContentList");
                MoveNode(testRoot, "ContentList/SourceItemFolder/SourceContentListItem", "ContentList");
                CheckContentListItem1(testRoot, "ContentList/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToSameContentListItem()
        {
            MoveTest(testRoot =>
            {
                //24: MoveContentListItemToSameContentListItem
                //Create [TestRoot]/ContentList/SourceItemFolder/SourceContentListItem
                //Create [TestRoot]/ContentList/TargetItemFolder
                //Move SourceContentListItem, TargetItemFolder
                //Check: 
                //PrepareTest();
                EnsureNode(testRoot, "ContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "ContentList/TargetItemFolder");
                MoveNode(testRoot, "ContentList/SourceItemFolder/SourceContentListItem", "ContentList/TargetItemFolder");
                CheckContentListItem1(testRoot, "ContentList/TargetItemFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTreeToSameContentList()
        {
            MoveTest(testRoot =>
            {
                //25: MoveContentListItemTreeToSameContentList
                //Create [TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
                //Create [TestRoot]/ContentList
                //Move SourceItemFolder2, SourceContentList
                //Check: 
                //PrepareTest();
                EnsureNode(testRoot, "ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                //EnsureNode(testRoot, "ContentList");
                MoveNode(testRoot, "ContentList/SourceItemFolder1/SourceItemFolder2", "ContentList");
                CheckContentListItem1(testRoot, "ContentList/SourceItemFolder2");
                CheckContentListItem1(testRoot, "ContentList/SourceItemFolder2/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTreeToSameContentListItem()
        {
            MoveTest(testRoot =>
            {
                //26: MoveContentListItemTreeToSameContentListItem
                //Create [TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
                //Create [TestRoot]/ContentList/TargetItemFolder
                //Move SourceItemFolder2, TargetItemFolder
                //Check: 
                //PrepareTest();
                EnsureNode(testRoot, "ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "ContentList/TargetItemFolder");
                MoveNode(testRoot, "ContentList/SourceItemFolder1/SourceItemFolder2", "ContentList/TargetItemFolder");
                CheckContentListItem1(testRoot, "ContentList/TargetItemFolder/SourceItemFolder2");
                CheckContentListItem1(testRoot, "ContentList/TargetItemFolder/SourceItemFolder2/SourceContentListItem");
            });
        }
        #endregion

        /* ============================================================================================== Tools */

        private void MoveTest(Action<SystemFolder> callback)
        {
            try
            {
                StorageTest(() =>
                {
                    if (ContentType.GetByName("Car") == null)
                        InstallCarContentType();
                    var testRoot = CreateTestRoot();
                    try
                    {
                        callback(testRoot);
                    }
                    finally
                    {

                    }
                    return Task.CompletedTask;
                }).Wait();
            }
            catch (AggregateException e)
            {
                if (e.InnerException == null)
                    throw;
                throw e.InnerException;
            }
        }

        #region Tools
        private void MoveNode(SystemFolder testRoot, string encodedSourcePath, string encodedTargetPath, bool clearTarget = false)
        {
            string sourcePath = DecodePath(testRoot, encodedSourcePath);
            string targetPath = DecodePath(testRoot, encodedTargetPath);
            int sourceId = Node.LoadNode(sourcePath).Id;
            int targetId = Node.LoadNode(targetPath).Id;

            //make sure target does not contain the source node
            if (clearTarget)
            {
                var sourceName = RepositoryPath.GetFileNameSafe(sourcePath);
                if (!string.IsNullOrEmpty(sourceName))
                {
                    var targetPathWithName = RepositoryPath.Combine(targetPath, sourceName);
                    if (Node.Exists(targetPathWithName))
                        Node.ForceDelete(targetPathWithName);
                }
            }

            Node.Move(sourcePath, targetPath);

            Node parentNode = Node.LoadNode(targetId);
            Node childNode = Node.LoadNode(sourceId);
            Assert.IsTrue(childNode.ParentId == parentNode.Id, "Source was not moved.");
        }

        private void CheckSimpleNode(SystemFolder testRoot, string relativePath)
        {
            Node node = LoadNode(testRoot, relativePath);
            Assert.IsTrue(node.ContentListId == 0, "ContentListId is not 0");
            Assert.IsNull(node.ContentListType, "ContentListType is not null");
        }
        private void CheckContentList1(SystemFolder testRoot, string relativePath)
        {
            ContentList contentlist = Node.Load<ContentList>(DecodePath(testRoot, relativePath));
            Assert.IsTrue(contentlist.ContentListId == 0, "ContentListId is not 0");
            Assert.IsNotNull(contentlist.ContentListType, "ContentListType is null");
            Assert.IsTrue(contentlist.ContentListDefinition == _listDef1);
        }
        private void CheckContentList2(SystemFolder testRoot, string relativePath)
        {
            ContentList contentlist = Node.Load<ContentList>(DecodePath(testRoot, relativePath));
            Assert.IsTrue(contentlist.ContentListId == 0, "ContentListId is not 0");
            Assert.IsNotNull(contentlist.ContentListType, "ContentListType is null");
            Assert.IsTrue(contentlist.ContentListDefinition == _listDef2);
        }
        private void CheckContentListItem1(SystemFolder testRoot, string relativePath)
        {
            Node node = LoadNode(testRoot, relativePath);
            Assert.IsTrue(node.HasProperty("#String_0"), "ContentListItem has not property: #String_0");
            Assert.IsNotNull(node.ContentListType, "ContentListItem ContentListType == null");
            Assert.IsTrue(node.ContentListId > 0, "ContentListItem ContentListId == 0");
        }
        private void CheckContentListItem2(SystemFolder testRoot, string relativePath)
        {
            Node node = LoadNode(testRoot, relativePath);
            Assert.IsTrue(node.HasProperty("#Int_0"), "ContentListItem has not property: #Int_0");
            Assert.IsNotNull(node.ContentListType, "ContentListItem ContentListType == null");
            Assert.IsTrue(node.ContentListId > 0, "ContentListItem ContentListId == 0");
        }

        private void EnsureNode(SystemFolder testRoot, string relativePath)
        {
            string path = DecodePath(testRoot, relativePath);
            if (Node.Exists(path))
                return;

            string name = RepositoryPath.GetFileName(path);
            string parentPath = RepositoryPath.GetParentPath(path);
            EnsureNode(testRoot, parentPath);

            switch (name)
            {
                case "ContentList":
                case "SourceContentList":
                    CreateContentList(parentPath, name, _listDef1);
                    break;
                case "TargetContentList":
                    CreateContentList(parentPath, name, _listDef2);
                    break;
                case "Folder":
                case "Folder1":
                case "Folder2":
                case "SourceFolder":
                case "SourceItemFolder":
                case "SourceItemFolder1":
                case "SourceItemFolder2":
                case "TargetFolder":
                case "TargetFolder1":
                case "TargetFolder2":
                case "TargetItemFolder":
                    CreateNode(parentPath, name, "Folder");
                    break;

                case "(apps)":
                case "SystemFolder":
                case "SystemFolder1":
                case "SystemFolder2":
                case "SystemFolder3":
                    CreateNode(parentPath, name, "SystemFolder");
                    break;

                case "SourceContentListItem":
                case "SourceNode":
                    CreateNode(parentPath, name, "Car");
                    break;
                default:
                    CreateNode(parentPath, name, "Car");
                    break;
            }
        }
        private Node LoadNode(SystemFolder testRoot, string relativePath)
        {
            return Node.LoadNode(DecodePath(testRoot, relativePath));
        }
        private void CreateContentList(string parentPath, string name, string listDef)
        {
            Node parent = Node.LoadNode(parentPath);
            ContentList contentlist = new ContentList(parent);
            contentlist.Name = name;
            contentlist.ContentListDefinition = listDef;
            contentlist.AllowChildTypes(new[] { "Folder", "Car" });
            contentlist.Save();
        }
        private void CreateNode(string parentPath, string name, string typeName)
        {
            Content parent = Content.Load(parentPath);
            Content content = Content.CreateNew(typeName, parent.ContentHandler, name);
            if (typeName != "SystemFolder" && typeName != "Folder" && typeName != "Page")
                ((GenericContent)content.ContentHandler).AllowChildTypes(new[] { "Folder", "ContentList", "Car" });
            content.Save();
        }
        private string DecodePath(SystemFolder testRoot, string relativePath)
        {
            if (relativePath.StartsWith("/Root/"))
                return relativePath;
            if(relativePath.StartsWith("[TestRoot]"))
                Assert.Fail("[TestRoot] is not allowed. Use relative path instead.");
            return RepositoryPath.Combine(testRoot.Path, relativePath);
        }
        #endregion

        /* ==============================================================================================  */

        #region ListDefs
        private static readonly string _listDef1 = @"<?xml version='1.0' encoding='utf-8'?>
<ContentListDefinition xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentListDefinition'>
	<DisplayName>Cars title</DisplayName>
	<Description>Cars description</Description>
	<Icon>automobile.gif</Icon>
	<Fields>
		<ContentListField name='#ContentListField1' type='ShortText'>
			<DisplayName>ContentListField1</DisplayName>
			<Description>ContentListField1 Description</Description>
			<Icon>icon.gif</Icon>
			<Configuration>
				<MaxLength>100</MaxLength>
			</Configuration>
		</ContentListField>
		<ContentListField name='#ContentListField2' type='WhoAndWhen'>
			<DisplayName>ContentListField2</DisplayName>
			<Description>ContentListField2 Description</Description>
			<Icon>icon.gif</Icon>
			<Configuration>
			</Configuration>
		</ContentListField>
		<ContentListField name='#ContentListField3' type='ShortText'>
			<DisplayName>ContentListField3</DisplayName>
			<Description>ContentListField3 Description</Description>
			<Icon>icon.gif</Icon>
			<Configuration>
				<MaxLength>200</MaxLength>
			</Configuration>
		</ContentListField>
	</Fields>
</ContentListDefinition>
";
        private static readonly string _listDef2 = @"<?xml version='1.0' encoding='utf-8'?>
<ContentListDefinition xmlns='http://schemas.sensenet.com/SenseNet/ContentRepository/ContentListDefinition'>
	<DisplayName>Trucks title</DisplayName>
	<Description>Trucks description</Description>
	<Icon>automobile.gif</Icon>
	<Fields>
		<ContentListField name='#ContentListField1' type='Integer' />
		<!--<ContentListField name='#ContentListField2' type='Number' />-->
	</Fields>
</ContentListDefinition>
";
        #endregion

    }
}
