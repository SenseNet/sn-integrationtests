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
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_LeafNodeToContentListItem()
        //{
        //    //2: MoveLeafNodeToContentListItem
        //    //Create [TestRoot]/SourceNode
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceNode, TargetItemFolder
        //    //Check: Node => Item
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceNode");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceNode", "[TestRoot]/TargetContentList/TargetItemFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceNode");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_NodeTreeToContentList()
        //{
        //    //3: MoveNodeTreeToContentList
        //    //Create [TestRoot]/SourceFolder/SourceNode
        //    //Create [TestRoot]/TargetContentList
        //    //Move SourceFolder, TargetContentList
        //    //Check: NodeTree => ItemTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceFolder/SourceNode");
        //    EnsureNode("[TestRoot]/TargetContentList");
        //    MoveNode("[TestRoot]/SourceFolder", "[TestRoot]/TargetContentList");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/SourceFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/SourceFolder/SourceNode");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_NodeTreeToContentListItem()
        //{
        //    //4: MoveNodeTreeToContentListItem
        //    //Create [TestRoot]/SourceFolder/SourceNode
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceFolder, TargetItemFolder
        //    //Check: NodeTree => ItemTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceFolder/SourceNode");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceFolder", "[TestRoot]/TargetContentList/TargetItemFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceFolder/SourceNode");
        //}
        ////[TestMethod]
        ////public void MsSqlDP_Move_ContentList_NodeWithContentListToNode()
        ////{
        ////    //5: MoveNodeWithContentListToNode
        ////    //Create [TestRoot]/SourceFolder/SourceContentList/SourceContentListItem
        ////    //Create [TestRoot]/TargetFolder
        ////    //Move SourceFolder, TargetFolder
        ////    //Check: Unchanged contentlist and item
        ////    PrepareTest();
        ////    EnsureNode("[TestRoot]/SourceFolder/SourceContentList/SourceContentListItem");
        ////    EnsureNode("[TestRoot]/TargetFolder");
        ////    MoveNode("[TestRoot]/SourceFolder", "[TestRoot]/TargetFolder");
        ////    CheckSimpleNode("[TestRoot]/TargetFolder/SourceFolder");
        ////    CheckContentList1("[TestRoot]/TargetFolder/SourceFolder/SourceContentList");
        ////    CheckContentListItem1("[TestRoot]/TargetFolder/SourceFolder/SourceContentList/SourceContentListItem");
        ////}
        //[TestMethod]
        //[ExpectedException(typeof(InvalidOperationException))]
        //public void MsSqlDP_Move_ContentList_NodeWithContentListToContentList()
        //{
        //    //6: MoveNodeWithContentListToContentList
        //    //Create [TestRoot]/SourceFolder/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList
        //    //Move SourceFolder, TargetContentList
        //    //Check: exception
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceFolder/SourceContentList/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList");
        //    MoveNode("[TestRoot]/SourceFolder", "[TestRoot]/TargetContentList");
        //}
        //[TestMethod]
        //[ExpectedException(typeof(InvalidOperationException))]
        //public void MsSqlDP_Move_ContentList_NodeWithContentListToContentListItem()
        //{
        //    //7: MoveNodeWithContentListToContentListItem
        //    //Create [TestRoot]/SourceFolder/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceFolder, TargetItemFolder
        //    //Check: exception
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceFolder/SourceContentList/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceFolder", "[TestRoot]/TargetContentList/TargetItemFolder");
        //}
        ////[TestMethod]
        ////public void MsSqlDP_Move_ContentList_ContentListToNode()
        ////{
        ////    //8: MoveContentListToNode
        ////    //Create [TestRoot]/SourceContentList
        ////    //Create [TestRoot]/TargetFolder
        ////    //Move SourceContentList, TargetFolder
        ////    //Check: ok
        ////    PrepareTest();
        ////    EnsureNode("[TestRoot]/SourceContentList");
        ////    EnsureNode("[TestRoot]/TargetFolder");
        ////    MoveNode("[TestRoot]/SourceContentList", "[TestRoot]/TargetFolder");
        ////    CheckContentList1("[TestRoot]/TargetFolder/SourceContentList");
        ////}
        //[TestMethod]
        //[ExpectedException(typeof(InvalidOperationException))]
        //public void MsSqlDP_Move_ContentList_ContentListToContentList()
        //{
        //    //9: MoveContentListToContentList
        //    //Create [TestRoot]/SourceContentList
        //    //Create [TestRoot]/TargetContentList
        //    //Move SourceContentList, TargetContentList
        //    //Check: exception
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList");
        //    EnsureNode("[TestRoot]/TargetContentList");
        //    MoveNode("[TestRoot]/SourceContentList", "[TestRoot]/TargetContentList");
        //}
        //[TestMethod]
        //[ExpectedException(typeof(InvalidOperationException))]
        //public void MsSqlDP_Move_ContentList_ContentListToContentListItem()
        //{
        //    //10: MoveContentListToContentListItem
        //    //Create [TestRoot]/SourceContentList
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceContentList, TargetItemFolder
        //    //Check: exception
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceContentList", "[TestRoot]/TargetContentList/TargetItemFolder");
        //}
        ////[TestMethod]
        ////public void MsSqlDP_Move_ContentList_ContentListTreeToNode()
        ////{
        ////    //11: MoveContentListTreeToNode
        ////    //Create [TestRoot]/SourceContentList/SourceContentListItem
        ////    //Create [TestRoot]/TargetFolder
        ////    //Move SourceContentList, TargetFolder
        ////    //Check: ok
        ////    PrepareTest();
        ////    EnsureNode("[TestRoot]/SourceContentList/SourceContentListItem");
        ////    EnsureNode("[TestRoot]/TargetFolder");
        ////    MoveNode("[TestRoot]/SourceContentList", "[TestRoot]/TargetFolder");
        ////    CheckContentList1("[TestRoot]/TargetFolder/SourceContentList");
        ////    CheckContentListItem1("[TestRoot]/TargetFolder/SourceContentList/SourceContentListItem");
        ////}
        //[TestMethod]
        //[ExpectedException(typeof(InvalidOperationException))]
        //public void MsSqlDP_Move_ContentList_ContentListTreeToContentList()
        //{
        //    //12: MoveContentListTreeToContentList
        //    //Create [TestRoot]/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList
        //    //Move SourceContentList, TargetContentList
        //    //Check: exception
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList");
        //    MoveNode("[TestRoot]/SourceContentList", "[TestRoot]/TargetContentList");
        //}
        //[TestMethod]
        //[ExpectedException(typeof(InvalidOperationException))]
        //public void MsSqlDP_Move_ContentList_ContentListTreeToContentListItem()
        //{
        //    //13: MoveContentListTreeToContentListItem
        //    //Create [TestRoot]/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceContentList, TargetItemFolder
        //    //Check: exception
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceContentList", "[TestRoot]/TargetContentList/TargetItemFolder");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemToNode()
        //{
        //    //14: MoveContentListItemToNode
        //    //Create [TestRoot]/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetFolder
        //    //Move SourceContentListItem, TargetFolder
        //    //Check: Item => Node
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetFolder");
        //    MoveNode("[TestRoot]/SourceContentList/SourceContentListItem", "[TestRoot]/TargetFolder");
        //    CheckSimpleNode("[TestRoot]/TargetFolder/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemToContentList()
        //{
        //    //15: MoveContentListItemToContentList
        //    //Create [TestRoot]/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList
        //    //Move SourceContentListItem, TargetContentList
        //    //Check: Item => Item
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList");
        //    MoveNode("[TestRoot]/SourceContentList/SourceContentListItem", "[TestRoot]/TargetContentList");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemToContentListItem()
        //{
        //    //16: MoveContentListItemToContentListItem
        //    //Create [TestRoot]/SourceContentList/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceContentListItem, TargetItemFolder
        //    //Check: Item => Item
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceContentList/SourceContentListItem", "[TestRoot]/TargetContentList/TargetItemFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTreeToNode()
        //{
        //    //17: MoveContentListItemTreeToNode
        //    //Create [TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem
        //    //Create [TestRoot]/TargetFolder
        //    //Move SourceItemFolder, TargetFolder
        //    //Check: ItemTree => NodeTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetFolder");
        //    MoveNode("[TestRoot]/SourceContentList/SourceItemFolder", "[TestRoot]/TargetFolder");
        //    CheckSimpleNode("[TestRoot]/TargetFolder/SourceItemFolder");
        //    CheckSimpleNode("[TestRoot]/TargetFolder/SourceItemFolder/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTreeToContentList()
        //{
        //    //18: MoveContentListItemTreeToContentList
        //    //Create [TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList
        //    //Move SourceItemFolder, TargetContentList
        //    //Check: ItemTree => ItemTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList");
        //    MoveNode("[TestRoot]/SourceContentList/SourceItemFolder", "[TestRoot]/TargetContentList");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/SourceItemFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/SourceItemFolder/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTreeToContentListItem()
        //{
        //    //19: MoveContentListItemTreeToContentListItem
        //    //Create [TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceItemFolder, TargetItemFolder
        //    //Check: ItemTree => ItemTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceItemFolder/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceContentList/SourceItemFolder", "[TestRoot]/TargetContentList/TargetItemFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceItemFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceItemFolder/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTree2ToNode()
        //{
        //    //20: MoveContentListItemTree2ToNode
        //    //Create [TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
        //    //Create [TestRoot]/TargetFolder
        //    //Move SourceItemFolder2, TargetFolder
        //    //Check: ItemTree => NodeTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetFolder");
        //    MoveNode("[TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2", "[TestRoot]/TargetFolder");
        //    CheckSimpleNode("[TestRoot]/TargetFolder/SourceItemFolder2");
        //    CheckSimpleNode("[TestRoot]/TargetFolder/SourceItemFolder2/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTree2ToContentList()
        //{
        //    //21: MoveContentListItemTree2ToContentList
        //    //Create [TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList
        //    //Move SourceItemFolder2, TargetContentList
        //    //Check: ItemTree => ItemTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList");
        //    MoveNode("[TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2", "[TestRoot]/TargetContentList");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/SourceItemFolder2");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/SourceItemFolder2/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTree2ToContentListItem()
        //{
        //    //22: MoveContentListItemTree2ToContentListItem
        //    //Create [TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
        //    //Create [TestRoot]/TargetContentList/TargetItemFolder
        //    //Move SourceItemFolder2, TargetItemFolder
        //    //Check: ItemTree => ItemTree
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
        //    EnsureNode("[TestRoot]/TargetContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/SourceContentList/SourceItemFolder1/SourceItemFolder2", "[TestRoot]/TargetContentList/TargetItemFolder");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceItemFolder2");
        //    CheckContentListItem2("[TestRoot]/TargetContentList/TargetItemFolder/SourceItemFolder2/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemToSameContentList()
        //{
        //    //23: MoveContentListItemToSameContentList
        //    //Create [TestRoot]/ContentList/SourceItemFolder/SourceContentListItem
        //    //Create [TestRoot]/ContentList
        //    //Move SourceContentListItem, SourceContentList
        //    //Check: 
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/ContentList/SourceItemFolder/SourceContentListItem");
        //    //EnsureNode("[TestRoot]/ContentList");
        //    MoveNode("[TestRoot]/ContentList/SourceItemFolder/SourceContentListItem", "[TestRoot]/ContentList");
        //    CheckContentListItem1("[TestRoot]/ContentList/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemToSameContentListItem()
        //{
        //    //24: MoveContentListItemToSameContentListItem
        //    //Create [TestRoot]/ContentList/SourceItemFolder/SourceContentListItem
        //    //Create [TestRoot]/ContentList/TargetItemFolder
        //    //Move SourceContentListItem, TargetItemFolder
        //    //Check: 
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/ContentList/SourceItemFolder/SourceContentListItem");
        //    EnsureNode("[TestRoot]/ContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/ContentList/SourceItemFolder/SourceContentListItem", "[TestRoot]/ContentList/TargetItemFolder");
        //    CheckContentListItem1("[TestRoot]/ContentList/TargetItemFolder/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTreeToSameContentList()
        //{
        //    //25: MoveContentListItemTreeToSameContentList
        //    //Create [TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
        //    //Create [TestRoot]/ContentList
        //    //Move SourceItemFolder2, SourceContentList
        //    //Check: 
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
        //    //EnsureNode("[TestRoot]/ContentList");
        //    MoveNode("[TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2", "[TestRoot]/ContentList");
        //    CheckContentListItem1("[TestRoot]/ContentList/SourceItemFolder2");
        //    CheckContentListItem1("[TestRoot]/ContentList/SourceItemFolder2/SourceContentListItem");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_ContentList_ContentListItemTreeToSameContentListItem()
        //{
        //    //26: MoveContentListItemTreeToSameContentListItem
        //    //Create [TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem
        //    //Create [TestRoot]/ContentList/TargetItemFolder
        //    //Move SourceItemFolder2, TargetItemFolder
        //    //Check: 
        //    //PrepareTest();
        //    EnsureNode("[TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
        //    EnsureNode("[TestRoot]/ContentList/TargetItemFolder");
        //    MoveNode("[TestRoot]/ContentList/SourceItemFolder1/SourceItemFolder2", "[TestRoot]/ContentList/TargetItemFolder");
        //    CheckContentListItem1("[TestRoot]/ContentList/TargetItemFolder/SourceItemFolder2");
        //    CheckContentListItem1("[TestRoot]/ContentList/TargetItemFolder/SourceItemFolder2/SourceContentListItem");
        //}
        #endregion

        /* ============================================================================================== Tools */

        private void MoveTest(Action<SystemFolder> callback)
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
