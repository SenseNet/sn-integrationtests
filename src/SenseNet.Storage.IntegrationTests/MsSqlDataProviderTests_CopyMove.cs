using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Schema;
using SenseNet.ContentRepository.Search;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.Search;
using SenseNet.Security;
using Task = System.Threading.Tasks.Task;

namespace SenseNet.Storage.IntegrationTests
{
    public partial class MsSqlDataProviderTests
    {
        #region General Move tests

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_SourceIsNotExist()
        {
            MoveTest(testRoot =>
            {
                Node.Move("/Root/osiejfvchxcidoklg6464783930020398473/iygfevfbvjvdkbu9867513125615", testRoot.Path);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_TargetIsNotExist()
        {
            MoveTest(testRoot =>
            {
                Node.Move(testRoot.Path, "/Root/fdgdffgfccxdxdsffcv31945581316942/udjkcmdkeieoeoodoc542364737827");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void MsSqlDP_Move_MoveTo_Null()
        {
            MoveTest(testRoot =>
            {
                testRoot.MoveTo(null);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void MsSqlDP_Move_NullSourcePath()
        {
            MoveTest(testRoot =>
            {
                Node.Move(null, testRoot.Path);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_InvalidSourcePath()
        {
            MoveTest(testRoot =>
            {
                Node.Move(string.Empty, testRoot.Path);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void MsSqlDP_Move_NullTargetPath()
        {
            MoveTest(testRoot =>
            {
                Node.Move(testRoot.Path, null);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_InvalidTargetPath()
        {
            MoveTest(testRoot =>
            {
                Node.Move(testRoot.Path, string.Empty);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ToItsParent()
        {
            MoveTest(testRoot =>
            {
                MoveNode(testRoot.Path, testRoot.ParentPath, testRoot);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ToItself()
        {
            MoveTest(testRoot =>
            {
                Node.Move(testRoot.Path, testRoot.Path);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ToUnderItself()
        {
            MoveTest(testRoot =>
            {
                EnsureNode(testRoot, "Source/N3");
                MoveNode("Source", "Source/N3", testRoot);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(NodeAlreadyExistsException))]
        public void MsSqlDP_Move_TargetHasSameName()
        {
            MoveTest(testRoot =>
            {
                EnsureNode(testRoot, "Source");
                EnsureNode(testRoot, "Target/Source");
                MoveNode("Source", "Target", testRoot);
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_NodeTreeToNode()
        {
            MoveTest(testRoot =>
            {
                EnsureNode(testRoot, "Source/N1/N2");
                EnsureNode(testRoot, "Source/N3");
                EnsureNode(testRoot, "Target");
                MoveNode("Source", "Target", testRoot, true);
                Assert.IsNotNull(LoadNode(testRoot, "Target/Source/N1"), "#1");
                Assert.IsNotNull(LoadNode(testRoot, "Target/Source/N1/N2"), "#2");
                Assert.IsNotNull(LoadNode(testRoot, "Target/Source/N3"), "#3");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_SourceIsLockedByAnother()
        {
            MoveTest(testRoot =>
            {
                AclEditor.Create(new SecurityContext(User.Current))
                    .Allow(Identifiers.PortalRootId, Identifiers.AdministratorUserId, false, PermissionType.PermissionTypes)
                    .Allow(Identifiers.VisitorUserId, Identifiers.VisitorUserId, false, PermissionType.PermissionTypes)
                    .Allow(testRoot.Id, Identifiers.VisitorUserId, false, PermissionType.PermissionTypes)
                    .Apply();

                IUser originalUser = AccessProvider.Current.GetCurrentUser();
                IUser visitor = Node.LoadNode(Identifiers.VisitorUserId) as IUser;
                SecurityHandler.CreateAclEditor().Allow(testRoot.Id, visitor.Id, false, PermissionType.Save, PermissionType.Delete).Apply();

                EnsureNode(testRoot, "Source/N1");
                EnsureNode(testRoot, "Source/N2");
                EnsureNode(testRoot, "Target");
                var lockedNode = (GenericContent)LoadNode(testRoot, "Source");

                AccessProvider.Current.SetCurrentUser(visitor);
                try
                {
                    lockedNode.CheckOut();
                }
                finally
                {
                    AccessProvider.Current.SetCurrentUser(originalUser);
                }

                AccessProvider.Current.SetCurrentUser(User.Administrator);
                bool expectedExceptionWasThrown = false;
                Exception thrownException = null;
                try
                {
                    MoveNode("Source", "Target", testRoot);
                }
                catch (Exception e)
                {
                    if (e is LockedNodeException || e.InnerException is LockedNodeException)
                        expectedExceptionWasThrown = true;
                    else
                        thrownException = e;
                }
                finally
                {
                    AccessProvider.Current.SetCurrentUser(originalUser);
                }

                lockedNode.Reload();
                AccessProvider.Current.SetCurrentUser(visitor);
                lockedNode.UndoCheckOut();
                AccessProvider.Current.SetCurrentUser(originalUser);

                Assert.IsTrue(expectedExceptionWasThrown, "The expected exception was not thrown.");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_SourceIsLockedByCurrent()
        {
            MoveTest(testRoot =>
            {
                AclEditor.Create(new SecurityContext(User.Current))
                    .Allow(Identifiers.PortalRootId, Identifiers.AdministratorUserId, false, PermissionType.PermissionTypes)
                    .Apply();

                EnsureNode(testRoot, "Source/N1");
                EnsureNode(testRoot, "Source/N2");
                EnsureNode(testRoot, "Target");
                var lockedNode = (GenericContent)LoadNode(testRoot, "Source");
                try
                {
                    lockedNode.CheckOut();
                    MoveNode("Source", "Target", testRoot);
                }
                finally
                {
                    lockedNode = (GenericContent)Node.LoadNode(lockedNode.Id);
                    if (lockedNode.Lock.Locked)
                        lockedNode.UndoCheckOut();
                }
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_LockedTarget_SameUser()
        {
            MoveTest(testRoot =>
            {
                AclEditor.Create(new SecurityContext(User.Current))
                    .Allow(Identifiers.PortalRootId, Identifiers.AdministratorUserId, false, PermissionType.PermissionTypes)
                    .Apply();

                EnsureNode(testRoot, "Source/N1/N2/N3");
                EnsureNode(testRoot, "Source/N1/N4/N5");
                EnsureNode(testRoot, "Target/N6");
                var lockedNode = (GenericContent)LoadNode(testRoot, "Source/N1/N4");
                try
                {
                    lockedNode.CheckOut();
                    MoveNode("Source", "Target", testRoot, true);
                }
                finally
                {
                    lockedNode = (GenericContent)Node.LoadNode(lockedNode.Id);
                    if (lockedNode.Lock.Locked)
                        lockedNode.UndoCheckOut();
                }
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_PathBeforeAfter()
        {
            MoveTest(testRoot =>
            {
                EnsureNode(testRoot, "Source/N1/N2");
                EnsureNode(testRoot, "Source/N1/N3");
                EnsureNode(testRoot, "Target");
                var n1 = LoadNode(testRoot, "Source/N1");
                var pathBefore = n1.Path;

                n1.MoveTo(Node.LoadNode(DecodePath(testRoot, "Target")));

                var pathAfter = n1.Path;

                var n2 = LoadNode(testRoot, "Target/N1");

                Assert.IsNotNull(n2, "#1");
                Assert.IsTrue(pathBefore != pathAfter, "#2");
                Assert.IsTrue(pathAfter == n2.Path, "#3");
            });
        }

        [TestMethod]
        public void MsSqlDP_Move_MinimalPermissions()
        {
            MoveTest(testRoot =>
            {
                IUser visitor = Node.LoadNode(RepositoryConfiguration.VisitorUserId) as IUser;
                EnsureNode(testRoot, "Source");
                EnsureNode(testRoot, "Target");
                Node sourceNode = LoadNode(testRoot, "Source");
                Node targetNode = LoadNode(testRoot, "Target");
                SecurityHandler.CreateAclEditor()
                    .Allow(sourceNode.Id, visitor.Id, false, PermissionType.OpenMinor, PermissionType.Delete)
                    .Allow(targetNode.Id, visitor.Id, false, PermissionType.AddNew)
                    .Apply();

                IUser originalUser = AccessProvider.Current.GetCurrentUser();
                try
                {
                    AccessProvider.Current.SetCurrentUser(visitor);
                }
                finally
                {
                    AccessProvider.Current.SetCurrentUser(originalUser);
                }
            });
        }
        //[TestMethod]
        //public void MsSqlDP_Move_SourceWithoutOpenMinorPermission()
        //{
        //    IUser originalUser = AccessProvider.Current.GetCurrentUser();
        //    IUser visitor = Node.LoadNode(RepositoryConfiguration.VisitorUserId) as IUser;
        //    EnsureNode(testRoot, "Source");
        //    EnsureNode(testRoot, "Target");
        //    Node sourceNode = LoadNode(testRoot, "Source");
        //    Node targetNode = LoadNode(testRoot, "Target");
        //    sourceNode.Security.SetPermission(visitor, PermissionType.Delete, PermissionValue.Allowed);
        //    targetNode.Security.SetPermission(visitor, PermissionType.AddNew, PermissionValue.Allowed);
        //    bool expectedExceptionWasThrown = false;
        //    try
        //    {
        //        AccessProvider.Current.SetCurrentUser(visitor);
        //        MoveNode("Source", "Target", testRoot);
        //    }
        //    catch (LockedNodeException)
        //    {
        //        expectedExceptionWasThrown = true;
        //    }
        //    finally
        //    {
        //        AccessProvider.Current.SetCurrentUser(originalUser);
        //    }
        //    Assert.IsTrue(expectedExceptionWasThrown, "The expected exception was not thrown.");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_SourceWithoutDeletePermission()
        //{
        //    IUser originalUser = AccessProvider.Current.GetCurrentUser();
        //    IUser visitor = Node.LoadNode(RepositoryConfiguration.VisitorUserId) as IUser;
        //    EnsureNode(testRoot, "Source");
        //    EnsureNode(testRoot, "Target");
        //    Node sourceNode = LoadNode(testRoot, "Source");
        //    Node targetNode = LoadNode(testRoot, "Target");
        //    sourceNode.Security.SetPermission(visitor, PermissionType.OpenMinor, PermissionValue.Allowed);
        //    targetNode.Security.SetPermission(visitor, PermissionType.AddNew, PermissionValue.Allowed);
        //    bool expectedExceptionWasThrown = false;
        //    try
        //    {
        //        AccessProvider.Current.SetCurrentUser(visitor);
        //        MoveNode("Source", "Target", testRoot);
        //    }
        //    catch (LockedNodeException)
        //    {
        //        expectedExceptionWasThrown = true;
        //    }
        //    finally
        //    {
        //        AccessProvider.Current.SetCurrentUser(originalUser);
        //    }
        //    Assert.IsTrue(expectedExceptionWasThrown, "The expected exception was not thrown.");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_TargetWithoutAddNewPermission()
        //{
        //    IUser originalUser = AccessProvider.Current.GetCurrentUser();
        //    IUser visitor = Node.LoadNode(RepositoryConfiguration.VisitorUserId) as IUser;
        //    EnsureNode(testRoot, "Source");
        //    EnsureNode(testRoot, "Target");
        //    Node sourceNode = LoadNode(testRoot, "Source");
        //    Node targetNode = LoadNode(testRoot, "Target");
        //    sourceNode.Security.SetPermission(visitor, PermissionType.OpenMinor, PermissionValue.Allowed);
        //    sourceNode.Security.SetPermission(visitor, PermissionType.Delete, PermissionValue.Allowed);
        //    bool expectedExceptionWasThrown = false;
        //    try
        //    {
        //        AccessProvider.Current.SetCurrentUser(visitor);
        //        MoveNode("Source", "Target", testRoot);
        //    }
        //    catch (LockedNodeException)
        //    {
        //        expectedExceptionWasThrown = true;
        //    }
        //    finally
        //    {
        //        AccessProvider.Current.SetCurrentUser(originalUser);
        //    }
        //    Assert.IsTrue(expectedExceptionWasThrown, "The expected exception was not thrown.");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_SourceTreeWithPartialOpenMinorPermission()
        //{
        //    IUser originalUser = AccessProvider.Current.GetCurrentUser();
        //    IUser visitor = Node.LoadNode(RepositoryConfiguration.VisitorUserId) as IUser;
        //    EnsureNode(testRoot, "Source/N1/N2");
        //    EnsureNode(testRoot, "Source/N1/N3");
        //    EnsureNode(testRoot, "Source/N4");
        //    EnsureNode(testRoot, "Target");
        //    Node source = LoadNode(testRoot, "Source");
        //    Node target = LoadNode(testRoot, "Target");
        //    source.Security.SetPermission(visitor, PermissionType.OpenMinor, PermissionValue.Allowed);
        //    source.Security.SetPermission(visitor, PermissionType.Delete, PermissionValue.Allowed);
        //    target.Security.SetPermission(visitor, PermissionType.AddNew, PermissionValue.Allowed);
        //    Node blockedNode = LoadNode(testRoot, "Source/N1/N3");
        //    blockedNode.Security.SetPermission(visitor, PermissionType.OpenMinor, PermissionValue.Undefined);
        //    bool expectedExceptionWasThrown = false;
        //    try
        //    {
        //        AccessProvider.Current.SetCurrentUser(visitor);
        //        MoveNode("Source", "Target", testRoot);
        //    }
        //    catch (LockedNodeException)
        //    {
        //        expectedExceptionWasThrown = true;
        //    }
        //    finally
        //    {
        //        AccessProvider.Current.SetCurrentUser(originalUser);
        //    }
        //    Assert.IsTrue(expectedExceptionWasThrown, "The expected exception was not thrown.");
        //}
        //[TestMethod]
        //public void MsSqlDP_Move_SourceTreeWithPartialDeletePermission()
        //{
        //    IUser originalUser = AccessProvider.Current.GetCurrentUser();
        //    IUser visitor = Node.LoadNode(RepositoryConfiguration.VisitorUserId) as IUser;
        //    EnsureNode(testRoot, "Source/N1/N2");
        //    EnsureNode(testRoot, "Source/N1/N3");
        //    EnsureNode(testRoot, "Source/N4");
        //    EnsureNode(testRoot, "Target");
        //    Node source = LoadNode(testRoot, "Source");
        //    Node target = LoadNode(testRoot, "Target");
        //    source.Security.SetPermission(visitor, PermissionType.OpenMinor, PermissionValue.Allowed);
        //    source.Security.SetPermission(visitor, PermissionType.Delete, PermissionValue.Allowed);
        //    target.Security.SetPermission(visitor, PermissionType.AddNew, PermissionValue.Allowed);
        //    Node blockedNode = LoadNode(testRoot, "Source/N1/N3");
        //    blockedNode.Security.SetPermission(visitor, PermissionType.Delete, PermissionValue.Undefined);
        //    bool expectedExceptionWasThrown = false;
        //    try
        //    {
        //        AccessProvider.Current.SetCurrentUser(visitor);
        //        MoveNode("Source", "Target", testRoot);
        //    }
        //    catch (LockedNodeException)
        //    {
        //        expectedExceptionWasThrown = true;
        //    }
        //    finally
        //    {
        //        AccessProvider.Current.SetCurrentUser(originalUser);
        //    }
        //    Assert.IsTrue(expectedExceptionWasThrown, "The expected exception was not thrown.");
        //}

        #endregion

        [TestMethod]
        public void MsSqlDP_Move_MoreVersion()
        {
            MoveTest(testRoot =>
            {
                EnsureNode(testRoot, "Source");
                var node = (GenericContent)LoadNode(testRoot, "Source");
                node.InheritableVersioningMode = ContentRepository.Versioning.InheritableVersioningType.MajorAndMinor;
                node.Save();
                EnsureNode(testRoot, "Source/M1");
                node = (GenericContent)LoadNode(testRoot, "Source/M1");
                var m1NodeId = node.Id;
                node.Index++;
                node.Save();
                node = (GenericContent)LoadNode(testRoot, "Source/M1");
                node.Index++;
                node.Save();
                ((GenericContent)LoadNode(testRoot, "Source/M1")).Publish();
                ((GenericContent)LoadNode(testRoot, "Source/M1")).CheckOut();
                EnsureNode(testRoot, "Target");

                MoveNode("Source", "Target", testRoot, true);

                var result = CreateSafeContentQuery($"InTree:'{DecodePath(testRoot, "Target")}' .AUTOFILTERS:OFF").Execute();
                var paths = result.Nodes.Select(n => n.Path).ToArray();
                Assert.IsTrue(paths.Length == 3, $"Count of paths is {paths.Length}, expected {3}");

                var lastMajorVer = Node.LoadNode(DecodePath(testRoot, "Target/Source/M1"), VersionNumber.LastMajor).Version.ToString();
                var lastMinorVer = Node.LoadNode(DecodePath(testRoot, "Target/Source/M1"), VersionNumber.LastMinor).Version.ToString();

                Assert.IsTrue(lastMajorVer == "V1.0.A", String.Concat("LastMajor version is ", lastMajorVer, ", expected: V1.0.A"));
                Assert.IsTrue(lastMinorVer == "V1.1.L", String.Concat("LastMinor version is ", lastMinorVer, ", expected: V1.1.L"));

                var versionDump = GetVersionDumpByNodeId(m1NodeId);
                Assert.AreEqual("V0.1.D, V0.2.D, V1.0.A, V1.1.L", versionDump);
            });
        }
        private string GetVersionDumpByNodeId(int nodeId)
        {
            var head = NodeHead.Get(nodeId);
            return string.Join(", ", head.Versions.Select(x=>x.VersionNumber.ToString()));
            //var docs = SenseNet.Search.Indexing.LuceneManager.GetDocumentsByNodeId(nodeId);
            //return String.Join(", ", docs.Select(d => d.Get(LucObject.FieldName.Version)).ToArray());
        }

        [TestMethod]
        public void MsSqlDP_Move_WithAspect()
        {
            MoveTest(testRoot =>
            {
                // create an aspect
                var aspect1 = EnsureAspect("CopyMoveTest_Move_WithAspect");
                aspect1.AddFields(new FieldInfo { Name = "Field1", Type = "ShortText" });

                // create structure
                EnsureNode(testRoot, "Source");
                EnsureNode(testRoot, "Source/Child");
                var content = Content.Create(LoadNode(testRoot, "Source/Child"));
                content.AddAspects(aspect1);
                content["CopyMoveTest_Move_WithAspect.Field1"] = "value1";
                content.Save();
                var contentId = content.Id;

                // remove aspect from cache
                var cacheKey = "SN_AspectCacheByName_" + aspect1.Name;
                DistributedApplication.Cache.Remove(cacheKey);

                // ACTION: rename
                var node = LoadNode(testRoot, "Source");
                node.Name = Guid.NewGuid().ToString();
                node.Save();

                // EXPECTATION: executed without any exception
            });
        }

        #region ContentList Move Tests
        [TestMethod]
        public void MsSqlDP_Move_ContentList_LeafNodeToContentList()
        {
            MoveTest(testRoot =>
            {
                //1: MoveLeafNodeToContentList
                EnsureNode(testRoot, "SourceNode");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceNode", "TargetContentList", testRoot);
                CheckContentListItem2(testRoot, "TargetContentList/SourceNode");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_LeafNodeToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //2: MoveLeafNodeToContentListItem
                EnsureNode(testRoot, "SourceNode");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceNode", "TargetContentList/TargetItemFolder", testRoot);
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceNode");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_NodeTreeToContentList()
        {
            MoveTest(testRoot =>
            {
                //3: MoveNodeTreeToContentList
                EnsureNode(testRoot, "SourceFolder/SourceNode");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceFolder", "TargetContentList", testRoot);
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
                EnsureNode(testRoot, "SourceFolder/SourceNode");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceFolder", "TargetContentList/TargetItemFolder", testRoot);
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceFolder");
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceFolder/SourceNode");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_NodeWithContentListToNode()
        {
            MoveTest(testRoot =>
            {
                //5: MoveNodeWithContentListToNode
                EnsureNode(testRoot, "SourceFolder/SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode("SourceFolder", "TargetFolder", testRoot);
                CheckSimpleNode(testRoot, "TargetFolder/SourceFolder");
                CheckContentList1(testRoot, "TargetFolder/SourceFolder/SourceContentList");
                CheckContentListItem1(testRoot, "TargetFolder/SourceFolder/SourceContentList/SourceContentListItem");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_NodeWithContentListToContentList()
        {
            MoveTest(testRoot =>
            {
                //6: MoveNodeWithContentListToContentList
                EnsureNode(testRoot, "SourceFolder/SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceFolder", "TargetContentList", testRoot);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_NodeWithContentListToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //7: MoveNodeWithContentListToContentListItem
                EnsureNode(testRoot, "SourceFolder/SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceFolder", "TargetContentList/TargetItemFolder", testRoot);
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListToNode()
        {
            MoveTest(testRoot =>
            {
                //8: MoveContentListToNode
                EnsureNode(testRoot, "SourceContentList");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode("SourceContentList", "TargetFolder", testRoot);
                CheckContentList1(testRoot, "TargetFolder/SourceContentList");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListToContentList()
        {
            MoveTest(testRoot =>
            {
                //9: MoveContentListToContentList
                EnsureNode(testRoot, "SourceContentList");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceContentList", "TargetContentList", testRoot);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //10: MoveContentListToContentListItem
                EnsureNode(testRoot, "SourceContentList");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceContentList", "TargetContentList/TargetItemFolder", testRoot);
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListTreeToNode()
        {
            MoveTest(testRoot =>
            {
                //11: MoveContentListTreeToNode
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode("SourceContentList", "TargetFolder", testRoot);
                CheckContentList1(testRoot, "TargetFolder/SourceContentList");
                CheckContentListItem1(testRoot, "TargetFolder/SourceContentList/SourceContentListItem");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListTreeToContentList()
        {
            MoveTest(testRoot =>
            {
                //12: MoveContentListTreeToContentList
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceContentList", "TargetContentList", testRoot);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void MsSqlDP_Move_ContentList_ContentListTreeToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //13: MoveContentListTreeToContentListItem
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceContentList", "TargetContentList/TargetItemFolder", testRoot);
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToNode()
        {
            MoveTest(testRoot =>
            {
                //14: MoveContentListItemToNode
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode("SourceContentList/SourceContentListItem", "TargetFolder", testRoot);
                CheckSimpleNode(testRoot, "TargetFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToContentList()
        {
            MoveTest(testRoot =>
            {
                //15: MoveContentListItemToContentList
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceContentList/SourceContentListItem", "TargetContentList", testRoot);
                CheckContentListItem2(testRoot, "TargetContentList/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToContentListItem()
        {
            MoveTest(testRoot =>
            {
                //16: MoveContentListItemToContentListItem
                EnsureNode(testRoot, "SourceContentList/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceContentList/SourceContentListItem", "TargetContentList/TargetItemFolder", testRoot);
                CheckContentListItem2(testRoot, "TargetContentList/TargetItemFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTreeToNode()
        {
            MoveTest(testRoot =>
            {
                //17: MoveContentListItemTreeToNode
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode("SourceContentList/SourceItemFolder", "TargetFolder", testRoot);
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
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceContentList/SourceItemFolder", "TargetContentList", testRoot);
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
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceContentList/SourceItemFolder", "TargetContentList/TargetItemFolder", testRoot);
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
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "TargetFolder");
                MoveNode("SourceContentList/SourceItemFolder1/SourceItemFolder2", "TargetFolder", testRoot);
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
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList");
                MoveNode("SourceContentList/SourceItemFolder1/SourceItemFolder2", "TargetContentList", testRoot);
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
                EnsureNode(testRoot, "SourceContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "TargetContentList/TargetItemFolder");
                MoveNode("SourceContentList/SourceItemFolder1/SourceItemFolder2", "TargetContentList/TargetItemFolder", testRoot);
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
                EnsureNode(testRoot, "ContentList/SourceItemFolder/SourceContentListItem");
                MoveNode("ContentList/SourceItemFolder/SourceContentListItem", "ContentList", testRoot);
                CheckContentListItem1(testRoot, "ContentList/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemToSameContentListItem()
        {
            MoveTest(testRoot =>
            {
                //24: MoveContentListItemToSameContentListItem
                EnsureNode(testRoot, "ContentList/SourceItemFolder/SourceContentListItem");
                EnsureNode(testRoot, "ContentList/TargetItemFolder");
                MoveNode("ContentList/SourceItemFolder/SourceContentListItem", "ContentList/TargetItemFolder", testRoot);
                CheckContentListItem1(testRoot, "ContentList/TargetItemFolder/SourceContentListItem");
            });
        }
        [TestMethod]
        public void MsSqlDP_Move_ContentList_ContentListItemTreeToSameContentList()
        {
            MoveTest(testRoot =>
            {
                //25: MoveContentListItemTreeToSameContentList
                EnsureNode(testRoot, "ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                MoveNode("ContentList/SourceItemFolder1/SourceItemFolder2", "ContentList", testRoot);
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
                EnsureNode(testRoot, "ContentList/SourceItemFolder1/SourceItemFolder2/SourceContentListItem");
                EnsureNode(testRoot, "ContentList/TargetItemFolder");
                MoveNode("ContentList/SourceItemFolder1/SourceItemFolder2", "ContentList/TargetItemFolder", testRoot);
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
                    var nextId = TDP.GetLastNodeIdAsync().Result + 1;

                    var testRoot = new SystemFolder(Repository.Root) {Name = "MoveTest-" + nextId};
                    testRoot.Save();

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
        private void MoveNode(string encodedSourcePath, string encodedTargetPath, SystemFolder testRoot, bool clearTarget = false)
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
            if (relativePath == "/Root" || relativePath.StartsWith("/Root/"))
                return relativePath;
            if (relativePath.StartsWith("[TestRoot]"))
                Assert.Fail("[TestRoot] is not allowed. Use relative path instead.");
            return RepositoryPath.Combine(testRoot.Path, relativePath);
        }

        private Aspect EnsureAspect(string name)
        {
            var existing = Aspect.LoadAspectByName(name);
            if (existing != null)
                return existing;
            var aspectContent = Content.CreateNew("Aspect", Repository.AspectsFolder, name);
            aspectContent.Save();
            return (Aspect)aspectContent.ContentHandler;
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
