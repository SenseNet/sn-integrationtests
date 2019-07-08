
//TODO: SQLFS: Use these class when the sn-blob-mssqlfs repository is implemented well

//using System;
//using System.IO;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using SenseNet.BlobStorage.IntegrationTests.Implementations;
//using SenseNet.ContentRepository;
//using SenseNet.ContentRepository.Storage;
//using SenseNet.ContentRepository.Storage.Data;
//using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
//using SenseNet.ContentRepository.Storage.Security;
//using SenseNet.MsSqlFsBlobProvider;
//using File = SenseNet.ContentRepository.File;

//namespace SenseNet.BlobStorage.IntegrationTests
//{
//    [TestClass]
//    public class SqlFsMigrationTests : BlobStorageIntegrationTests
//    {
//        protected override string DatabaseName => "sn7blobtests_sqlfs";

//        protected override bool SqlFsEnabled => true;
//        protected override bool SqlFsUsed => true;
//        protected override Type ExpectedExternalBlobProviderType => typeof(SqlFileStreamBlobProvider);
//        protected override Type ExpectedMetadataProviderType => typeof(SqlFileStreamBlobMetaDataProvider);
//        protected override Type ExpectedBlobProviderDataType => null;
//        protected internal override void ConfigureMinimumSizeForFileStreamInBytes(int newValue, out int oldValue)
//        {
//            oldValue = Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes;
//            Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes = newValue;
//        }

//        [ClassCleanup]
//        public static void CleanupClass()
//        {
//            TearDown(typeof(SqlFsMigrationTests));
//        }


//        private static readonly string _text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit,"
//                                           + " sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

//        private readonly int _smallSizeLimit = 50;
//        private readonly int _bigSizeLimit = 500;

//        [TestMethod]
//        public void Blob_Migration_SqlFsMeta_SqlFsToBuiltIn()
//        {
//            using (new SystemAccount())
//            using (new SizeLimitSwindler(this, _smallSizeLimit))
//            {
//                var file = CreateFile(_text);
//                Assert.AreEqual(typeof(SqlFileStreamBlobProvider), GetUsedBlobProvider(file));

//                using (new SizeLimitSwindler(this, _bigSizeLimit))
//                    MoveData(file);

//                Assert.AreEqual(typeof(BuiltInBlobProvider), GetUsedBlobProvider(file));
//            }
//        }
//        [TestMethod]
//        public void Blob_Migration_SqlFsMeta_SqlFsToExternal()
//        {
//            using (new SystemAccount())
//            using (new SizeLimitSwindler(this, _smallSizeLimit))
//            {
//                var file = CreateFile(_text);
//                Assert.AreEqual(typeof(SqlFileStreamBlobProvider), GetUsedBlobProvider(file));

//                using (new BlobProviderSwindler(typeof(LocalDiskBlobProvider)))
//                    MoveData(file);

//                Assert.AreEqual(typeof(LocalDiskBlobProvider), GetUsedBlobProvider(file));
//            }
//        }
//        [TestMethod]
//        public void Blob_Migration_SqlFsMeta_BuiltInToExternal()
//        {
//            using (new SystemAccount())
//            using (new SizeLimitSwindler(this, _bigSizeLimit))
//            {
//                var file = CreateFile(_text);
//                Assert.AreEqual(typeof(BuiltInBlobProvider), GetUsedBlobProvider(file));

//                using (new BlobProviderSwindler(typeof(LocalDiskBlobProvider)))
//                using (new SizeLimitSwindler(this, _smallSizeLimit))
//                    MoveData(file);

//                Assert.AreEqual(typeof(LocalDiskBlobProvider), GetUsedBlobProvider(file));
//            }
//        }


//        [TestMethod]
//        public void Blob_Migration_Staging_SqlFsMeta_BuiltInToExternal()
//        {
//            using (new SystemAccount())
//            using (new BlobProviderSwindler(typeof(SqlFileStreamBlobProvider)))
//            using (new SizeLimitSwindler(this, _bigSizeLimit))
//            {
//                var file = CreateFile(_text);
//                Assert.AreEqual(typeof(BuiltInBlobProvider), GetUsedBlobProvider(file));
//                string loadedText = null;
//                using (new BlobProviderSwindler(typeof(LocalDiskBlobProvider)))
//                using (new SizeLimitSwindler(this, _smallSizeLimit))
//                {
//                    file = Node.Load<File>(file.Id);
//                    Assert.IsNull(file.Binary.BlobProvider);
//                    Assert.IsNull(file.Binary.BlobProviderData);
//                    var fileIdBefore = file.Binary.FileId;

//                    var stream = file.Binary.GetStream();
//                    var snStream = stream as SnStream;
//                    Assert.IsNotNull(snStream);
//                    Assert.AreEqual(typeof(BuiltInBlobProvider).Name, snStream.Context.Provider.GetType().Name);

//                    // action
//                    file.Binary.SetStream(stream);
//                    file.Save();

//                    // assert
//                    file = Node.Load<File>(file.Id);
//                    loadedText = GetStringFromBinary(file.Binary);

//                    Assert.AreEqual(typeof(LocalDiskBlobProvider).FullName, file.Binary.BlobProvider);
//                    Assert.AreEqual(fileIdBefore + 1, file.Binary.FileId);
//                }

//                Assert.AreEqual(_text, loadedText);
//            }
//        }
//        [TestMethod]
//        public void Blob_Migration_Staging_SqlFsMeta_SqlFsToExternal()
//        {
//            using (new SystemAccount())
//            using (new BlobProviderSwindler(typeof(SqlFileStreamBlobProvider)))
//            using (new SizeLimitSwindler(this, _smallSizeLimit))
//            {
//                var file = CreateFile(_text);
//                Assert.AreEqual(typeof(SqlFileStreamBlobProvider), GetUsedBlobProvider(file));
//                string loadedText = null;
//                using (new BlobProviderSwindler(typeof(LocalDiskBlobProvider)))
//                {
//                    file = Node.Load<File>(file.Id);
//                    Assert.IsNull(file.Binary.BlobProvider);
//                    Assert.IsNull(file.Binary.BlobProviderData);
//                    var fileIdBefore = file.Binary.FileId;

//                    var stream = file.Binary.GetStream();
//                    var snStream = stream as SnStream;
//                    Assert.IsNotNull(snStream);
//                    Assert.AreEqual(typeof(SqlFileStreamBlobProvider).Name, snStream.Context.Provider.GetType().Name);

//                    // action
//                    file.Binary.SetStream(stream);
//                    file.Save();

//                    // assert
//                    file = Node.Load<File>(file.Id);
//                    loadedText = GetStringFromBinary(file.Binary);

//                    Assert.AreEqual(typeof(LocalDiskBlobProvider).FullName, file.Binary.BlobProvider);
//                    Assert.AreEqual(fileIdBefore + 1, file.Binary.FileId);
//                }

//                Assert.AreEqual(_text, loadedText);
//            }
//        }
//        [TestMethod]
//        public void Blob_Migration_Staging_SqlFsMeta_ExternalToExternal()
//        {
//            using (new SystemAccount())
//            using (new BlobProviderSwindler(typeof(LocalDiskBlobProvider)))
//            using (new SizeLimitSwindler(this, _smallSizeLimit))
//            {
//                var file = CreateFile(_text);
//                Assert.AreEqual(typeof(LocalDiskBlobProvider), GetUsedBlobProvider(file));
//                string loadedText = null;
//                using (new BlobProviderSwindler(typeof(LocalDiskChunkBlobProvider)))
//                {
//                    file = Node.Load<File>(file.Id);
//                    Assert.AreEqual(typeof(LocalDiskBlobProvider).FullName, file.Binary.BlobProvider);
//                    Assert.IsNotNull(file.Binary.BlobProviderData);
//                    var fileIdBefore = file.Binary.FileId;

//                    var stream = file.Binary.GetStream();
//                    var snStream = stream as SnStream;
//                    Assert.IsNotNull(snStream);
//                    Assert.AreEqual(typeof(LocalDiskBlobProvider).Name, snStream.Context.Provider.GetType().Name);

//                    // action
//                    file.Binary.SetStream(stream);
//                    file.Save();

//                    // assert
//                    file = Node.Load<File>(file.Id);
//                    loadedText = GetStringFromBinary(file.Binary);

//                    Assert.AreEqual(typeof(LocalDiskChunkBlobProvider).FullName, file.Binary.BlobProvider);
//                    Assert.AreEqual(fileIdBefore + 1, file.Binary.FileId);
//                }

//                Assert.AreEqual(_text, loadedText);
//            }
//        }

//        /* ====================================================================================== */

//        private File CreateFile(string fileContent)
//        {
//            var testRoot = CreateTestRoot();

//            var file = new File(testRoot) { Name = "File1.file" };
//            file.Binary.SetStream(RepositoryTools.GetStreamFromString(fileContent));
//            file.Save();

//            return file;
//        }

//        private void MoveData(File file)
//        {
//            var readerStream = file.Binary.GetStream();
//            var buffer = new byte[readerStream.Length];
//            readerStream.Read(buffer, 0, readerStream.Length.ToInt());

//            var updaterStream = new MemoryStream(buffer);
//            file.Binary.SetStream(updaterStream);

//            file.Save();
//        }

//    }
//}
