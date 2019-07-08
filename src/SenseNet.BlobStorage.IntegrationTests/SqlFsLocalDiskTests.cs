
//TODO: Use these class when the sn-blob-mssqlfs repository is implemented well

//using System;
//using System.Data;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using SenseNet.BlobStorage.IntegrationTests.Implementations;
//using SenseNet.ContentRepository;
//using SenseNet.ContentRepository.Storage;
//using SenseNet.ContentRepository.Storage.Data;
//using SenseNet.ContentRepository.Storage.Schema;
//using SenseNet.ContentRepository.Storage.Security;
//using SenseNet.MsSqlFsBlobProvider;

//namespace SenseNet.BlobStorage.IntegrationTests
//{
//    [TestClass]
//    public class SqlFsLocalDiskTests : BlobStorageIntegrationTests
//    {
//        protected override string DatabaseName => "sn7blobtests_builtinfs";
//        protected override bool SqlFsEnabled => true;
//        protected override bool SqlFsUsed => false;
//        protected override Type ExpectedExternalBlobProviderType => typeof(LocalDiskBlobProvider);
//        protected override Type ExpectedMetadataProviderType => typeof(SqlFileStreamBlobMetaDataProvider);
//        protected override Type ExpectedBlobProviderDataType => typeof(LocalDiskBlobProvider.LocalDiskBlobProviderData);
//        protected internal override void ConfigureMinimumSizeForFileStreamInBytes(int newValue, out int oldValue)
//        {
//            oldValue = Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes;
//            Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes = newValue;
//        }

//        [ClassCleanup]
//        public static void CleanupClass()
//        {
//            TearDown(typeof(SqlFsLocalDiskTests));
//        }


//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_CreateFileSmall()
//        {
//            TestCase_CreateFileSmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_CreateFileBig()
//        {
//            TestCase_CreateFileBig();
//        }

//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_UpdateFileSmallSmall()
//        {
//            TestCase_UpdateFileSmallSmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_UpdateFileSmallBig()
//        {
//            TestCase_UpdateFileSmallBig();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_UpdateFileBigSmall()
//        {
//            TestCase_UpdateFileBigSmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_UpdateFileBigBig()
//        {
//            TestCase_UpdateFileBigBig();
//        }

//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_WriteChunksSmall()
//        {
//            TestCase_WriteChunksSmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_WriteChunksBig()
//        {
//            TestCase_WriteChunksBig();
//        }

//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_DeleteBinaryPropertySmall()
//        {
//            TestCase_DeleteBinaryPropertySmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_DeleteBinaryPropertyBig()
//        {
//            TestCase_DeleteBinaryPropertyBig();
//        }

//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_CopyfileRowSmall()
//        {
//            TestCase_CopyfileRowSmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_CopyfileRowBig()
//        {
//            TestCase_CopyfileRowBig();
//        }

//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_BinaryCacheEntitySmall()
//        {
//            TestCase_BinaryCacheEntitySmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_BinaryCacheEntityBig()
//        {
//            TestCase_BinaryCacheEntityBig();
//        }

//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_DeleteSmall()
//        {
//            TestCase_DeleteSmall();
//        }
//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_DeleteBig()
//        {
//            TestCase_DeleteBig();
//        }

//        [TestMethod]
//        public void Blob_SqlFsLocalDisk_Bug_EmptyFileStreamAndExternalRecord()
//        {
//            // Symptom: record in the Files table that contains external provider and empty
//            // FileStream value (0x instead of [null]) causes error: LoadBinaryCacheEntity of the 
//            // BlobMetadata provider reads this value that overrides the BlobProvider settings
//            // and the SnStream constructor instantiates a RepositoryStream with a zero length buffer.

//            using (new SystemAccount())
//            using (new SizeLimitSwindler(this, 10))
//            {
//                var testRoot = CreateTestRoot();

//                var file = new File(testRoot) { Name = "File1.file" };
//                file.Binary.SetStream(RepositoryTools.GetStreamFromString("Lorem ipsum dolor sit amet..."));
//                file.Save();
//                var fileId = file.Binary.FileId;
//                var versionId = file.VersionId;
//                HackFileRowFileStream(fileId, new byte[0]);
//                var dbFile = LoadDbFile(fileId);
//                Assert.IsNotNull(dbFile.BlobProvider);
//                Assert.IsNotNull(dbFile.BlobProviderData);
//                Assert.IsNotNull(dbFile.FileStream);
//                Assert.AreEqual(0, dbFile.FileStream.Length);

//                // action
//                var bcEentity =
//                    BlobStorageComponents.DataProvider.LoadBinaryCacheEntity(versionId,
//                        PropertyType.GetByName("Binary").Id);

//                // assert
//                Assert.IsNull(bcEentity.RawData);
//            }
//        }

//    }
//}
