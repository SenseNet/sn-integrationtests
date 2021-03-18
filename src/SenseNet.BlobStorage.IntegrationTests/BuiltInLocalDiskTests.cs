using System;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.BlobStorage.IntegrationTests.Implementations;
using SenseNet.ContentRepository;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Schema;
using SenseNet.ContentRepository.Storage.Security;

namespace SenseNet.BlobStorage.IntegrationTests
{
    [TestClass]
    public class BuiltInLocalDiskTests : BlobStorageIntegrationTests
    {
        protected override string DatabaseName => "sn7blobtests_builtin"; //"sn7blobtests_builtinfs";
        protected override bool SqlFsEnabled => false;
        protected override bool SqlFsUsed => false;
        protected override Type ExpectedExternalBlobProviderType => typeof(LocalDiskBlobProvider);
        protected override Type ExpectedMetadataProviderType => typeof(MsSqlBlobMetaDataProvider);
        protected override Type ExpectedBlobProviderDataType => typeof(LocalDiskBlobProvider.LocalDiskBlobProviderData);
        protected internal override void ConfigureMinimumSizeForFileStreamInBytes(int newValue, out int oldValue)
        {
            oldValue = Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes;
            Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes = newValue;
        }

        [ClassCleanup]
        public static void CleanupClass()
        {
            TearDown(typeof(BuiltInLocalDiskTests));
        }

        /* ==================================================== Test cases */

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_CreateFileSmall()
        {
            TestCase_CreateFileSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_CreateFileBig()
        {
            TestCase_CreateFileBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_UpdateFileSmallEmpty()
        {
            TestCase_UpdateFileSmallEmpty();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_UpdateFileBigEmpty()
        {
            TestCase_UpdateFileBigEmpty();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_UpdateFileSmallSmall()
        {
            TestCase_UpdateFileSmallSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_UpdateFileSmallBig()
        {
            TestCase_UpdateFileSmallBig();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_UpdateFileBigSmall()
        {
            TestCase_UpdateFileBigSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_UpdateFileBigBig()
        {
            TestCase_UpdateFileBigBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_WriteChunksSmall()
        {
            TestCase_WriteChunksSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_WriteChunksBig()
        {
            TestCase_WriteChunksBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_DeleteBinaryPropertySmall()
        {
            TestCase_DeleteBinaryPropertySmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_DeleteBinaryPropertyBig()
        {
            TestCase_DeleteBinaryPropertyBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_CopyfileRowSmall()
        {
            TestCase_CopyfileRowSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_CopyfileRowBig()
        {
            TestCase_CopyfileRowBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_BinaryCacheEntitySmall()
        {
            TestCase_BinaryCacheEntitySmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_BinaryCacheEntityBig()
        {
            TestCase_BinaryCacheEntityBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_DeleteSmall_Maintenance()
        {
            TestCase_DeleteSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_DeleteBig_Maintenance()
        {
            TestCase_DeleteBig();
        }

        //TODO: Use this test when the sn-blob-mssqlfs repository is implemented well
        //// /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDisk_Bug_EmptyFileStreamAndExternalRecord()
        {
            // Symptom: record in the Files table that contains external provider and empty
            // FileStream value (0x instead of [null]) causes error: LoadBinaryCacheEntity of the 
            // BlobMetadata provider reads this value that overrides the BlobProvider settings
            // and the SnStream constructor instantiates a RepositoryStream with a zero length buffer.

            Assert.Inconclusive();

            using (new SystemAccount())
            using (new SizeLimitSwindler(this, 10))
            {
                var testRoot = CreateTestRoot();

                var file = new File(testRoot) { Name = "File1.file" };
                file.Binary.SetStream(RepositoryTools.GetStreamFromString("Lorem ipsum dolor sit amet..."));
                file.Save();
                var fileId = file.Binary.FileId;
                var versionId = file.VersionId;
                HackFileRowStream(fileId, new byte[0]);
                var dbFile = LoadDbFile(fileId);
                Assert.IsNotNull(dbFile.BlobProvider);
                Assert.IsNotNull(dbFile.BlobProviderData);
                Assert.IsNotNull(dbFile.FileStream);
                Assert.AreEqual(0, dbFile.FileStream.Length);

                // action
                var bcEentity =
                    BlobStorageComponents.DataProvider.LoadBinaryCacheEntityAsync(versionId,
                        PropertyType.GetByName("Binary").Id,
                        CancellationToken.None).GetAwaiter().GetResult();

                // assert
                Assert.IsNull(bcEentity.RawData);
            }
        }
    }
}
