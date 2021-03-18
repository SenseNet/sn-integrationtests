using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.BlobStorage.IntegrationTests.Implementations;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;

namespace SenseNet.BlobStorage.IntegrationTests
{
    [TestClass]
    public class BuiltInLocalDiskChunkTests : BlobStorageIntegrationTests
    {
        protected override string DatabaseName => "sn7blobtests_builtin";
        protected override bool SqlFsEnabled => false;
        protected override bool SqlFsUsed => false;
        protected override Type ExpectedExternalBlobProviderType => typeof(LocalDiskChunkBlobProvider);
        protected override Type ExpectedMetadataProviderType => typeof(MsSqlBlobMetaDataProvider);
        protected override Type ExpectedBlobProviderDataType => typeof(LocalDiskChunkBlobProvider.LocalDiskChunkBlobProviderData);
        protected internal override void ConfigureMinimumSizeForFileStreamInBytes(int newValue, out int oldValue)
        {
            oldValue = Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes;
            Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes = newValue;
        }

        [ClassCleanup]
        public static void CleanupClass()
        {
            TearDown(typeof(BuiltInLocalDiskChunkTests));
        }

        /* ==================================================== Test cases */

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_CreateFileSmall()
        {
            TestCase_CreateFileSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_CreateFileBig()
        {
            TestCase_CreateFileBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_UpdateFileSmallEmpty()
        {
            TestCase_UpdateFileSmallEmpty();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_UpdateFileBigEmpty()
        {
            TestCase_UpdateFileBigEmpty();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_UpdateFileSmallSmall()
        {
            TestCase_UpdateFileSmallSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_UpdateFileSmallBig()
        {
            TestCase_UpdateFileSmallBig();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_UpdateFileBigSmall()
        {
            TestCase_UpdateFileBigSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_UpdateFileBigBig()
        {
            TestCase_UpdateFileBigBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_WriteChunksSmall()
        {
            TestCase_WriteChunksSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_WriteChunksBig()
        {
            TestCase_WriteChunksBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_DeleteBinaryPropertySmall()
        {
            TestCase_DeleteBinaryPropertySmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_DeleteBinaryPropertyBig()
        {
            TestCase_DeleteBinaryPropertyBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_CopyfileRowSmall()
        {
            TestCase_CopyfileRowSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_CopyfileRowBig()
        {
            TestCase_CopyfileRowBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_BinaryCacheEntitySmall()
        {
            TestCase_BinaryCacheEntitySmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_BinaryCacheEntityBig()
        {
            TestCase_BinaryCacheEntityBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_DeleteSmall_Maintenance()
        {
            TestCase_DeleteSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltInLocalDiskChunk_DeleteBig_Maintenance()
        {
            TestCase_DeleteBig();
        }
    }
}
