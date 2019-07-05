using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;

namespace SenseNet.BlobStorage.IntegrationTests
{
    [TestClass]
    public class BuiltInFsTests : BlobStorageIntegrationTests
    {
        protected override string DatabaseName => "sn7blobtests_builtinfs";
        protected override bool SqlFsEnabled => true;
        protected override bool SqlFsUsed => false;
        protected override Type ExpectedExternalBlobProviderType => null;
        protected override Type ExpectedMetadataProviderType => typeof(MsSqlBlobMetaDataProvider);
        protected override Type ExpectedBlobProviderDataType => typeof(BuiltinBlobProviderData);
        protected internal override void ConfigureMinimumSizeForFileStreamInBytes(int newValue, out int oldValue)
        {
            // do nothing
            oldValue = 0;
        }

        [ClassCleanup]
        public static void CleanupClass()
        {
            TearDown(typeof(BuiltInFsTests));
        }

        /* ==================================================== Test cases */

        [TestMethod]
        public void Blob_BuiltInFS_CreateFileSmall()
        {
            TestCase_CreateFileSmall();
        }
        [TestMethod]
        public void Blob_BuiltInFS_CreateFileBig()
        {
            TestCase_CreateFileBig();
        }

        [TestMethod]
        public void Blob_BuiltInFS_UpdateFileSmallEmpty()
        {
            TestCase_UpdateFileSmallEmpty();
        }
        [TestMethod]
        public void Blob_BuiltInFS_UpdateFileBigEmpty()
        {
            TestCase_UpdateFileBigEmpty();
        }
        [TestMethod]
        public void Blob_BuiltInFS_UpdateFileSmallSmall()
        {
            TestCase_UpdateFileSmallSmall();
        }
        [TestMethod]
        public void Blob_BuiltInFS_UpdateFileSmallBig()
        {
            TestCase_UpdateFileSmallBig();
        }
        [TestMethod]
        public void Blob_BuiltInFS_UpdateFileBigSmall()
        {
            TestCase_UpdateFileBigSmall();
        }
        [TestMethod]
        public void Blob_BuiltInFS_UpdateFileBigBig()
        {
            TestCase_UpdateFileBigBig();
        }

        [TestMethod]
        public void Blob_BuiltInFS_WriteChunksSmall()
        {
            TestCase_WriteChunksSmall();
        }
        [TestMethod]
        public void Blob_BuiltInFS_WriteChunksBig()
        {
            TestCase_WriteChunksBig();
        }

        [TestMethod]
        public void Blob_BuiltInFs_DeleteBinaryPropertySmall()
        {
            TestCase_DeleteBinaryPropertySmall();
        }
        [TestMethod]
        public void Blob_BuiltInFs_DeleteBinaryPropertyBig()
        {
            TestCase_DeleteBinaryPropertyBig();
        }

        [TestMethod]
        public void Blob_BuiltInFs_CopyfileRowSmall()
        {
            TestCase_CopyfileRowSmall();
        }
        [TestMethod]
        public void Blob_BuiltInFs_CopyfileRowBig()
        {
            TestCase_CopyfileRowBig();
        }

        [TestMethod]
        public void Blob_BuiltInFs_BinaryCacheEntitySmall()
        {
            TestCase_BinaryCacheEntitySmall();
        }
        [TestMethod]
        public void Blob_BuiltInFs_BinaryCacheEntityBig()
        {
            TestCase_BinaryCacheEntityBig();
        }


        [TestMethod]
        public void Blob_BuiltInFs_DeleteSmall()
        {
            TestCase_DeleteSmall();
        }
        [TestMethod]
        public void Blob_BuiltInFs_DeleteBig()
        {
            TestCase_DeleteBig();
        }
    }
}
