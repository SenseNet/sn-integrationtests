using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.MsSqlFsBlobProvider;

namespace SenseNet.BlobStorage.IntegrationTests
{
    [TestClass]
    public class SqFsTests : BlobStorageIntegrationTests
    {
        protected override string DatabaseName => "sn7blobtests_sqlfs";

        protected override bool SqlFsEnabled => true;
        protected override bool SqlFsUsed => true;
        protected override Type ExpectedExternalBlobProviderType => typeof(SqlFileStreamBlobProvider);
        protected override Type ExpectedMetadataProviderType => typeof(SqlFileStreamBlobMetaDataProvider);
        protected override Type ExpectedBlobProviderDataType => typeof(SqlFileStreamBlobProviderData);
        protected internal override void ConfigureMinimumSizeForFileStreamInBytes(int newValue, out int oldValue)
        {
            oldValue = Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes;
            Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes = newValue;
        }


        [ClassCleanup]
        public static void CleanupClass()
        {
            TearDown(typeof(SqFsTests));
        }

        [TestMethod]
        public void Blob_SqlFS_CreateFileSmall()
        {
            TestCase_CreateFileSmall();
        }
        [TestMethod]
        public void Blob_SqlFS_CreateFileBig()
        {
            TestCase_CreateFileBig();
        }

        [TestMethod]
        public void Blob_SqlFS_UpdateFileSmallSmall()
        {
            TestCase_UpdateFileSmallSmall();
        }
        [TestMethod]
        public void Blob_SqlFS_UpdateFileSmallBig()
        {
            TestCase_UpdateFileSmallBig();
        }
        [TestMethod]
        public void Blob_SqlFS_UpdateFileBigSmall()
        {
            TestCase_UpdateFileBigSmall();
        }
        [TestMethod]
        public void Blob_SqlFS_UpdateFileBigBig()
        {
            TestCase_UpdateFileBigBig();
        }

        [TestMethod]
        public void Blob_SqlFs_WriteChunksSmall()
        {
            TestCase_WriteChunksSmall();
        }
        [TestMethod]
        public void Blob_SqlFs_WriteChunksBig()
        {
            TestCase_WriteChunksBig();
        }

        [TestMethod]
        public void Blob_SqlFs_DeleteBinaryPropertySmall()
        {
            TestCase_DeleteBinaryPropertySmall();
        }
        [TestMethod]
        public void Blob_SqlFs_DeleteBinaryPropertyBig()
        {
            TestCase_DeleteBinaryPropertyBig();
        }

        [TestMethod]
        public void Blob_SqlFs_CopyfileRowSmall()
        {
            TestCase_CopyfileRowSmall();
        }
        [TestMethod]
        public void Blob_SqlFs_CopyfileRowBig()
        {
            TestCase_CopyfileRowBig();
        }

        [TestMethod]
        public void Blob_SqlFs_BinaryCacheEntitySmall()
        {
            TestCase_BinaryCacheEntitySmall();
        }
        [TestMethod]
        public void Blob_SqlFs_BinaryCacheEntityBig()
        {
            TestCase_BinaryCacheEntityBig();
        }

        [TestMethod]
        public void Blob_SqlFs_DeleteSmall()
        {
            TestCase_DeleteSmall();
        }
        [TestMethod]
        public void Blob_SqlFs_DeleteBig()
        {
            TestCase_DeleteBig();
        }
    }
}
