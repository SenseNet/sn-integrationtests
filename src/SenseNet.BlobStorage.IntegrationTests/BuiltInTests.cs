using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;

namespace SenseNet.BlobStorage.IntegrationTests
{
    [TestClass]
    public class BuiltInTests : BlobStorageIntegrationTests
    {
        protected override string DatabaseName => "sn7blobtests_builtin";
        protected override bool SqlFsEnabled => false;
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
            TearDown(typeof(BuiltInTests));
        }

        /* ==================================================== Test cases */

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_CreateFileSmall()
        {
            TestCase_CreateFileSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_CreateFileBig()
        {
            TestCase_CreateFileBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_UpdateFileSmallEmpty()
        {
            TestCase_UpdateFileSmallEmpty();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_UpdateFileBigEmpty()
        {
            TestCase_UpdateFileBigEmpty();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_UpdateFileSmallSmall()
        {
            TestCase_UpdateFileSmallSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_UpdateFileSmallBig()
        {
            TestCase_UpdateFileSmallBig();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_UpdateFileBigSmall()
        {
            TestCase_UpdateFileBigSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_UpdateFileBigBig()
        {
            TestCase_UpdateFileBigBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_WriteChunksSmall()
        {
            TestCase_WriteChunksSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_WriteChunksBig()
        {
            TestCase_WriteChunksBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_DeleteBinaryPropertySmall()
        {
            TestCase_DeleteBinaryPropertySmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_DeleteBinaryPropertyBig()
        {
            TestCase_DeleteBinaryPropertyBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_CopyfileRowSmall()
        {
            TestCase_CopyfileRowSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_CopyfileRowBig()
        {
            TestCase_CopyfileRowBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_BinaryCacheEntitySmall()
        {
            TestCase_BinaryCacheEntitySmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_BinaryCacheEntityBig()
        {
            TestCase_BinaryCacheEntityBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_DeleteSmall_Maintenance()
        {
            TestCase_DeleteSmall();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_DeleteBig_Maintenance()
        {
            TestCase_DeleteBig();
        }

        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_DeletionPolicy_Default()
        {
            TestCase_DeletionPolicy_Default();
        }
        // /*ok*/ [TestMethod]
        public void Blob_BuiltIn_DeletionPolicy_Immediately()
        {
            TestCase_DeletionPolicy_Immediately();
        }
        //// /*ok*/ [TestMethod]
        public void Blob_BuiltIn_DeletionPolicy_BackgroundImmediately()
        {
            // This test cannot be executed well because the background threading does not work.
            TestCase_DeletionPolicy_BackgroundImmediately();
        }
    }
}
