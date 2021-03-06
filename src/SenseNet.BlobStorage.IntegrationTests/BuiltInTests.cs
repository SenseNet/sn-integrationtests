﻿using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data.SqlClient;

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

        [TestMethod]
        public void Blob_BuiltIn_CreateFileSmall()
        {
            TestCase_CreateFileSmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_CreateFileBig()
        {
            TestCase_CreateFileBig();
        }

        [TestMethod]
        public void Blob_BuiltIn_UpdateFileSmallEmpty()
        {
            TestCase_UpdateFileSmallEmpty();
        }
        [TestMethod]
        public void Blob_BuiltIn_UpdateFileBigEmpty()
        {
            TestCase_UpdateFileBigEmpty();
        }
        [TestMethod]
        public void Blob_BuiltIn_UpdateFileSmallSmall()
        {
            TestCase_UpdateFileSmallSmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_UpdateFileSmallBig()
        {
            TestCase_UpdateFileSmallBig();
        }
        [TestMethod]
        public void Blob_BuiltIn_UpdateFileBigSmall()
        {
            TestCase_UpdateFileBigSmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_UpdateFileBigBig()
        {
            TestCase_UpdateFileBigBig();
        }

        [TestMethod]
        public void Blob_BuiltIn_WriteChunksSmall()
        {
            TestCase_WriteChunksSmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_WriteChunksBig()
        {
            TestCase_WriteChunksBig();
        }

        [TestMethod]
        public void Blob_BuiltIn_DeleteBinaryPropertySmall()
        {
            TestCase_DeleteBinaryPropertySmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_DeleteBinaryPropertyBig()
        {
            TestCase_DeleteBinaryPropertyBig();
        }

        [TestMethod]
        public void Blob_BuiltIn_CopyfileRowSmall()
        {
            TestCase_CopyfileRowSmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_CopyfileRowBig()
        {
            TestCase_CopyfileRowBig();
        }

        [TestMethod]
        public void Blob_BuiltIn_BinaryCacheEntitySmall()
        {
            TestCase_BinaryCacheEntitySmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_BinaryCacheEntityBig()
        {
            TestCase_BinaryCacheEntityBig();
        }

        [TestMethod]
        public void Blob_BuiltIn_DeleteSmall()
        {
            TestCase_DeleteSmall();
        }
        [TestMethod]
        public void Blob_BuiltIn_DeleteBig()
        {
            TestCase_DeleteBig();
        }
    }
}
