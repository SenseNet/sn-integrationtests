﻿using System;
using System.Linq;
using System.Threading;
using Tasks = System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.ContentRepository.Storage.Security;
using SenseNet.IntegrationTests.Common;
using SenseNet.IntegrationTests.Common.Implementations;

namespace SenseNet.ContentRepository.IntegrationTests
{
    [TestClass]
    public class AccessTokenTests : MsSqlIntegrationTestBase
    {
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUser()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMinutes(10);

                // ACTION
                var token = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ASSERT
                Assert.IsTrue(token.Id > 0);
                Assert.IsNotNull(token.Value);
                Assert.AreEqual(userId, token.UserId);
                Assert.AreEqual(0, token.ContentId);
                Assert.IsNull(token.Feature);
                Assert.IsTrue((DateTime.UtcNow - token.CreationDate).TotalMilliseconds < 1000);
                Assert.IsTrue((token.ExpirationDate - DateTime.UtcNow - timeout).TotalMilliseconds < 1000);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUser_ValueLength()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMinutes(10);

                // ACTION
                var token = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ASSERT
                Assert.IsTrue(token.Value.Length >= 50);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUser_Twice()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMinutes(10);

                // ACTION
                var token1 = await AccessTokenVault.CreateTokenAsync(userId, timeout);
                var token2 = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ASSERT
                Assert.AreNotEqual(token1.Id, token2.Id);
                Assert.AreNotEqual(token1.Value, token2.Value);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUserAndContent()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var contentId = 142;
                var timeout = TimeSpan.FromMinutes(10);

                // ACTION
                var token = await AccessTokenVault.CreateTokenAsync(userId, timeout, contentId);

                // ASSERT
                Assert.IsTrue(token.Id > 0);
                Assert.IsNotNull(token.Value);
                Assert.AreEqual(userId, token.UserId);
                Assert.AreEqual(contentId, token.ContentId);
                Assert.IsNull(token.Feature);
                Assert.IsTrue((DateTime.UtcNow - token.CreationDate).TotalMilliseconds < 1000);
                Assert.IsTrue((token.ExpirationDate - DateTime.UtcNow - timeout).TotalMilliseconds < 1000);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUserAndFeature()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var feature = "Feature1";
                var timeout = TimeSpan.FromMinutes(10);

                // ACTION
                var token = await AccessTokenVault.CreateTokenAsync(userId, timeout, 0, feature);

                // ASSERT
                Assert.IsTrue(token.Id > 0);
                Assert.IsNotNull(token.Value);
                Assert.AreEqual(userId, token.UserId);
                Assert.AreEqual(0, token.ContentId);
                Assert.AreEqual(feature, token.Feature);
                Assert.IsTrue((DateTime.UtcNow - token.CreationDate).TotalMilliseconds < 1000);
                Assert.IsTrue((token.ExpirationDate - DateTime.UtcNow - timeout).TotalMilliseconds < 1000);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUserContentAndFeature()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var contentId = 142;
                var feature = "Feature1";
                var timeout = TimeSpan.FromMinutes(10);

                // ACTION
                var token = await AccessTokenVault.CreateTokenAsync(userId, timeout, contentId, feature);

                // ASSERT
                Assert.IsTrue(token.Id > 0);
                Assert.IsNotNull(token.Value);
                Assert.AreEqual(userId, token.UserId);
                Assert.AreEqual(contentId, token.ContentId);
                Assert.AreEqual(feature, token.Feature);
                Assert.IsTrue((DateTime.UtcNow - token.CreationDate).TotalMilliseconds < 1000);
                Assert.IsTrue((token.ExpirationDate - DateTime.UtcNow - timeout).TotalMilliseconds < 1000);
            });
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUser()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMinutes(10);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ACTION
                var token = await AccessTokenVault.GetTokenAsync(savedToken.Value);

                // ASSERT
                AssertTokensAreEqual(savedToken, token);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUserAndContent()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var contentId = 142;
                var timeout = TimeSpan.FromMinutes(10);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout, contentId);

                // ACTION
                var token = await AccessTokenVault.GetTokenAsync(savedToken.Value, contentId);

                // ASSERT
                AssertTokensAreEqual(savedToken, token);
                Assert.IsNull(await AccessTokenVault.GetTokenAsync(savedToken.Value));
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUserAndFeature()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var feature = "Feature1";
                var timeout = TimeSpan.FromMinutes(10);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout, 0, feature);

                // ACTION
                var token = await AccessTokenVault.GetTokenAsync(savedToken.Value, 0, feature);

                // ASSERT
                AssertTokensAreEqual(savedToken, token);
                Assert.IsNull(await AccessTokenVault.GetTokenAsync(savedToken.Value));
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUserContentAndFeature()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var contentId = 142;
                var feature = "Feature1";
                var timeout = TimeSpan.FromMinutes(10);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout, contentId, feature);

                // ACTION
                var token = await AccessTokenVault.GetTokenAsync(savedToken.Value, contentId, feature);

                // ASSERT
                AssertTokensAreEqual(savedToken, token);
                Assert.IsNull(await AccessTokenVault.GetTokenAsync(savedToken.Value));
                Assert.IsNull(await AccessTokenVault.GetTokenAsync(savedToken.Value, 0, feature));
                Assert.IsNull(await AccessTokenVault.GetTokenAsync(savedToken.Value, contentId));
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_Expired()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMilliseconds(1);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ACTION
                Thread.Sleep(10);
                var token = await AccessTokenVault.GetTokenAsync(savedToken.Value);

                // ASSERT
                Assert.IsNull(token);
            });
        }

        [TestMethod]
        public async Tasks.Task AccessToken_GetByUser()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var contentId = 142;
                var feature = "Feature1";
                var timeout = TimeSpan.FromMinutes(10);
                var shortTimeout = TimeSpan.FromSeconds(1);
                var savedTokens = new[]
                {
                    await AccessTokenVault.CreateTokenAsync(userId, timeout),
                    await AccessTokenVault.CreateTokenAsync(userId, timeout, contentId),
                    await AccessTokenVault.CreateTokenAsync(userId, timeout, 0, feature),
                    await AccessTokenVault.CreateTokenAsync(userId, timeout, contentId, feature),
                    await AccessTokenVault.CreateTokenAsync(userId, shortTimeout),
                    await AccessTokenVault.CreateTokenAsync(userId, shortTimeout, contentId),
                    await AccessTokenVault.CreateTokenAsync(userId, shortTimeout, 0, feature),
                    await AccessTokenVault.CreateTokenAsync(userId, shortTimeout, contentId, feature),
                };

                // ACTION-1
                var tokens = await AccessTokenVault.GetAllTokensAsync(userId, CancellationToken.None);

                // ASSERT-1
                Assert.AreEqual(
                    string.Join(",", savedTokens.OrderBy(x => x.Id).Select(x => x.Id.ToString())),
                    string.Join(",", tokens.OrderBy(x => x.Id).Select(x => x.Id.ToString())));

                // ACTION-2
                Thread.Sleep(1100);
                tokens = await AccessTokenVault.GetAllTokensAsync(userId, CancellationToken.None);

                // ASSERT-2
                // The last 4 tokens are expired
                Assert.AreEqual(
                    string.Join(",", savedTokens.Take(4).OrderBy(x => x.Id).Select(x => x.Id.ToString())),
                    string.Join(",", tokens.OrderBy(x => x.Id).Select(x => x.Id.ToString())));
            });
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Exists()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMinutes(10);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ACTION
                var isExists = await AccessTokenVault.TokenExistsAsync(savedToken.Value);

                // ASSERT
                Assert.IsTrue(isExists);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Exists_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);

                // ACTION
                var isExists = await AccessTokenVault.TokenExistsAsync("asdf");

                // ASSERT
                Assert.IsFalse(isExists);
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Exists_Expired()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMilliseconds(1);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ACTION
                Thread.Sleep(1100);
                var isExists = await AccessTokenVault.TokenExistsAsync(savedToken.Value);

                // ASSERT
                Assert.IsFalse(isExists);
            });
        }

        [TestMethod]
        public async Tasks.Task AccessToken_AssertExists()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMinutes(10);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ACTION
                AccessTokenVault.AssertTokenExists(savedToken.Value);

                //Assert.AllRight() :)
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_AssertExists_Missing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                await AccessTokenVault.AssertTokenExistsAsync("asdf");
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_AssertExists_Expired()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMilliseconds(1);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ACTION
                Thread.Sleep(1100);
                await AccessTokenVault.AssertTokenExistsAsync(savedToken.Value);
            });
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Update()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMinutes(10.0d);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);
                Assert.IsTrue(savedToken.ExpirationDate < DateTime.UtcNow.AddMinutes(20.0d));

                // ACTION
                await AccessTokenVault.UpdateTokenAsync(savedToken.Value, DateTime.UtcNow.AddMinutes(30.0d), CancellationToken.None);

                // ASSERT
                var loadedToken = await AccessTokenVault.GetTokenAsync(savedToken.Value);
                Assert.IsNotNull(loadedToken);
                Assert.IsTrue(loadedToken.ExpirationDate > DateTime.UtcNow.AddMinutes(20.0d));
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_UpdateMissing()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                await AccessTokenVault.UpdateTokenAsync("asdf", DateTime.UtcNow.AddMinutes(30.0d), CancellationToken.None);
            });
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_UpdateExpired()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId = 42;
                var timeout = TimeSpan.FromMilliseconds(1);
                var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

                // ACTION
                Thread.Sleep(1100);
                await AccessTokenVault.UpdateTokenAsync(savedToken.Value, DateTime.UtcNow.AddMinutes(30.0d), CancellationToken.None);
            });
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Delete_Token()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId1 = 42;
                var userId2 = 43;
                var timeout = TimeSpan.FromMinutes(10);
                var shortTimeout = TimeSpan.FromSeconds(1);
                var savedTokens = new[]
                {
                    await AccessTokenVault.CreateTokenAsync(userId1, timeout),
                    await AccessTokenVault.CreateTokenAsync(userId1, shortTimeout),
                    await AccessTokenVault.CreateTokenAsync(userId2, timeout),
                    await AccessTokenVault.CreateTokenAsync(userId2, shortTimeout),
                };

                // ACTION
                Thread.Sleep(1100);
                await AccessTokenVault.DeleteTokenAsync(savedTokens[0].Value, CancellationToken.None);
                await AccessTokenVault.DeleteTokenAsync(savedTokens[3].Value, CancellationToken.None);

                // ASSERT
                Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[0].Id, CancellationToken.None));
                Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[1].Id, CancellationToken.None));
                Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[2].Id, CancellationToken.None));
                Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[3].Id, CancellationToken.None));
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Delete_ByUser()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId1 = 42;
                var userId2 = 43;
                var timeout = TimeSpan.FromMinutes(10);
                var shortTimeout = TimeSpan.FromSeconds(1);
                var savedTokens = new[]
                {
                    await AccessTokenVault.CreateTokenAsync(userId1, timeout),
                    await AccessTokenVault.CreateTokenAsync(userId1, shortTimeout),
                    await AccessTokenVault.CreateTokenAsync(userId2, timeout),
                    await AccessTokenVault.CreateTokenAsync(userId2, shortTimeout),
                };

                // ACTION
                Thread.Sleep(1100);
                await AccessTokenVault.DeleteTokensByUserAsync(userId1, CancellationToken.None);

                // ASSERT
                Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[0].Id, CancellationToken.None));
                Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[1].Id, CancellationToken.None));
                Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[2].Id, CancellationToken.None));
                Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[3].Id, CancellationToken.None));
            });
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Delete_ByContent()
        {
            await NoRepositoryIntegrtionTest(async () =>
            {
                await AccessTokenVault.DeleteAllAccessTokensAsync(CancellationToken.None);
                var userId1 = 42;
                var userId2 = 43;
                var contentId1 = 142;
                var contentId2 = 143;
                var timeout = TimeSpan.FromMinutes(10);
                var shortTimeout = TimeSpan.FromSeconds(1);
                var savedTokens = new[]
                {
                    await AccessTokenVault.CreateTokenAsync(userId1, timeout, contentId1),
                    await AccessTokenVault.CreateTokenAsync(userId1, shortTimeout, contentId2),
                    await AccessTokenVault.CreateTokenAsync(userId2, timeout, contentId1),
                    await AccessTokenVault.CreateTokenAsync(userId2, shortTimeout, contentId2),
                };

                // ACTION
                Thread.Sleep(1100);
                await AccessTokenVault.DeleteTokensByContentAsync(contentId1, CancellationToken.None);

                // ASSERT
                Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[0].Id, CancellationToken.None));
                Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[1].Id, CancellationToken.None));
                Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[2].Id, CancellationToken.None));
                Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[3].Id, CancellationToken.None));
            });
        }

        /* ===================================================================================== */

        private void AssertTokensAreEqual(AccessToken expected, AccessToken actual)
        {
            Assert.AreNotSame(expected, actual);
            Assert.AreEqual(expected.Id, actual.Id);
            Assert.AreEqual(expected.Value, actual.Value);
            Assert.AreEqual(expected.UserId, actual.UserId);
            Assert.AreEqual(expected.ContentId, actual.ContentId);
            Assert.AreEqual(expected.Feature, actual.Feature);
            Assert.AreEqual(expected.CreationDate.ToString("u"), actual.CreationDate.ToString("u"));
            Assert.AreEqual(expected.ExpirationDate.ToString("u"), actual.ExpirationDate.ToString("u"));
        }
    }
}