using System;
using System.Linq;
using System.Threading;
using Tasks = System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.Configuration;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MsSqlClient;
using SenseNet.ContentRepository.Storage.Data.SqlClient;
using SenseNet.ContentRepository.Storage.Security;

namespace SenseNet.ContentRepository.IntegrationTests
{
    [TestClass]
    public class AccessTokenTests : IntegrationTestBase
    {
        [TestInitialize]
        public async Tasks.Task InitializeTest()
        {
            // set default implementation directly to avoid repository start
            Providers.Instance.DataProvider2 = new MsSqlDataProvider();
            DataStore.DataProvider.SetExtension(typeof(IAccessTokenDataProviderExtension), new SqlAccessTokenDataProvider());

            DataStore.Enabled = true;

            await AccessTokenVault.DeleteAllAccessTokensAsync();
        }

        [ClassCleanup]
        public static void CleanupClass()
        {
            TearDown();
        }

        /* ====================================================================== */

        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUser()
        {
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
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUser_ValueLength()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMinutes(10);

            // ACTION
            var token = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ASSERT
            Assert.IsTrue(token.Value.Length >= 50);
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUser_Twice()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMinutes(10);

            // ACTION
            var token1 = await AccessTokenVault.CreateTokenAsync(userId, timeout);
            var token2 = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ASSERT
            Assert.AreNotEqual(token1.Id, token2.Id);
            Assert.AreNotEqual(token1.Value, token2.Value);
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUserAndContent()
        {
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
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUserAndFeature()
        {
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
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Create_ForUserContentAndFeature()
        {
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
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUser()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMinutes(10);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ACTION
            var token = await AccessTokenVault.GetTokenAsync(savedToken.Value);

            // ASSERT
            AssertTokensAreEqual(savedToken, token);
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUserAndContent()
        {
            var userId = 42;
            var contentId = 142;
            var timeout = TimeSpan.FromMinutes(10);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout, contentId);

            // ACTION
            var token = await AccessTokenVault.GetTokenAsync(savedToken.Value, contentId);

            // ASSERT
            AssertTokensAreEqual(savedToken, token);
            Assert.IsNull(await AccessTokenVault.GetTokenAsync(savedToken.Value));
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUserAndFeature()
        {
            var userId = 42;
            var feature = "Feature1";
            var timeout = TimeSpan.FromMinutes(10);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout, 0, feature);

            // ACTION
            var token = await AccessTokenVault.GetTokenAsync(savedToken.Value, 0, feature);

            // ASSERT
            AssertTokensAreEqual(savedToken, token);
            Assert.IsNull(await AccessTokenVault.GetTokenAsync(savedToken.Value));
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_ForUserContentAndFeature()
        {
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
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Get_Expired()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMilliseconds(1);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ACTION
            Thread.Sleep(10);
            var token = await AccessTokenVault.GetTokenAsync(savedToken.Value);

            // ASSERT
            Assert.IsNull(token);
        }

        [TestMethod]
        public async Tasks.Task AccessToken_GetByUser()
        {
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
            var tokens = await AccessTokenVault.GetAllTokensAsync(userId);

            // ASSERT-1
            Assert.AreEqual(
                string.Join(",", savedTokens.OrderBy(x => x.Id).Select(x => x.Id.ToString())),
                string.Join(",", tokens.OrderBy(x => x.Id).Select(x => x.Id.ToString())));

            // ACTION-2
            Thread.Sleep(1100);
            tokens = await AccessTokenVault.GetAllTokensAsync(userId);

            // ASSERT-2
            // The last 4 tokens are expired
            Assert.AreEqual(
                string.Join(",", savedTokens.Take(4).OrderBy(x => x.Id).Select(x => x.Id.ToString())),
                string.Join(",", tokens.OrderBy(x => x.Id).Select(x => x.Id.ToString())));
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Exists()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMinutes(10);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ACTION
            var isExists = await AccessTokenVault.TokenExistsAsync(savedToken.Value);

            // ASSERT
            Assert.IsTrue(isExists);
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Exists_Missing()
        {
            // ACTION
            var isExists = await AccessTokenVault.TokenExistsAsync("asdf");

            // ASSERT
            Assert.IsFalse(isExists);
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Exists_Expired()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMilliseconds(1);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ACTION
            Thread.Sleep(1100);
            var isExists = await AccessTokenVault.TokenExistsAsync(savedToken.Value);

            // ASSERT
            Assert.IsFalse(isExists);
        }

        [TestMethod]
        public async Tasks.Task AccessToken_AssertExists()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMinutes(10);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ACTION
            AccessTokenVault.AssertTokenExists(savedToken.Value);

            //Assert.AllRight() :)
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_AssertExists_Missing()
        {
            await AccessTokenVault.AssertTokenExistsAsync("asdf");
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_AssertExists_Expired()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMilliseconds(1);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ACTION
            Thread.Sleep(1100);
            await AccessTokenVault.AssertTokenExistsAsync(savedToken.Value);
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Update()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMinutes(10.0d);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);
            Assert.IsTrue(savedToken.ExpirationDate < DateTime.UtcNow.AddMinutes(20.0d));

            // ACTION
            await AccessTokenVault.UpdateTokenAsync(savedToken.Value, DateTime.UtcNow.AddMinutes(30.0d));

            // ASSERT
            var loadedToken = await AccessTokenVault.GetTokenAsync(savedToken.Value);
            Assert.IsNotNull(loadedToken);
            Assert.IsTrue(loadedToken.ExpirationDate > DateTime.UtcNow.AddMinutes(20.0d));
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_UpdateMissing()
        {
            await AccessTokenVault.UpdateTokenAsync("asdf", DateTime.UtcNow.AddMinutes(30.0d));
        }
        [TestMethod]
        [ExpectedException(typeof(InvalidAccessTokenException))]
        public async Tasks.Task AccessToken_UpdateExpired()
        {
            var userId = 42;
            var timeout = TimeSpan.FromMilliseconds(1);
            var savedToken = await AccessTokenVault.CreateTokenAsync(userId, timeout);

            // ACTION
            Thread.Sleep(1100);
            await AccessTokenVault.UpdateTokenAsync(savedToken.Value, DateTime.UtcNow.AddMinutes(30.0d));
        }

        [TestMethod]
        public async Tasks.Task AccessToken_Delete_Token()
        {
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
            await AccessTokenVault.DeleteTokenAsync(savedTokens[0].Value);
            await AccessTokenVault.DeleteTokenAsync(savedTokens[3].Value);

            // ASSERT
            Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[0].Id));
            Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[1].Id));
            Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[2].Id));
            Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[3].Id));
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Delete_ByUser()
        {
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
            await AccessTokenVault.DeleteTokensByUserAsync(userId1);

            // ASSERT
            Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[0].Id));
            Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[1].Id));
            Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[2].Id));
            Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[3].Id));
        }
        [TestMethod]
        public async Tasks.Task AccessToken_Delete_ByContent()
        {
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
            await AccessTokenVault.DeleteTokensByContentAsync(contentId1);

            // ASSERT
            Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[0].Id));
            Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[1].Id));
            Assert.IsNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[2].Id));
            Assert.IsNotNull(await AccessTokenVault.GetTokenByIdAsync(savedTokens[3].Id));
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