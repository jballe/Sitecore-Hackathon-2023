
using Elasticsearch.Net;
using GlitterBucket.ElasticSearchStorage;
using GlitterBucket.Shared;
using Nest;
using Xunit;

namespace GlitterBucket.IntegrationTests.ElasticSearchStorage
{
    [CollectionDefinition(nameof(ElasticSearchCollectionDefinition), DisableParallelization = true)]
    public class ElasticSearchCollectionDefinition { }


    [Trait("Category", "Integration")]
    [Collection(nameof(ElasticSearchCollectionDefinition))]
    public class ElasticSearchStorageClientShould
    {
        public ElasticSearchStorageClientShould()
        {
            Client = new ElasticClient(
                new ConnectionSettings(
                    new SingleNodeConnectionPool(new Uri("https://elasticsearch.localhost"))
                    ).DisableDirectStreaming()
                );
        }

        public ElasticClient Client { get; set; }

        [Fact]
        public Task AddSimpleDocument()
        {
            var sut = new ElasticSearchStorageClient(Client);

            return sut.Add("123", new SitecoreWebHookModel
            {
                EventName = "Test"
            });
        }

        [Fact]
        public async Task AddAndFindByItemId()
        {
            var doc = CreateSampleDocument();
            var sut = new ElasticSearchStorageClient(Client);

            await sut.Add("123", doc);

            Thread.Sleep(TimeSpan.FromSeconds(5));

            var result = await sut.GetByItemId(doc.Item.Id);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
            Assert.Equal(1, result.Count());
        }

        [Fact]
        public async Task AddAndFindByItem()
        {
            var doc = CreateSampleDocument();
            var sut = new ElasticSearchStorageClient(Client);

            await sut.Add("123", doc);

            Thread.Sleep(TimeSpan.FromSeconds(5));

            var result = await sut.GetByItem(doc.Item.Id, doc.Item.Language, doc.Item.Version);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
            Assert.Equal(1, result.Count());
        }

        private SitecoreWebHookModel CreateSampleDocument()
        {
            var itemId = Guid.NewGuid();
            var instance = Guid.NewGuid();
            var language = "en";
            var version = 1;

            return new SitecoreWebHookModel
            {
                EventName = "Test",
                Item = new ItemModel
                {
                    Id = itemId,
                    Language = language,
                    Version = version,
                },
                Changes = new ItemChanges
                {
                    FieldChanges = new List<FieldChange>
                    {
                        new FieldChange
                        {
                            FieldId = ElasticSearchStorageClient.FieldIdEditor,
                            OriginalValue = @"sitecore\admin",
                            Value = @"sitecore\someone"
                        }
                    }

                }
            };
        }
    }
}
