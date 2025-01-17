using System.Text.Json;
using GlitterBucket.Shared;
using Nest;

namespace GlitterBucket.ElasticSearchStorage
{
    public class ElasticSearchStorageClient : IStorageClient
    {
        private readonly ElasticClient _client;

        public ElasticSearchStorageClient(ElasticClient client)
        {
            _client = client;
        }

        private const string IndexPrefix = "glitteraudit";

        public static readonly Guid FieldIdEditor = new Guid("badd9cf9-53e0-4d0c-bcc0-2d784c282f6a");

        private string IndexName => $"{IndexPrefix}-{DateTime.UtcNow:yyyy.MM}";

        public async Task Add(string sitecoreInstanceId, SitecoreWebHookModel model, string? raw = null)
        {
            if (model == null) throw new ArgumentNullException(nameof(model));

            var now = DateTime.UtcNow;

            if (raw == null)
            {
                raw = await Serialize(model) ?? throw new ArgumentException("model was unexpected serialized to null");
            }

            var indexName = IndexName;
            await EnsureIndex(indexName);

            var fieldIds = model.Changes?.FieldChanges?.Select(x => x.FieldId).ToArray() ?? Array.Empty<Guid>();
            var userName = model.Changes?.FieldChanges?.FirstOrDefault(x => x.FieldId == FieldIdEditor)?.Value;
            var changedFields = await Serialize(model.Changes?.FieldChanges?
                .Select(x => new { field = x.FieldId, from = x.OriginalValue, to = x.Value }).ToArray());
            var fields = new IndexChangeModel
            {
                Timestamp = now,
                EventName = model.EventName,
                Raw = raw,
                ItemId = model.Item?.Id ?? Guid.Empty,
                Version = model.Item?.Version ?? 0,
                ParentId = model.Item?.ParentId ?? Guid.Empty,
                Language = model.Item?.Language,
                SitecoreInstance = sitecoreInstanceId,
                FieldIds = fieldIds,
                ChangedFields = changedFields,
                User = userName,
            };
            var response = await _client.CreateAsync(fields, opt => opt.Index(indexName).Id(Guid.NewGuid()));
            if (!response.IsValid)
            {
                throw response.OriginalException;
            }
        }

        public async Task<IEnumerable<IndexChangeModel>> GetByItemId(Guid itemId)
        {
            var result = await _client.SearchAsync<IndexChangeModel>(s =>
                s
                    .AllIndices()
                    .Query(q => q.Term(f => f.ItemId.Suffix("keyword"), itemId.ToString("D")))
                    .Sort(o => o.Descending(f => f.Timestamp))
                    .Size(10)
                );
            return result.Hits.Select(hit => hit.Source);
        }

        public async Task<IEnumerable<IndexChangeModel>> GetByItem(Guid itemId, string language, int? version)
        {
            var result = await _client.SearchAsync<IndexChangeModel>(s =>
                s.AllIndices()
                .Query(query => query
                    .Bool(b => b
                        .Filter(
                                q => q.Term(f => f.ItemId.Suffix("keyword"), itemId.ToString("D")),
                                q => q.Term(f => f.Language.Suffix("keyword"), language),
                                q => q.Term(f => f.Version, version)
                            )
                        )
                    )
                .Fields(fl => fl.Fields(f => f.Timestamp, f => f.User, f => f.FieldIds))
                .Sort(o => o.Descending(f => f.Timestamp))
            );
            return result.Hits.Select(hit => hit.Source).Where(x => x.ItemId == itemId && x.Language == language && x.Version == version);
        }

        private static async Task<string?> Serialize<T>(T model)
        {
            if (model == null)
            {
                return null;
            }
            using var stream = new MemoryStream();
            await JsonSerializer.SerializeAsync(stream, model);
            using var reader = new StreamReader(stream);
            var result = await reader.ReadToEndAsync();
            return result;
        }

        public async Task RecreateIndex()
        {
            var indexName = IndexName;
            await _client.Indices.DeleteAsync(new DeleteIndexRequest(Indices.Parse((indexName))));
            await EnsureIndex(indexName);
        }

        private async Task EnsureIndex(string indexName)
        {
            var response = await _client.Indices.CreateAsync(indexName, i => i
                .Settings(se => se
                    .NumberOfReplicas(1)
                )
            );

            if (!response.IsValid)
            {
                // Ignore if index already exists
                if (response.ServerError?.Status != 400)
                {
                    throw response.OriginalException;
                }
            }
        }


    }
}
