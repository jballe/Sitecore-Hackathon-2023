using Nest;

namespace GlitterBucket.ElasticSearchStorage
{
    [ElasticsearchType]
    public class IndexChangeModel
    {
        public DateTime Timestamp { get; init; }
        
        [Keyword] 
        public string EventName { get; init; } = "DummyEvent";

        [Keyword]
        public Guid ItemId { get; init; }
        
        [Keyword]
        public Guid ParentId { get; init; }
        
        public int Version { get; set; }
        
        [Keyword]
        public string? Language { get; set; }
        
        public Guid[]? FieldIds { get; init; }
        
        public string Raw { get; init; }
        
        [Keyword]
        public string? User { get; init; }

        [Keyword]
        public string SitecoreInstance { get; init; } = "DefaultInstance";

        public string? ChangedFields { get; init; }
    }
}
