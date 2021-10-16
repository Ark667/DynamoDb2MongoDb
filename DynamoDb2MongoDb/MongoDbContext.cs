namespace DynamoDb2MongoDb
{
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization;
    using MongoDB.Driver;

    /// <summary>
    /// Defines the <see cref="MongoDbContext" />.
    /// </summary>
    public class MongoDbContext
    {
        /// <summary>
        /// Gets or sets the Database.
        /// </summary>
        public IMongoDatabase Database { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="MongoDbContext"/> class.
        /// </summary>
        /// <param name="connectionstring">The connectionstring<see cref="string"/>.</param>
        public MongoDbContext(string connectionstring)
        {
            var url = new MongoUrl(connectionstring);
            var client = new MongoClient(url);
            Database = client.GetDatabase(url.DatabaseName);
        }

        /// <summary>
        /// The Save.
        /// </summary>
        /// <param name="collectionName">The tableName<see cref="string"/>.</param>
        /// <param name="json">The json<see cref="string"/>.</param>
        public void Save(string collectionName, string json)
        {
            var mongoCollection = Database.GetCollection<BsonDocument>(collectionName);
            var document = BsonSerializer.Deserialize<BsonDocument>(json);
            mongoCollection.InsertOne(document);
        }

        /// <summary>
        /// The Count.
        /// </summary>
        /// <param name="collectionName">The tableName<see cref="string"/>.</param>
        /// <returns>The <see cref="long"/>.</returns>
        public long Count(string collectionName)
        {
            var mongoCollection = Database.GetCollection<BsonDocument>(collectionName);
            return mongoCollection.CountDocuments(FilterDefinition<BsonDocument>.Empty);
        }

        /// <summary>
        /// The Drop.
        /// </summary>
        /// <param name="collectionName">The tableName<see cref="string"/>.</param>
        public void Drop(string collectionName)
        {
            Database.DropCollection(collectionName);
        }
    }
}
