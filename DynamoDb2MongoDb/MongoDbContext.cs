namespace DynamoDb2MongoDb
{
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization;
    using MongoDB.Driver;
    using System;

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
        /// Initializes a new instance of the <see cref="MongoDbContext"/> class. The specified database is used for operations, no as authentication as MongoDb default!.
        /// </summary>
        /// <param name="connectionstring">The connectionstring<see cref="string"/>.</param>
        public MongoDbContext(string connectionstring)
        {
            ParseConnectionString(connectionstring, out var clientConnectionstring, out var databaseName);

            var url = new MongoUrl(clientConnectionstring);
            var client = new MongoClient(url);
            Database = client.GetDatabase(databaseName);
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

        /// <summary>
        /// The ParseConnectionString. The default pourpose of database specification in connection string is for authentication, this class uses for
        /// for database operations for convenience, so the connection string is parsed to use on mongodb driver parameters as required.
        /// </summary>
        /// <param name="sourceConnectionstring">The connectionstring<see cref="string"/>.</param>
        /// <param name="connectionstring">The clientConnectionstring<see cref="string"/>.</param>
        /// <param name="databaseName">The databaseName<see cref="string"/>.</param>
        public static void ParseConnectionString(string sourceConnectionstring, out string connectionstring, out string databaseName)
        {
            try
            {
                var uri = new Uri(sourceConnectionstring);
                connectionstring = sourceConnectionstring.Replace(uri.LocalPath, string.Empty);
                databaseName = uri.LocalPath.Replace("/", string.Empty);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid mongodb connection string", ex);
            }

        }
    }
}
