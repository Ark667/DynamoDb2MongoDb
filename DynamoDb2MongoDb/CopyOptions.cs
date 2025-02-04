using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using CommandLine;
using CommandLine.Text;
using DynamoDb2MongoDb.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace DynamoDb2MongoDb;

[Verb("copy", HelpText = "Copy data from source dynmoDb table to target MongoDb collection.")]
public class CopyOptions
{
    [Usage(ApplicationAlias = "dynamodb2mongodb")]
    public static IEnumerable<Example> Examples
    {
        get
        {
            return new List<Example>()
            {
                new Example(
                    "Copy data from source DynamoDb table to target MongoDb collection",
                    new CopyOptions()
                    {
                        DynamoAccessKey = "***",
                        DynamoSecretAccesKey = "***",
                        DynamoRegion = "eu-west-3",
                        DynamoTable = "mytable",
                        MongoConnectionString = "mongodb://localhost:27027/mydatabase",
                        MongoCollection = "mycollection",
                    }
                )
            };
        }
    }

    [Option(
        "dynamoaccesskey",
        HelpText = "Source AWS access key with DynamoDb access.",
        Required = true
    )]
    public string DynamoAccessKey { get; set; }

    [Option(
        "dynamosecretaccesskey",
        HelpText = "Source AWS secret access key with DynamoDb access.",
        Required = true
    )]
    public string DynamoSecretAccesKey { get; set; }

    [Option("dynamoregion", HelpText = "Source AWS region with DynamoDb service.", Required = true)]
    public string DynamoRegion { get; set; }

    [Option("dynamotable", HelpText = "Source DynamoDb table.", Required = true)]
    public string DynamoTable { get; set; }

    [Option(
        "mongoconnectionstring",
        HelpText = "Target MongoDb connection string.",
        Required = true
    )]
    public string MongoConnectionString { get; set; }

    [Option(
        "mongocollection",
        HelpText = "Target MongoDb collection. Uses source DynamoDb table as default."
    )]
    public string MongoCollection { get; set; }

    /// <summary>
    /// The Build.
    /// </summary>
    /// <param name="opts">The opts<see cref="CopyOptions"/>.</param>
    /// <returns>The <see cref="int"/>.</returns>
    public static int Copy(CopyOptions opts)
    {
        try
        {
            // Load connections
            var dynamodbContext = new AmazonDynamoDBClient(
                opts.DynamoAccessKey,
                opts.DynamoSecretAccesKey,
                RegionEndpoint.GetBySystemName(opts.DynamoRegion)
            );
            var mongodbContext = new MongoDbContext(opts.MongoConnectionString);
            if (string.IsNullOrEmpty(opts.MongoCollection))
                opts.MongoCollection = opts.DynamoTable;
            mongodbContext.Drop(opts.MongoCollection);
            Console.WriteLine("Databases connected");

            // Get Dynamo data
            long count = 0;
            var table = Table.LoadTable(dynamodbContext, opts.DynamoTable);
            var search = table.Scan(new ScanFilter());

            do
            {
                Console.WriteLine($"Scanning DynamoDb {opts.DynamoTable} from {count}...");
                var dynamoData = search.GetNextSetAsync().Result;
                count += dynamoData.Count;

                // Save on Mongo
                foreach (var item in dynamoData)
                {
                    var data = GetDynamoDocument(item);
                    try
                    {
                        mongodbContext.Save(opts.MongoCollection, data);
                        Console.WriteLine(data);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{ex} - {data}");
                    }
                }
            } while (!search.IsDone);

            var mongodbCount = mongodbContext.Count(opts.MongoCollection);
            Console.WriteLine($"{count} items in DynamoDb {opts.DynamoTable} table");
            Console.WriteLine($"{mongodbCount} items in MongoDb {opts.MongoCollection} collection");
            Console.WriteLine(mongodbCount == count ? "Ok!" : "Fail");

            return mongodbCount == count ? 0 : 2;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            return 1;
        }
    }

    /// <summary>
    /// The GetDynamoDocument.
    /// </summary>
    /// <param name="item">The item<see cref="Document"/>.</param>
    /// <returns>The <see cref="string"/>.</returns>
    private static string GetDynamoDocument(Document item)
    {
        var memoryStream = new MemoryStream();
        var writer = new Utf8JsonWriter(memoryStream);

        var placeholders = new Dictionary<string, string>();

        writer.WriteStartObject();

        foreach (var value in item)
        {
            // Number
            if (
                value.Value is Primitive
                && ((Primitive)value.Value).Type == DynamoDBEntryType.Numeric
            )
            {
                writer.WriteNumber(value.Key, Convert.ToDecimal(value.Value));
                continue;
            }

            // String array
            if (
                value.Value is PrimitiveList
                && ((PrimitiveList)value.Value).Type == DynamoDBEntryType.String
            )
            {
                writer.WritePropertyName(value.Key);
                writer.WriteStartArray();
                foreach (var v in ((PrimitiveList)value.Value).AsListOfString())
                    writer.WriteStringValue(v);
                writer.WriteEndArray();
                continue;
            }

            // String & Nested Json
            if (
                value.Value is Primitive
                && ((Primitive)value.Value).Type == DynamoDBEntryType.String
            )
            {
                if (IsJson(value.Value))
                {
                    var placeholder = Guid.NewGuid().ToString();
                    placeholders.Add(placeholder, Convert.ToString(value.Value));
                    writer.WriteString(value.Key, placeholder);
                    continue;
                }
                else
                {
                    writer.WriteString(value.Key, value.Value);
                    continue;
                }
            }

            throw new ArgumentException($"Data loss for {value.Key}");
        }

        writer.WriteEndObject();
        writer.Flush();

        memoryStream.Position = 0;
        var result = new StreamReader(memoryStream).ReadToEnd();

        foreach (var placeholder in placeholders)
            result = result.Replace($"\"{placeholder.Key}\"", placeholder.Value);

        return result;
    }

    /// <summary>
    /// Check if input string is Json format, looking for starting '{' and ending '}'.
    /// </summary>
    /// <param name="input">The input<see cref="string"/>.</param>
    /// <returns>The <see cref="bool"/>.</returns>
    public static bool IsJson(string input)
    {
        input = input.Trim();

        if (
            (!input.StartsWith("{") || !input.EndsWith("}"))
            && (!input.StartsWith("[") || !input.EndsWith("]"))
        )
            return false;

        try
        {
            _ = System.Json.JsonValue.Parse(input);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
