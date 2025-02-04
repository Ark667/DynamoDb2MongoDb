using CommandLine;
using System;

namespace DynamoDb2MongoDb;

/// <summary>
/// Defines the <see cref="Program" />.
/// </summary>
internal class Program
{
    /// <summary>
    /// The Main.
    /// </summary>
    /// <param name="args">The args<see cref="string[]"/>.</param>
    /// <returns>The <see cref="int"/>.</returns>
    internal static int Main(string[] args)
    {
        try
        {
            return Parser.Default
                .ParseArguments<CopyOptions>(args)
                .MapResult((CopyOptions opts) => CopyOptions.Copy(opts), errs => 1);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            throw;
        }
    }
}
