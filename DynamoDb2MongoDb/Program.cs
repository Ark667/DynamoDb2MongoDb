namespace DynamoDb2MongoDb
{
    using CommandLine;
    using System;

    /// <summary>
    /// Defines the <see cref="Program" />.
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// The Main.
        /// </summary>
        /// <param name="args">The args<see cref="string[]"/>.</param>
        internal static int Main(string[] args)
        {
            try
            {
                return Parser.Default.ParseArguments<CopyOptions>(args)
                    .MapResult(
                        (CopyOptions opts) => CopyOptions.Copy(opts),
                        errs => 1
                    );
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }
    }
}
