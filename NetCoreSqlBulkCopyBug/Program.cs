using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Runtime.InteropServices.ComTypes;
using Microsoft.Extensions.CommandLineUtils;

namespace NetCoreSqlBulkCopyBug
{
    public class Program
    {
        #region Const

        public static readonly string IssueDescription = @"# Summary:

    Issue occurs in SqlBulkCopy when trying to copy data from one table to another
and there is an implicit cast of float/real to decimal SQL Sever types.


# Root cause:

    There is a special SQL Server specific type adjustment in SqlBulkCopy which is missing check for a NULL.
        ""case ValueMethod.SqlTypeSqlDouble:""
            ""value = new SqlDecimal(_SqlDataReaderRowSource.GetSqlDouble(sourceOrdinal).Value);""
    This line: new SqlDecimal(...).Value does not have a check for NULL.


# Source code: https://github.com/dotnet/corefx/blob/master/src/System.Data.SqlClient/src/System/Data/SqlClient/SqlBulkCopy.cs
# Source code listing:
    
namespace System.Data.SqlClient
    internal sealed class Result
        private object GetValueFromSourceRow(int destRowIndex, out bool isSqlType, out bool isDataFeed, out bool isNull)
        ...
                    // SqlDataReader-specific logic
                    else if (null != _SqlDataReaderRowSource)
                    {
                        if (_currentRowMetadata[destRowIndex].IsSqlType)
                        {
                            INullable value;
                            isSqlType = true;
                            isDataFeed = false;
                            switch (_currentRowMetadata[destRowIndex].Method)
                            {
                                case ValueMethod.SqlTypeSqlDecimal:
                                    value = _SqlDataReaderRowSource.GetSqlDecimal(sourceOrdinal);
                                    break;
                                case ValueMethod.SqlTypeSqlDouble:
                                    value = new SqlDecimal(_SqlDataReaderRowSource.GetSqlDouble(sourceOrdinal).Value);
                                    break;
                                case ValueMethod.SqlTypeSqlSingle:
                                    value = new SqlDecimal(_SqlDataReaderRowSource.GetSqlSingle(sourceOrdinal).Value);
                                    break;
                                default:
                                    Debug.Fail(string.Format(""Current column is marked as being a SqlType, but no SqlType compatible method was provided. Method: {0}"", _currentRowMetadata[destRowIndex].Method));
                                    value = (INullable)_SqlDataReaderRowSource.GetSqlValue(sourceOrdinal);
                                    break;
                            }

                            isNull = value.IsNull;
                            return value;
                        }
";
        #endregion

        #region Main
        public static int Main(string[] args)
        {
            var app = new CommandLineApplication { Name = "SqlBulkCopyBugExample" };
            app.HelpOption("-?|-h|--help");

            app.Command("intro", (command) =>
            {
                command.OnExecute(() =>
                {
                    Console.WriteLine(IssueDescription);
                    return 0;
                });
            });

            app.Command("reproduce", (command) =>
            {
                var connectionStringOption = command.Option("-c | --connectionString <connectionString>", "Connection to SQL Server DB.", CommandOptionType.SingleValue);
                command.OnExecute(() =>
                {
                    if (!connectionStringOption.HasValue())
                    {
                        Console.WriteLine("Source connection string is not provided.");
                        return 1;
                    }
                    ReproduceBug(connectionStringOption.Value(), connectionStringOption.Value());
                    return 0;
                });
            });

            app.OnExecute(() =>
            {
                app.ShowHelp();
                return 0;
            });
            return app.Execute(args);
        }
        #endregion

        #region Issue
        public static void ReproduceBug(
            string sourceDatabaseConnectionString,
            string destinationDatabaseConnectionString)
        {
            Console.WriteLine("Running test");
            using (var sourceConnection = new SqlConnection(sourceDatabaseConnectionString))
            using (var destinationConnection = new SqlConnection(destinationDatabaseConnectionString))
            {
                Console.WriteLine("Connecting to SQL Server");
                sourceConnection.Open();
                destinationConnection.Open();

                Console.WriteLine("Initialize source");
                RunCommands(sourceConnection,
                    new[]
                    {
                        "drop table if exists dbo.__SqlBulkCopyBug_Source",
                        "create table dbo.__SqlBulkCopyBug_Source (val float null)",
                        "insert dbo.__SqlBulkCopyBug_Source(val) values(1),(2),(null),(0.00000000000001)"
                    });

                Console.WriteLine("Initialize destination");
                RunCommands(destinationConnection,
                    new[]
                {
                    "drop table if exists dbo.__SqlBulkCopyBug_Destination",
                    "create table dbo.__SqlBulkCopyBug_Destination (val decimal(18,10) null)"
                });

                Console.WriteLine("Run buggy SqlBulkCopy");
                Exception error = null;
                try
                {
                    var bulkCopy = new SqlBulkCopy(destinationConnection, SqlBulkCopyOptions.Default, null);
                    bulkCopy.DestinationTableName = "dbo.__SqlBulkCopyBug_Destination";
                    using (var sourceCommand = new SqlCommand("select * from dbo.__SqlBulkCopyBug_Source", sourceConnection, null))
                    using (var sourceReader = sourceCommand.ExecuteReader())
                    {
                        bulkCopy.WriteToServer(sourceReader);
                    }
                }
                catch (Exception ex)
                {
                    error = ex;
                }

                Console.WriteLine("Assert exception");
                if (error == null)
                {
                    throw new Exception("No error occured???");
                }
                else if (error is System.Data.SqlTypes.SqlNullValueException typedError)
                {
                    Console.WriteLine($"Issue is reproduced: {typedError}");
                }
                else
                {
                    throw new Exception("Some other error???", error);
                }
            }
        }
        #endregion

        #region Helper Methods
        public static void RunCommands(SqlConnection connection, IEnumerable<string> commands)
        {
            foreach (var command in commands)
            {
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = command;
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
        #endregion

    }
}
