using System.Collections.Generic;
using Amazon.CDK;
using Amazon.CDK.AWS.DynamoDB;

namespace CdkDdbPopulate
{
    public class CdkDdbPopulateStack : Stack
    {
        // .NET implementation of the Java sample adding a record to a DynamoDB table:
        // https://stackoverflow.com/questions/62724486/aws-cdk-dynamodb-initial-data
        internal CdkDdbPopulateStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {
            const string ddbTableName = "CdkTestTable";

            var  table = new Table(this, $"{ddbTableName}_ddb_table", new TableProps {
                TableName = ddbTableName,
                RemovalPolicy = RemovalPolicy.DESTROY,
                PartitionKey = new Attribute { Name = "id", Type = AttributeType.STRING },
                SortKey = new Attribute { Name = "sort", Type = AttributeType.STRING }
            });

            this.GeneratePutData(table, ddbTableName, //2,
                new Dictionary<string, object> {
                    { "id", "0" },
                    { "sort", "a"},
                    { "field1", "Hello!"},
                    { "field2", "Good bye!"},
                    { "isActive", true },
                    { "age", 28 }
                },
                new Dictionary<string, object> {
                    { "id", "1" },
                    { "sort", "a"},
                    { "field1", "What's up!"},
                    { "field2", "See ya!"},
                    { "isActive", false },
                    { "age", 3.1415 }
                },
                new Dictionary<string, object> {
                    { "id", "2" },
                    { "sort", "a"},
                    { "field1", "Greetings!"},
                    { "field2", "Farewell!"},
                    { "isActive", true },
                    { "age", 33.3333 }
                }
            );
        }
    }
}
