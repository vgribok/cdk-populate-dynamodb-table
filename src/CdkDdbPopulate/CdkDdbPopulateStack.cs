using System.Collections.Generic;
using Amazon.CDK;
using Amazon.CDK.AWS.DynamoDB;
using Amazon.CDK.CustomResources;
using Amazon.CDK.AWS.IAM;
using System.Linq;

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

            var data = new Dictionary<string, object>[] {
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
                },
            };

            //var putItemCall = GenSinglePutItem(ddbTableName, data[1]);


            const int maxBatchSize = 2;
            for(int i = 0, batchNum = 1 ; i < data.Length ; i += maxBatchSize, batchNum++)
            {
                //var batch = data.Skip(i).Take(maxBatchSize).ToArray();
                int upperBound = i+maxBatchSize;
                if(upperBound > data.Length)
                    upperBound = data.Length;

                var batch = data[i..upperBound];

                var putItemCall = GenBatchPut(ddbTableName, batchNum, data);

                var ddbTableInitializer = new AwsCustomResource(this, $"DdbTableInitializer_{batchNum}", new AwsCustomResourceProps {
                    Policy = AwsCustomResourcePolicy.FromStatements(new PolicyStatement[] {
                        new PolicyStatement(new PolicyStatementProps{
                            Effect = Effect.ALLOW,
                            Actions = new string[] { "dynamodb:PutItem", "dynamodb:BatchWriteItem" },
                            Resources = new string[] { table.TableArn }
                        })
                    }),
                    OnCreate = putItemCall,
                    OnUpdate = putItemCall
                });

                ddbTableInitializer.Node.AddDependency(table);
            }
        }

        private static AwsSdkCall GenBatchPut(string tableName, int batchNum, Dictionary<string,object>[] items) =>
            items.Length > 25 ? throw new System.ArgumentException($"DynamoDB batchWriteItem batch can't take {items.Length} items, which exceeds the maximum of 25", nameof(items)) :
            new AwsSdkCall 
            {
                Service = "DynamoDB",
                Action = "batchWriteItem",
                PhysicalResourceId = PhysicalResourceId.Of($"{tableName}_initialization_{batchNum}"),
                Parameters = new Dictionary<string, object> { 
                    {"RequestItems", new Dictionary<string, object>{{tableName, PrepItems(items)}}},
                }
            };

        private static AwsSdkCall GenSinglePutItem(string tableName, IEnumerable<KeyValuePair<string,object>> item) =>
            new AwsSdkCall 
            {
                Service = "DynamoDB",
                Action = "putItem",
                PhysicalResourceId = PhysicalResourceId.Of($"{tableName}_initialization"),
                Parameters = new Dictionary<string, object> {
                    {"TableName", tableName},
                    { "Item", PrepItem(item) },
                    {"ConditionExpression", "attribute_not_exists(id)"}
                }
            };

        private static string GetDdbType(object val)
        {
            if(val == null || val is string)
                return "S";
            
            if(val.GetType().IsPrimitive)            
            {
                if(val is bool || val is int || val is long || val is decimal || val is double || val is byte || val is float || val is short || val is uint || val is ulong)
                    return "N";
            }
            return "S";
        }

        private static object GetDdbValue(object val)
        {
            if(val == null)
                return val;

            if(val is bool)
                return (bool)val ? 1 : 0;
            
            var ddbType = GetDdbType(val);

            return ddbType == "S" ? val.ToString() : val;
        }

        /*
        Example:
        {
            id: { S: "1" };
            firstName: { S: "John" }
            lastName: { S: "Doe" }
            shoeSize: { N: 10 }
            favoriteColor: { S: "blue" }
            isActive: {B: true }
        }
        */
        private static Dictionary<string,Dictionary<string,object>> PrepItem(IEnumerable<KeyValuePair<string,object>> item) => 
            item.ToDictionary(
                prop => prop.Key, 
                prop => new Dictionary<string,object>{{GetDdbType(prop.Value), GetDdbValue(prop.Value)}}
            );



        private static Dictionary<string, object>[] PrepItems(params Dictionary<string,object>[] items) =>
            items.Select(item => 
                new Dictionary<string, object>{
                    {"PutRequest", new Dictionary<string, object>{{"Item", PrepItem(item)}}}
                }
            ).ToArray();
        
    }
}
