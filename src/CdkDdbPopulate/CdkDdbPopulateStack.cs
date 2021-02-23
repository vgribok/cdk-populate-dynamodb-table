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
                    { "field2", "Good bye!"}
                },
                new Dictionary<string, object> {
                    { "id", "1" },
                    { "sort", "a"},
                    { "field1", "What's up!"},
                    { "field2", "See ya!"}
                },
            };

            var putItemCall = new AwsSdkCall 
            {
                Service = "DynamoDB",
                Action = "putItem",
                PhysicalResourceId = PhysicalResourceId.Of($"{ddbTableName}_initialization"),
                Parameters = new Dictionary<string, object> {
                    {"TableName", ddbTableName},
                    { "Item", PrepItem(data[1]) },
                    {"ConditionExpression", "attribute_not_exists(id)"}
                }
            };

            var ddbTableInitializer = new AwsCustomResource(this, "DdbTableInitializer", new AwsCustomResourceProps {
                Policy = AwsCustomResourcePolicy.FromStatements(new PolicyStatement[] {
                    new PolicyStatement(new PolicyStatementProps{
                        Effect = Effect.ALLOW,
                        Actions = new string[] { "dynamodb:PutItem" },
                        Resources = new string[] { table.TableArn }
                    })
                }),
                OnCreate = putItemCall,
                OnUpdate = putItemCall
            });

            ddbTableInitializer.Node.AddDependency(table);
        }

        private static string GetDdbType(object val)
        {
            if(val == null || val is string)
                return "S";

            if(val.GetType().IsPrimitive)            
            {
                if(val is bool)
                    return "B";
                if(val is int || val is long || val is decimal || val is double || val is byte || val is float || val is short || val is uint || val is ulong || val is ushort)
                    return "N";
            }
            return "S";
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
        private static Dictionary<string,Dictionary<string,object>> PrepItem(Dictionary<string,object> item) => 
            item.ToDictionary(
                prop => prop.Key, 
                prop => new Dictionary<string,object>{{GetDdbType(prop.Value), prop.Value}}
            );
    }
}
