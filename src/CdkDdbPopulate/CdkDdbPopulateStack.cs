using System.Collections.Generic;
using Amazon.CDK;
using Amazon.CDK.AWS.DynamoDB;
using Amazon.CDK.CustomResources;
using Amazon.CDK.AWS.IAM;

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

            var putItemCall = new AwsSdkCall 
            {
                Service = "DynamoDB",
                Action = "putItem",
                PhysicalResourceId = PhysicalResourceId.Of($"{ddbTableName}_initialization"),
                Parameters = new Dictionary<string, object> {
                    {"TableName", ddbTableName},
                    {"Item", new Dictionary<string,object>{
                        { "id", new Dictionary<string, object> {{"S", "0"}} },
                        { "sort",  new Dictionary<string, object> {{"S", "a"}} },
                        { "field1", new Dictionary<string, object> {{"S", "Hello!"}} },
                        { "field2", new Dictionary<string, object> {{"S", "Good bye!"}} },
                    }},
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
    }
}
