using System.Collections.Generic;
using Amazon.CDK;
using Amazon.CDK.AWS.DynamoDB;
using Amazon.CDK.CustomResources;
using Amazon.CDK.AWS.IAM;
using System.Linq;

public static class DdbDataInitializer
{
    public static List<AwsCustomResource> GeneratePutData(this Construct scope, Table table, string tableName, params Dictionary<string, object>[] data) =>
        scope.GeneratePutData(table, tableName, 25, data);

    public static List<AwsCustomResource> GeneratePutData(this Construct scope, Table table, string tableName, int batchSize, params Dictionary<string, object>[] data)
    {
        if(batchSize < 1 || batchSize > 25)
            throw new System.ArgumentException($"DynamoDB batchWriteItem batch can't take {batchSize} items, which isn't between 1 and 25", nameof(data));

        var customResources = new List<AwsCustomResource>();

        for(int i = 0, batchNum = 1 ; i < data.Length ; i += batchSize, batchNum++)
        {
            int upperBound = i+batchSize;
            if(upperBound > data.Length)
                upperBound = data.Length;

            var batch = data[i..upperBound];

            var putItemCall = GenBatchPut(tableName, batchNum, data);

            var ddbTableInitializer = new AwsCustomResource(scope, $"DdbTableInitializer_{batchNum}", new AwsCustomResourceProps {
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

            customResources.Add(ddbTableInitializer);
        }

        return customResources;
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
        isActive: {N: 0 }
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