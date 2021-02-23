using Amazon.CDK;
using Amazon.CDK.AWS.SNS;
using Amazon.CDK.AWS.SNS.Subscriptions;
using Amazon.CDK.AWS.SQS;

namespace CdkDdbPopulate
{
    public class CdkDdbPopulateStack : Stack
    {
        internal CdkDdbPopulateStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {
             // The CDK includes built-in constructs for most resource types, such as Queues and Topics.
            var queue = new Queue(this, "CdkDdbPopulateQueue", new QueueProps
            {
                VisibilityTimeout = Duration.Seconds(300)
            });

            var topic = new Topic(this, "CdkDdbPopulateTopic");

            topic.AddSubscription(new SqsSubscription(queue));
        }
    }
}
