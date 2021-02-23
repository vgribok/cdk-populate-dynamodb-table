using Amazon.CDK;

namespace CdkDdbPopulate
{
    sealed class Program
    {
        public static void Main(string[] args)
        {
            var app = new App();
            new CdkDdbPopulateStack(app, "CdkDdbPopulateStack");

            app.Synth();
        }
    }
}
