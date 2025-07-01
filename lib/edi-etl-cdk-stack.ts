import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as path from 'path';

export class EdiEtlCdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // 🔹 1. Input Bucket (Raw .dat)
    const rawBucket = new s3.Bucket(this, 'RawEdiBucket', {
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // 🔹 2. Output Bucket (Parsed JSON)
    const parsedBucket = new s3.Bucket(this, 'ParsedEdiJson', {
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // 🔹 3. Lambda Function to Parse Files
    const ediParserLambda = new lambda.Function(this, 'EdiParserLambda', {
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: 'parse_835.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda')),
      environment: {
        OUTPUT_BUCKET: parsedBucket.bucketName,
      },
    });

    // 🔹 4. Permissions
    rawBucket.grantRead(ediParserLambda);
    parsedBucket.grantWrite(ediParserLambda);

    // 🔹 5. S3 Event Trigger
    rawBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(ediParserLambda),
    { prefix: '835-' }  // ✅ This avoids overlap
    );
    
const edi837Lambda = new lambda.Function(this, 'Edi837ParserLambda', {
  runtime: lambda.Runtime.PYTHON_3_10,
  handler: 'parse_837.handler',
  code: lambda.Code.fromAsset(path.join(__dirname, '../lambda')),
  environment: {
    OUTPUT_BUCKET: parsedBucket.bucketName,
  },
});

rawBucket.grantRead(edi837Lambda);
parsedBucket.grantWrite(edi837Lambda);

rawBucket.addEventNotification(
  s3.EventType.OBJECT_CREATED,
  new s3n.LambdaDestination(edi837Lambda),
  { prefix: '837-' } // 👈 Filter: only triggers on filenames like 837-something.dat
);

  }
}
