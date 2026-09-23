import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';

/**
 * A training-sized copy of datalake-service's infrastructure: raw files land in an S3 bucket,
 * an event-driven Lambda (apps/fileevent_handler) organizes/preprocesses/enqueues them, an SQS-
 * triggered Lambda (apps/ingest_worker) applies a schema and writes the result into a second,
 * partitioned "lake" bucket. A DynamoDB table tracks each batch's status end-to-end.
 *
 * Two infra decisions worth calling out:
 *   1. The fileevent Lambda is invoked directly by S3 event notifications (not via SQS/EventBridge)
 *      because every phase of its handler relies on ITS OWN writes (a copy, a metadata update)
 *      firing the next event - see apps/fileevent_handler/handler.py's module docstring. Only the
 *      hand-off to the ingest worker goes through a queue, because that's a genuinely different
 *      Lambda doing genuinely different (slower, retryable) work.
 *   2. The DynamoDB table's key schema (`source_system` partition key + `batch_id` sort key)
 *      mirrors StatusLedger's `_key()` helper exactly - the infra and the application code that
 *      reads/writes it have to agree on this, so it's worth keeping them side by side mentally.
 */
export interface DatalakeStackProps extends cdk.StackProps {
  maxReceiveCount?: number;
}

export class DatalakeStack extends cdk.Stack {
  public readonly rawBucket: s3.Bucket;
  public readonly lakeBucket: s3.Bucket;
  public readonly statusTable: dynamodb.Table;
  public readonly ingestQueue: sqs.Queue;
  public readonly ingestDeadLetterQueue: sqs.Queue;
  public readonly fileEventFunction: lambda.Function;
  public readonly ingestWorkerFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: DatalakeStackProps = {}) {
    super(scope, id, props);

    const maxReceiveCount = props.maxReceiveCount ?? 3;

    this.rawBucket = new s3.Bucket(this, 'RawBucket', {
      bucketName: undefined,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    this.lakeBucket = new s3.Bucket(this, 'LakeBucket', {
      bucketName: undefined,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    this.statusTable = new dynamodb.Table(this, 'StatusTable', {
      tableName: 'datalake-ingest-status',
      partitionKey: { name: 'source_system', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'batch_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.ingestDeadLetterQueue = new sqs.Queue(this, 'IngestDlq', {
      queueName: 'datalake-ingest-dlq',
      encryption: sqs.QueueEncryption.SQS_MANAGED,
    });

    this.ingestQueue = new sqs.Queue(this, 'IngestQueue', {
      queueName: 'datalake-ingest-queue',
      visibilityTimeout: cdk.Duration.minutes(10),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: {
        queue: this.ingestDeadLetterQueue,
        maxReceiveCount,
      },
    });

    this.fileEventFunction = new lambda.Function(this, 'FileEventFunction', {
      functionName: 'datalake-fileevent-handler',
      description: 'Organizes/preprocesses raw files and enqueues them for ingest once ready.',
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(2),
      memorySize: 256,
      environment: {
        STATUS_TABLE_NAME: this.statusTable.tableName,
        INGEST_QUEUE_URL: this.ingestQueue.queueUrl,
      },
      // See apps/fileevent_handler/handler.py for the real handler code - a training-sized copy
      // of this repo synths cleanly with an inline stub, no build step required.
      code: lambda.Code.fromInline(
        'def handler(event, context):\n    raise NotImplementedError("placeholder - see apps/fileevent_handler/handler.py")\n',
      ),
      handler: 'index.handler',
      logGroup: new logs.LogGroup(this, 'FileEventLogGroup', {
        logGroupName: '/aws/lambda/datalake-fileevent-handler',
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    this.rawBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(this.fileEventFunction),
    );

    this.statusTable.grantReadWriteData(this.fileEventFunction);
    this.rawBucket.grantReadWrite(this.fileEventFunction);
    this.ingestQueue.grantSendMessages(this.fileEventFunction);

    this.ingestWorkerFunction = new lambda.Function(this, 'IngestWorkerFunction', {
      functionName: 'datalake-ingest-worker',
      description: 'Applies a schema to a raw batch and writes it into the partitioned lake bucket.',
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(10),
      memorySize: 1024,
      environment: {
        STATUS_TABLE_NAME: this.statusTable.tableName,
        LAKE_BUCKET_NAME: this.lakeBucket.bucketName,
      },
      // See apps/ingest_worker/handler.py for the real handler code.
      code: lambda.Code.fromInline(
        'def handler(event, context):\n    raise NotImplementedError("placeholder - see apps/ingest_worker/handler.py")\n',
      ),
      handler: 'index.handler',
      logGroup: new logs.LogGroup(this, 'IngestWorkerLogGroup', {
        logGroupName: '/aws/lambda/datalake-ingest-worker',
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    this.ingestWorkerFunction.addEventSource(
      new SqsEventSource(this.ingestQueue, {
        batchSize: 1,
      }),
    );

    this.statusTable.grantReadWriteData(this.ingestWorkerFunction);
    this.rawBucket.grantRead(this.ingestWorkerFunction);
    this.lakeBucket.grantReadWrite(this.ingestWorkerFunction);

    new cdk.CfnOutput(this, 'RawBucketName', { value: this.rawBucket.bucketName });
    new cdk.CfnOutput(this, 'LakeBucketName', { value: this.lakeBucket.bucketName });
    new cdk.CfnOutput(this, 'StatusTableName', { value: this.statusTable.tableName });
    new cdk.CfnOutput(this, 'IngestQueueUrl', { value: this.ingestQueue.queueUrl });
  }
}
