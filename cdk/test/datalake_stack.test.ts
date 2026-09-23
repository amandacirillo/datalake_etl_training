import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { DatalakeStack } from '../lib/datalake_stack';

describe('DatalakeStack', () => {
  function synth(props: ConstructorParameters<typeof DatalakeStack>[2] = {}) {
    const app = new cdk.App();
    const stack = new DatalakeStack(app, 'TestStack', {
      env: { account: '111111111111', region: 'us-east-1' },
      ...props,
    });
    return Template.fromStack(stack);
  }

  it('creates a raw bucket and a lake bucket, both private and encrypted', () => {
    const template = synth();
    template.resourceCountIs('AWS::S3::Bucket', 2);
    template.allResourcesProperties('AWS::S3::Bucket', {
      BucketEncryption: Match.anyValue(),
      PublicAccessBlockConfiguration: Match.objectLike({ BlockPublicAcls: true }),
    });
  });

  it('creates a status table keyed by source_system + batch_id', () => {
    const template = synth();
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      TableName: 'datalake-ingest-status',
      KeySchema: [
        { AttributeName: 'source_system', KeyType: 'HASH' },
        { AttributeName: 'batch_id', KeyType: 'RANGE' },
      ],
    });
  });

  it('wires the ingest queue to a dead-letter queue with the default maxReceiveCount', () => {
    const template = synth();
    template.hasResourceProperties('AWS::SQS::Queue', {
      QueueName: 'datalake-ingest-queue',
      RedrivePolicy: Match.objectLike({ maxReceiveCount: 3 }),
    });
    template.hasResourceProperties('AWS::SQS::Queue', { QueueName: 'datalake-ingest-dlq' });
  });

  it('honors a custom maxReceiveCount', () => {
    const template = synth({ maxReceiveCount: 5 });
    template.hasResourceProperties('AWS::SQS::Queue', {
      QueueName: 'datalake-ingest-queue',
      RedrivePolicy: Match.objectLike({ maxReceiveCount: 5 }),
    });
  });

  it('triggers the fileevent Lambda from S3 object-created notifications on the raw bucket', () => {
    const template = synth();
    template.hasResourceProperties('Custom::S3BucketNotifications', {
      NotificationConfiguration: Match.objectLike({
        LambdaFunctionConfigurations: Match.arrayWith([
          Match.objectLike({ Events: ['s3:ObjectCreated:*'] }),
        ]),
      }),
    });
  });

  it('triggers the ingest worker Lambda from the ingest queue with a batch size of 1', () => {
    const template = synth();
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 1,
    });
  });

  it('names both Lambda functions', () => {
    const template = synth();
    template.hasResourceProperties('AWS::Lambda::Function', { FunctionName: 'datalake-fileevent-handler' });
    template.hasResourceProperties('AWS::Lambda::Function', { FunctionName: 'datalake-ingest-worker' });
  });
});
