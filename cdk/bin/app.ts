import * as cdk from 'aws-cdk-lib';
import { DatalakeStack } from '../lib/datalake_stack';

const app = new cdk.App();

new DatalakeStack(app, 'DatalakeStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
});
