// Creates the two tables against DynamoDB Local (or a real account, if you
// point DYNAMO_ENDPOINT away). Safe to re-run.
import { CreateTableCommand, DynamoDBClient, ResourceInUseException } from '@aws-sdk/client-dynamodb';
import { loadConfig } from '../src/config.js';

const config = loadConfig({ JWT_SECRET: 'unused-for-table-setup', ...process.env });
const client = new DynamoDBClient({
  region: config.AWS_REGION,
  endpoint: config.DYNAMO_ENDPOINT ?? 'http://localhost:8000',
  credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
});

const tables = [
  new CreateTableCommand({
    TableName: config.CONVERSATIONS_TABLE,
    BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [
      { AttributeName: 'user_id', AttributeType: 'S' },
      { AttributeName: 'conversation_id', AttributeType: 'S' },
      { AttributeName: 'last_message_at', AttributeType: 'S' },
    ],
    KeySchema: [
      { AttributeName: 'user_id', KeyType: 'HASH' },
      { AttributeName: 'conversation_id', KeyType: 'RANGE' },
    ],
    GlobalSecondaryIndexes: [
      {
        // Sparse: a conversation shows up in the inbox once it has a message.
        IndexName: 'by_last_message',
        KeySchema: [
          { AttributeName: 'user_id', KeyType: 'HASH' },
          { AttributeName: 'last_message_at', KeyType: 'RANGE' },
        ],
        Projection: { ProjectionType: 'ALL' },
      },
    ],
  }),
  new CreateTableCommand({
    TableName: config.MESSAGES_TABLE,
    BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [
      { AttributeName: 'conversation_id', AttributeType: 'S' },
      { AttributeName: 'message_id', AttributeType: 'S' },
    ],
    KeySchema: [
      { AttributeName: 'conversation_id', KeyType: 'HASH' },
      { AttributeName: 'message_id', KeyType: 'RANGE' },
    ],
  }),
];

for (const cmd of tables) {
  try {
    await client.send(cmd);
    console.log(`created ${cmd.input.TableName}`);
  } catch (err) {
    if (err instanceof ResourceInUseException) console.log(`${cmd.input.TableName} exists`);
    else throw err;
  }
}
