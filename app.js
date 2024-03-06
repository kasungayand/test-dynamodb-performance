import dotenv from 'dotenv';
dotenv.config();
import { DynamoDBClient, GetItemCommand, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import {marshall} from "@aws-sdk/util-dynamodb" 
import {writeFileSync, createReadStream, access, writeFile, constants} from "fs"
import {randomUUID} from "crypto"
import {NodeHttpHandler} from "@aws-sdk/node-http-handler"

// import {
//     ExecuteStatementCommand,
//     DynamoDBDocumentClient,
//     GetCommand
//   } from "@aws-sdk/lib-dynamodb";
import {
    S3Client,
    PutObjectCommand,
    GetObjectCommand
  } from "@aws-sdk/client-s3";

let awsConfigParams = {
    region: 'us-east-1',  
    requestHandler: new NodeHttpHandler({
        socketAcquisitionWarningTimeout: 1000, // Set to a higher value in milliseconds
    }),
}

if(process.env.local && parseInt(process.env.local) == 1)
    awsConfigParams = {
        ...awsConfigParams,
        credentials:{
            accessKeyId: process.env.aws_access_key, // Replace with your AWS access key
            secretAccessKey: process.env.aws_secret_access_key // Replace with your AWS secret key
        }
    }

const client = new DynamoDBClient(awsConfigParams);
const s3 = new S3Client(awsConfigParams)

const generateSampleData = (fileIndex, itemIndex) => {
  const index = `${fileIndex}#${itemIndex}`
  return {
    "Item": marshall({
        "settings_pk": index.toString(),
        "timestamp": Date.now().toString(),
        "collection_id": fileIndex,
        "Title": "Book 101 Title",
        "ISBN": "111-1111111111",
        "Authors": [{"surname": "Joe","lastname":"Biden"},{"surname": "Joe","lastname":"Biden"}],
        "Price": "200",
        "Dimensions": "8.5 x 11.0 x 0.5",
        "PageCount": "500",
        "InPublication": true,
        "ProductCategory": "Book",
        "ProductDescrription": "Amazon DynamoDB is a serverless, NoSQL database service that enables you to develop modern applications at any scale. As a serverless database, you only pay for what you use and DynamoDB scales to zero, has no cold starts, no version upgrades, no maintenance windows, no patching, and no downtime maintenance. DynamoDB offers a broad set of security controls and compliance standards. For globally distributed applications, DynamoDB global tables is a multi-Region, multi-active database with a 99.999% availability SLA and increased resilience. DynamoDB reliability is supported with managed backups, point-in-time recovery, and more. With DynamoDB streams, you can build serverless event-driven applications."
    })
    };
};

const getS3Items = async () => {
    const command = new GetObjectCommand({
        Bucket: "dynamodb-load-test-data",
        Key: "output-909.json",
      });
      try {
        const response = await s3.send(command);
        // The Body object also has 'transformToByteArray' and 'transformToWebStream' methods.
        const str = await response.Body.transformToString();
        console.log(str);
      } catch (err) {
        console.error(err);
      }
}

const insertItemsInBatches = async () => {
    for(let j=1;j<=1000;j++){
        const jsonFilePath = `output-${randomUUID()}.json`;
        access(`outputs/${jsonFilePath}`, constants.F_OK, async (err) => {
            if (err) {
                writeFile(`outputs/${jsonFilePath}`, '', (err) => {
                });
            }
            const records = Array.from({ length: 1000 }, (_, index) => generateSampleData(j, index));
            let jsonString = JSON.stringify(records).replace(/^[\[\]]|[\[\]]$/g, '').replace(/(?<=})\s*,\s*(?={"Item")/g, '');
            writeFileSync(`outputs/${jsonFilePath}`, jsonString);
            const data = await s3.send(new PutObjectCommand({
                Bucket: 'dynamodb-load-test-data',
                Key: `files/${jsonFilePath}`,
                Body: createReadStream(`outputs/${jsonFilePath}`)
            }));   
        });
    }
}

const generateQueryExecution = async () => {
    const command = new ExecuteStatementCommand({
        Statement: `SELECT settings_pk,collection_id FROM "performance-testing-table"."collection_id-index"`,
      });
    const res  = await client.send(command);
}

const queryDynamoDB = async () => {
    const promises = Array.from({ length: 1000 }, (_, index) => generateQueryExecution());
    const responses = await Promise.all(promises)
    console.log(responses.length)
}

// getS3Items()
// .then(() => console.log('Data fetched successfully.'))
// .catch((err) => console.error('Error fetching data:', err));

// insertItemsInBatches()
//   .then(() => console.log('Data inserted successfully.'))
//   .catch((err) => console.error('Error inserting data:', err));

queryDynamoDB()
    .then(() => console.log('Data fetched successfully.'))
    .catch((err) => console.error('Error fetching data:', err));
