import dotenv from 'dotenv';
dotenv.config();
import { DynamoDBClient, GetItemCommand, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import {marshall} from "@aws-sdk/util-dynamodb" 
import {writeFileSync, createReadStream, access, writeFile, constants} from "fs"
import {randomUUID} from "crypto"
import {NodeHttpHandler} from "@aws-sdk/node-http-handler"
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

/**
 * 
 * @param {*} command 
 * @param {*} client . Only need for the testcase execution.
 * @returns 
 */
const retryableCommand = async (command, client) => {
    const maxRetryAttempts = 10;
    let retryCount = 0;
    const retryErrors = ["LimitExceededException","ProvisionedThroughputExceeded",
      "ProvisionedThroughputExceededException","RequestLimitExceeded","ThrottlingException","ExpiredTokenException"]
    while (true) {
          try {
              const response = client ? await client.send(command) : await dynamoDb.send(command);
              return response;
          } catch (error) {
              if (retryErrors.includes(error.name) && retryCount < maxRetryAttempts) {
                  const retryDelay = 200 + retryCount * 1000
                  await new Promise((resolve) => setTimeout(resolve, retryDelay));
                  retryCount++;
                  continue;
              }
              throw error;
          }
      }
  }

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
        Statement: `SELECT settings_pk,collection_id FROM "performance-testing-table"."collection_id-timestamp-index"`,
        Limit: 1
      });
    return retryableCommand(command, client)
}

const getDocumentCount = async (results) => {
    const documents = results.map(result => result ? result.Items.length : 0);
   return documents.reduce((acc, count) => acc + count, 0);
}

const queryDynamoDB = async () => {
    try{
        console.time("Total execution time")
        const promises = Array.from({ length: 5000 }, (_, index) => generateQueryExecution());
        const responses = await Promise.all(promises)
        console.log('Data fetched successfully.')
        console.log("Promise all response length",responses.length)
        console.time('Data loop time')
        getDocumentCount(responses).then(count => {
            console.timeEnd('Data loop time')
            console.log("Total Document Count:", count);
        })
    }catch(err){
        throw err
    }
}

// getS3Items()
// .then(() => console.log('Data fetched successfully.'))
// .catch((err) => console.error('Error fetching data:', err));

// insertItemsInBatches()
//   .then(() => console.log('Data inserted successfully.'))
//   .catch((err) => console.error('Error inserting data:', err));

queryDynamoDB()
    .then(() => {
        console.timeEnd("Total execution time")
    })
    .catch((err) => console.error('Error fetching data:', err));
