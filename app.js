require('dotenv').config()
const AWS = require('aws-sdk');
const { marshall } = require('@aws-sdk/util-dynamodb');
const fs = require('fs');

let awsConfigParams = {
    region: 'us-east-1'
}

if(process.env.local && parseInt(process.env.local) == 1)
    awsConfigParams = {
        ...awsConfigParams,
        accessKeyId: process.env.aws_access_key, // Replace with your AWS access key
        secretAccessKey: process.env.aws_secret_access_key // Replace with your AWS secret key
    }

AWS.config.update(awsConfigParams);

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();

const tableName = 'test-dynamodb'; // Replace with your DynamoDB table name
const numberOfItems = 1000000;

// Function to generate sample data
const generateSampleData = (index) => {
  return {
    "Item": marshall({
        "settings_pk": index.toString(),
        "timestamp": Date.now().toString(),
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

// Function to insert items into DynamoDB in batches
// const insertItemsInBatches = async () => {
//   const batchWriteParams = { RequestItems: {} };

//   for (let i = 0; i < numberOfItems; i++) {
//     const item = generateSampleData(i);
//     batchWriteParams.RequestItems[tableName] = batchWriteParams.RequestItems[tableName] || [];
//     batchWriteParams.RequestItems[tableName].push({
//       PutRequest: {
//         Item: item
//       }
//     });

//     if (batchWriteParams.RequestItems[tableName].length === 25) {
//         console.log(batchWriteParams)
//       await dynamoDB.batchWrite(batchWriteParams).promise();
//       batchWriteParams.RequestItems[tableName] = [];
//     }
//   }

//   // Write the remaining items
//   if (batchWriteParams.RequestItems[tableName].length > 0) {
//     await dynamoDB.batchWrite(batchWriteParams).promise();
//   }
// };

const insertItemsInBatches = async () => {
    for(let j=1;j<=1000;j++){
        const jsonFilePath = `output-${j}.json`;
        fs.access(`outputs/${jsonFilePath}`, fs.constants.F_OK, async (err) => {
            if (err) {
                fs.writeFile(`outputs/${jsonFilePath}`, '', (err) => {
                });
            }
            const params = {
                Bucket: 'dynamodb-load-test-data',
                Key: jsonFilePath,
                Body: fs.createReadStream(`outputs/${jsonFilePath}`)
            };
            const records = Array.from({ length: 1001 }, (_, index) => generateSampleData(`${j}#${index}`));
            let jsonString = JSON.stringify(records).replace(/^[\[\]]|[\[\]]$/g, '').replace(/(?<=})\s*,\s*(?={"Item")/g, '');
            fs.writeFileSync(`outputs/${jsonFilePath}`, jsonString);
            const data = await s3.upload(params).promise();   
        });
    }
}

// Insert items into DynamoDB
insertItemsInBatches()
  .then(() => console.log('Data inserted successfully.'))
  .catch((err) => console.error('Error inserting data:', err));
