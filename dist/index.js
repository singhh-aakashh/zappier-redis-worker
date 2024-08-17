"use strict";
// import { PrismaClient } from "@prisma/client"
// import { parser } from "./parser";
// import { JsonObject } from "@prisma/client/runtime/library";
// import { sendGmail } from "./gmail";
// import Redis from "ioredis";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
//   const prismaClient = new PrismaClient();
//   type RedisMessage = [string, string[]]; // A message ID and an array of fields
// type RedisStream = [string, RedisMessage[]]; // Stream key and messages
// // Initialize Redis
// const redis = new Redis({
//   host: 'redis-18656.c321.us-east-1-2.ec2.redns.redis-cloud.com',  // Replace with your Redis host
//   port: 18656,         // Default Redis port
//   password: 'LEKR3q6SCkNxgjPa6adjScmBb44qrjeu',       // Add Redis password if necessary
// });
// // Subscribe to the "zap" channel
// const streamKey = "zap-stream" ;
// const groupName = "zap-consumers"
// const consumerName = 'consumer-1';
//   async function main() {
//     try {
//       await redis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM');
//     } catch (e) {
//       console.log('Consumer group already exists');
//     }
//     const messages = await redis.xreadgroup(
//       'GROUP',
//       groupName,
//       consumerName,     // Block indefinitely until a message is available
//       'COUNT',
//       10,     
//       'BLOCK',
//       0,    // Read one message at a time
//       'STREAMS',
//       streamKey,
//       '>'
//     ) as RedisStream[]; // 
//     if (messages) {
//       for(const [stream, messageList] of messages){
//           for(const [id,field] of messageList){
//             if(field[1]){
//             try {
//               const zapRunDetails = await prismaClient.zapRun.findFirst({
//                 where:{
//                   id:field[1]
//                 },
//                 include:{
//                   zap:{
//                     include:{
//                       actions:{
//                         include:{
//                           AvailableAction:true
//                         }
//                       }
//                     }
//                   }
//                 }
//               })
//               const currentAction = zapRunDetails?.zap.actions.find( x => x.sortingOrder === Number(field[3]))
//               const zapRunMetadata= zapRunDetails?.metadata;
//               if(currentAction?.AvailableAction.id === "gmail"){
//                 try {
//                   const from = parser((currentAction.metadata as JsonObject)?.from as string,zapRunMetadata)
//                   const to = parser((currentAction.metadata as JsonObject)?.to as string,zapRunMetadata)
//                   const subject = parser((currentAction.metadata as JsonObject)?.subject as string,zapRunMetadata)
//                   const body = parser((currentAction.metadata as JsonObject)?.body as string,zapRunMetadata)
//                   const res = await sendGmail(from,to,subject,body)
//                   if(res){
//                     console.log(`Gmail send successfully`)
//                   }else{
//                     console.log('failed to send gmail')
//                   }
//                 } catch (error) {
//                   console.log(error)
//                 }
//               }
//               const lastStage = (zapRunDetails?.zap.actions.length || 1)-1;
//                 if(lastStage !== Number(field[3])){
//                const res= await redis.xadd(streamKey,"*", 'zapRunId',(field[1]).toString(),'stage',(Number(field[3])+1).toString());
//                   console.log(`stage ${Number(field[3]+1)} is added.`,res)
//                 }
//                 else{
//                   console.log(`stage ${field[3]} is the last action`)
//                 }
//             } catch (error) {
//               console.log(error)
//             }
//           }
//           await redis.xack(streamKey, groupName,id);
//           }
//         }
//       }
//   }
//   main();
const client_1 = require("@prisma/client");
const ioredis_1 = __importDefault(require("ioredis"));
const parser_1 = require("./parser");
const gmail_1 = require("./gmail"); // Assume this is your Gmail sending function
const prismaClient = new client_1.PrismaClient();
// Initialize Redis
const redis = new ioredis_1.default({
    host: 'redis-18656.c321.us-east-1-2.ec2.redns.redis-cloud.com', // Replace with your Redis host
    port: 18656, // Default Redis port
    password: 'LEKR3q6SCkNxgjPa6adjScmBb44qrjeu', // Add Redis password if necessary
});
const streamKey = "zap-stream"; // Redis stream key
const groupName = "zap-consumers"; // Consumer group name
const consumerName = 'consumer-1'; // Consumer name
// Create or ensure consumer group exists
async function ensureConsumerGroup() {
    try {
        await redis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM');
    }
    catch (e) {
        console.log('Consumer group already exists');
    }
}
// Process the current stage of the task
async function processStage(zapRunId, currentStage) {
    try {
        // Fetch zapRun and related details from the database
        const zapRunDetails = await prismaClient.zapRun.findFirst({
            where: { id: zapRunId },
            include: {
                zap: {
                    include: {
                        actions: {
                            include: {
                                AvailableAction: true,
                            },
                        },
                    },
                },
            },
        });
        if (!zapRunDetails) {
            console.log(`No zapRun found with id ${zapRunId}`);
            return;
        }
        const currentAction = zapRunDetails.zap.actions.find((x) => x.sortingOrder === currentStage);
        const zapRunMetadata = zapRunDetails.metadata;
        if (currentAction?.AvailableAction.id === "gmail") {
            // Process Gmail action
            const from = (0, parser_1.parser)(currentAction.metadata?.from, zapRunMetadata);
            const to = (0, parser_1.parser)(currentAction.metadata?.to, zapRunMetadata);
            const subject = (0, parser_1.parser)(currentAction.metadata?.subject, zapRunMetadata);
            const body = (0, parser_1.parser)(currentAction.metadata?.body, zapRunMetadata);
            const result = await (0, gmail_1.sendGmail)(from, to, subject, body);
            if (result) {
                console.log("Gmail sent successfully");
            }
            else {
                console.log("Failed to send Gmail");
            }
        }
        // Check if this is the last stage
        const lastStage = zapRunDetails.zap.actions.length - 1;
        if (currentStage < lastStage) {
            // Push the next stage to the stream
            await redis.xadd(streamKey, "*", "zapRunId", zapRunId, "stage", (currentStage + 1).toString());
            console.log(`Stage ${currentStage + 1} added to stream for zapRunId ${zapRunId}`);
        }
        else {
            console.log(`Stage ${currentStage} is the last stage for zapRunId ${zapRunId}`);
        }
    }
    catch (error) {
        console.log(`Error processing stage ${currentStage} for zapRunId ${zapRunId}:`, error);
    }
}
// Main worker function to process tasks from the stream
async function main() {
    await ensureConsumerGroup();
    while (true) {
        const messages = await redis.xreadgroup('GROUP', groupName, consumerName, 'COUNT', 1, // Process one message at a time
        'BLOCK', 0, // Block indefinitely until a message is available
        'STREAMS', streamKey, '>'); // [stream, [id, fields][]] format
        if (messages) {
            for (const [stream, messageList] of messages) {
                for (const [id, fields] of messageList) {
                    const zapRunId = fields[1];
                    const currentStage = parseInt(fields[3], 10);
                    console.log(`Processing zapRunId: ${zapRunId}, stage: ${currentStage}`);
                    // Process the current stage
                    await processStage(zapRunId, currentStage);
                    // Acknowledge the message after processing
                    await redis.xack(streamKey, groupName, id);
                }
            }
        }
    }
}
main();
