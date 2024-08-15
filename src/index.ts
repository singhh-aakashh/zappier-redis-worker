import { PrismaClient } from "@prisma/client"
import {Kafka} from "kafkajs"
import { parser } from "./parser";
import { JsonObject } from "@prisma/client/runtime/library";
import { sendGmail } from "./gmail";

const TOPIC = "zap-events"

const kafka = new Kafka({
    clientId: 'Outbox-processor',
    brokers: ['localhost:9092']
  })

  const prismaClient = new PrismaClient();

  async function main() {
    const consumer = kafka.consumer({ groupId: 'main-worker' })
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    const producer = kafka.producer()
    await producer.connect()

        await consumer.run({
            autoCommit:false,
            eachMessage: async ({ topic, partition, message }) => {
              if(message.value === null){
                console.log("message.value is null")
                return;
              }
              console.log({
                partition,
                offset: message.offset,
                value: message?.value.toString(),
              })
              console.log("here in worker")
              const parsedData = JSON.parse(message?.value.toString());
              const zapRunId = parsedData.zapRunId;
              const stage = parsedData.stage;

              const zapRunDetails = await prismaClient.zapRun.findFirst({
                where:{
                  id:zapRunId
                },
                include:{
                  zap:{
                    include:{
                      actions:{
                        include:{
                          AvailableAction:true
                        }
                      }
                    }
                  }
                }
              })
              // finding action to perform on corressponding stage
              const currentAction = zapRunDetails?.zap.actions.find(x=>x.sortingOrder === stage);

              const zapRunMetadata = zapRunDetails?.metadata
              
              if(currentAction?.AvailableAction.id === "gmail"){
                try {
                  const from = parser((currentAction.metadata as JsonObject)?.from as string,zapRunMetadata)
                  const to = parser((currentAction.metadata as JsonObject)?.to as string,zapRunMetadata)
                  const body = parser((currentAction.metadata as JsonObject)?.body as string,zapRunMetadata)
                  const subject = parser((currentAction.metadata as JsonObject)?.subject as string,zapRunDetails)
                  const res =  await sendGmail(from,to,body,subject)
                  if(res){
                  console.log(`Sending out email from ${from} to ${to} and body is ${body}`)
                  }
                  else{
                    console.log("failed to send gmail")
                  }

                } catch (error) {
                  console.log("error",error)
                }
             
              }

              //checking if this is last stage or not
              const lastStage = (zapRunDetails?.zap.actions.length || 1 ) -1 ;
              if(lastStage !== stage){
                await producer.send({
                  topic:TOPIC,
                  messages:[{value:JSON.stringify({zapRunId:zapRunId,stage:stage+1})}]
              })
              }

              await new Promise(r => setTimeout(r,5000))
              console.log("processing done")
                await consumer.commitOffsets([{
                    topic:TOPIC,
                    partition,
                    offset:(parseInt(message.offset)+1).toString()
                }])
            },
          })


        }
    

  
  main();