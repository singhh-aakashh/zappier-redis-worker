"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = require("@prisma/client");
const kafkajs_1 = require("kafkajs");
const parser_1 = require("./parser");
const gmail_1 = require("./gmail");
const TOPIC = "zap-events";
const kafka = new kafkajs_1.Kafka({
    clientId: 'Outbox-processor',
    brokers: ['localhost:9092']
});
const prismaClient = new client_1.PrismaClient();
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const consumer = kafka.consumer({ groupId: 'main-worker' });
        yield consumer.connect();
        yield consumer.subscribe({ topic: TOPIC, fromBeginning: true });
        const producer = kafka.producer();
        yield producer.connect();
        yield consumer.run({
            autoCommit: false,
            eachMessage: (_a) => __awaiter(this, [_a], void 0, function* ({ topic, partition, message }) {
                var _b, _c, _d, _e;
                if (message.value === null) {
                    console.log("message.value is null");
                    return;
                }
                console.log({
                    partition,
                    offset: message.offset,
                    value: message === null || message === void 0 ? void 0 : message.value.toString(),
                });
                console.log("here in worker");
                const parsedData = JSON.parse(message === null || message === void 0 ? void 0 : message.value.toString());
                const zapRunId = parsedData.zapRunId;
                const stage = parsedData.stage;
                const zapRunDetails = yield prismaClient.zapRun.findFirst({
                    where: {
                        id: zapRunId
                    },
                    include: {
                        zap: {
                            include: {
                                actions: {
                                    include: {
                                        AvailableAction: true
                                    }
                                }
                            }
                        }
                    }
                });
                // finding action to perform on corressponding stage
                const currentAction = zapRunDetails === null || zapRunDetails === void 0 ? void 0 : zapRunDetails.zap.actions.find(x => x.sortingOrder === stage);
                const zapRunMetadata = zapRunDetails === null || zapRunDetails === void 0 ? void 0 : zapRunDetails.metadata;
                if ((currentAction === null || currentAction === void 0 ? void 0 : currentAction.AvailableAction.id) === "gmail") {
                    try {
                        const from = (0, parser_1.parser)((_b = currentAction.metadata) === null || _b === void 0 ? void 0 : _b.from, zapRunMetadata);
                        const to = (0, parser_1.parser)((_c = currentAction.metadata) === null || _c === void 0 ? void 0 : _c.to, zapRunMetadata);
                        const body = (0, parser_1.parser)((_d = currentAction.metadata) === null || _d === void 0 ? void 0 : _d.body, zapRunMetadata);
                        const subject = (0, parser_1.parser)((_e = currentAction.metadata) === null || _e === void 0 ? void 0 : _e.subject, zapRunDetails);
                        const res = yield (0, gmail_1.sendGmail)(from, to, body, subject);
                        if (res) {
                            console.log(`Sending out email from ${from} to ${to} and body is ${body}`);
                        }
                        else {
                            console.log("failed to send gmail");
                        }
                    }
                    catch (error) {
                        console.log("error", error);
                    }
                }
                //checking if this is last stage or not
                const lastStage = ((zapRunDetails === null || zapRunDetails === void 0 ? void 0 : zapRunDetails.zap.actions.length) || 1) - 1;
                if (lastStage !== stage) {
                    yield producer.send({
                        topic: TOPIC,
                        messages: [{ value: JSON.stringify({ zapRunId: zapRunId, stage: stage + 1 }) }]
                    });
                }
                yield new Promise(r => setTimeout(r, 5000));
                console.log("processing done");
                yield consumer.commitOffsets([{
                        topic: TOPIC,
                        partition,
                        offset: (parseInt(message.offset) + 1).toString()
                    }]);
            }),
        });
    });
}
main();
