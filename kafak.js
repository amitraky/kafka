const { Kafka } = require("kafkajs");
const { v4: uuidv4 } = require("uuid");
const TOPIC = "lama-saas";
const TOPIC_REQ = `${TOPIC}-req-topic`;
const TOPIC_RES = `${TOPIC}-res-topic`;
const GROUPIDReq = `${TOPIC}-request-group`;
const GROUPIDRes = `${TOPIC}-response-group`;

const DeleteHostService =  require("../services/deleteHostService")

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const requestConsumer = kafka.consumer({ groupId: GROUPIDReq });
const responseConsumer = kafka.consumer({ groupId: GROUPIDRes });
const pendingRequests = new Map();

const initKafka = async () => {
  await producer.connect();
  await responseConsumer.connect();
  await requestConsumer.connect();

  await requestConsumer.subscribe({ topic: TOPIC_REQ, fromBeginning: false });
  await responseConsumer.subscribe({ topic: TOPIC_RES, fromBeginning: false });

  // Consumer for handling responses
  await responseConsumer.run({
    eachMessage: async ({ message }) => {
      const response = JSON.parse(message.value.toString());
      const { correlationId } = response;

      if (pendingRequests.has(correlationId)) {
        const { resolve } = pendingRequests.get(correlationId);
        resolve(response);
        pendingRequests.delete(correlationId);
      }
    },
  });

  // Consumer for handling incoming requests from other microservices
  await requestConsumer.run({
    eachMessage: async ({ message }) => {
      const request = JSON.parse(message.value.toString());
      const { correlationId, sender_topic } = request;

      // Process the request
      const response = await processRequest(request);

      // Send the response back to the response topic
      await producer.send({
        topic: `${sender_topic}-res-topic`,
        messages: [
          {
            value: JSON.stringify({
              ...response,
              correlationId,
              sender_topic: TOPIC,
            }),
          },
        ],
      });
    },
  });
};

// Simulated request processing function
const processRequest = async (request) => {
  // Here you would implement the actual processing logic for the request
  let action = request?.action || "";
  let response = { status: "success", err_msg: "", data: "no action" };
  if (action == 'delete-host-data') {
 
      const customer_id = request?.customer_id || "";
      const host_name = request?.host_name || "";
      const host_ip = request?.host_ip || "";

      if (!customer_id) {
        response = { status: "error", err_msg: "no customer_id", data: "" };
      } else {
        console.log("Hello......................")
        DeleteHostService.deleteHostRecords(customer_id,host_ip)
        console.log("Host Details: ",customer_id,host_name,host_ip)
      }
      
    }
  return response;
};

const sendRequest = async (message, topic) => {
  const correlationId = uuidv4();
  const responsePromise = new Promise((resolve, reject) => {
    pendingRequests.set(correlationId, { resolve, reject });
  });


  console.log("//////////////////////////////////////////////////////////**********************************///////////////////////////////////////////////")



  await producer.send({
    topic: `${topic}-req-topic`,
    messages: [
      {
        value: JSON.stringify({
          ...message,
          correlationId,
          sender_topic: TOPIC,
        }),
      },
    ],
  });

  console.log("123456:",responsePromise)
  return responsePromise;
};

const sendMessage = async (message, topic) => {
  const correlationId = uuidv4();
  let payload = { ...message, correlationId, sender_topic: TOPIC };
  await producer.send({
    topic: topic,
    messages: [
      {
        value: JSON.stringify(payload),
      },
    ],
  });
  return correlationId;
};

const consumeResponses = async (correlationId, callback) => {
  await consumer.run({
    eachMessage: async ({ message }) => {
      const response = JSON.parse(message.value.toString());
      if (response.correlationId === correlationId) {
        callback(null, response);
      }
    },
  });
};

module.exports = {
  initKafka,
  //   sendMessage,
  //   consumeResponses,
  sendRequest,
};
initKafka()
