const kafka = require('kafka-node');

const fs = require('fs');

// Read the configuration file
const rawConfig = fs.readFileSync('config.json');
const config = JSON.parse(rawConfig);

// SASL PLAINTEXT username and password
const saslConfig = {
  mechanism: 'PLAIN',
  username: config.username,
  password: config.password
};
// Kafka broker address and port
const kafkaHost = config.kafkaHost;

// Kafka client options
const kafkaClientOptions = {
    kafkaHost: kafkaHost,
    sasl: saslConfig,
    ssl: false, // Set to true if SSL is enabled
    // Other options like clientId, connectionTimeout, etc.
  };
    // Create Kafka client
const client = new kafka.KafkaClient(kafkaClientOptions);
  
const admin = new kafka.Admin(client);
const topicToCreate = [{
    topic: config.topicName,
    partitions: 1, // Number of partitions for the topic
    replicationFactor: 1 // Number of replications for the topic
}];

admin.createTopics(topicToCreate, (error, result) => {
    if (error) {
        console.error(`Error creating topic: ${error}`);
    } else {
        console.log(`Topic created: ${JSON.stringify(result)}`);
    }
});


const producer = new kafka.Producer(client);

producer.on('ready', () => {
    console.log('Producer is ready');
    
    // Send a message to Kafka topic
    producer.send([{ topic: config.topicName, messages: 'Hello from Kafka producer' }], (err, data) => {
        if (err) {
            console.error('Error:', err);
        } else {
            console.log('Message sent:', data);
        }
        // Close the producer after sending messages
        producer.close();
    });
});

producer.on('error', (err) => {
    console.error('Producer error:', err);
});