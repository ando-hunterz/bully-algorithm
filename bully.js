// Require libraries
const cron = require("node-cron");
const os = require("os");
const axios = require('axios').default;

//Connect to the IP address of the mqtt container
var mqtt = require('mqtt');
var client  = mqtt.connect('mqtt://mosquitto')
const topic = "main-channel";
let coordinator;
let resultSet = new Set();
let coordinatorCheck = false;
let receivedElection = false;
let electionState = false;
let electionSend = [];
let hostname = process.env.HOSTNAME;

const express = require('express')
const app = express()
const port = 80;

app.use(express.json())

app.get('/health', (req, res) => {
    return res.send('ok');
})

app.post('/reply', (req, res) => {
    const { hostname } = req.body
    electionSend = [];
    return res.send("Ok");
})

app.listen(port, () => {
    console.log(`Server is listening on http://localhost:${port}`)
})

console.log(hostname)
  

client.on('connect', function () {
    client.subscribe('main-channel', function(err) {
        if(!err) {
            console.log('connected');
            sendContainerId();
        } else {
            console.log(err)
        }
    }) 
})

function sendContainerId() {
    const msg = `Discovery_My id is ${hostname}`;

    console.log(msg)
    client.publish(topic, msg);
}

  

//Receiver
client.on('message', function (topic, message) {
    // message is Buffer
    let stringMessage = message.toString();
    let type = stringMessage.split('_')[0]
    if(type == 'Discovery') initLeader(stringMessage.split('_')[1]);
    if(type == 'Discovery-Reply') addDatabase(stringMessage.split('_')[1]) 
    if(type == 'Election-Init') getElectInitiator(stringMessage.split('_')[1])
    if(type == 'Coordinator') setCoordinator(stringMessage.split('_')[1])
    // if(type == 'Alive') coordinatorCheck = true;
    // if(type == 'Election') checkElection(stringMessage.split('_')[1]);
    // if(type == 'Reply') checkReply(stringMessage.split('_')[1]);
})

function initLeader(stringMessage) {
    const id = stringMessage
              .toString()
              .split("is")[1]
              .trim();
    resultSet.add(id);

    if(resultSet.size > 0) {
        const msg = `Discovery-Reply_My id is ${hostname}`;

        console.log(msg)
        client.publish(topic, msg);
    }

    const findCoordinator = Array.from(resultSet).sort().reverse();
    coordinator = findCoordinator[0]
    console.log("Connected Id is "+Array.from(resultSet).join(' '))
    console.log(`Coordinator is ${coordinator}`)
}

function addDatabase(stringMessage){
    const id = stringMessage
              .toString()
              .split("is")[1]
              .trim();
    resultSet.add(id);
    const findCoordinator = Array.from(resultSet).sort().reverse();
    coordinator = findCoordinator[0]
    console.log("Connected Id is "+Array.from(resultSet).join(' '))
    console.log(`Coordinator is ${coordinator}`)
}

function setCoordinator(stringMessage) {
    const coordinator_id = stringMessage
                        .toString()
                        .split("is")[1]
                        .trim();
    coordinator = coordinator_id;
    console.log("New Coordinator is "+coordinator)
    receivedElection = electionState = false;
    electionSend = [];
}

function sendCoordinator() {
    coordinator = hostname;
    let msg = `Coordinator_Coordinator is ${hostname}`
    console.log(msg);
    client.publish(topic, msg);
}

async function checkCoordinatorHealth () {
    try{
        const { data } = await axios.get(`http://${coordinator}/health`)
    }
    catch (err) {
        coordinator = null;
        setTimeout(checkElectLeader , Math.floor(Math.random() * 100))
    }
        
}

function checkElectLeader(){
    if(!receivedElection && !electionState) electLeader(); 
}

function electLeader() {
    receivedElection = false;
    electionState = true;
    let msg = `Election-Init_Election initiate by ${hostname}`
    console.log(msg);
    client.publish(topic, msg);
    electionSend = Array.from(resultSet).sort();
    electionSend.filter((item) => item < hostname);
    electionSend.forEach(item => console.log('Election sent to '+item));
    setTimeout(() => {
        if(electionSend.length > 1) sendCoordinator();
    }, 1000)
}

async function getElectInitiator(stringMessage) {
    receivedElection = electionState =  true;
    const initiator = stringMessage
                        .toString()
                        .split("by")[1]
                        .trim();
    console.log('Initiation Received from '+initiator)
    try {
        if(initiator < hostname && initiator != hostname) {
            const {data} = await axios.post(`http://${initiator}/reply`, {hostname: hostname})
            console.log(data);
            receivedElection = false;
            if(data == "Ok") electLeader(); 
        }
    } catch (err) {
        console.log(err)
    }
}

cron.schedule("*/5 * * * * *", () => {
    checkCoordinatorHealth();
})


