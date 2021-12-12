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
let checkTimeout = null;
let receivedElection = false;
let electionState = false;
let electionSend = [];
let elected = false;
let coordinatorReceived = false;
let hostname = process.env.HOSTNAME;

const express = require('express')
const app = express()
const port = 80;

app.use(express.json())

app.get('/health', (req, res) => {
    return res.send('Coordinator is Alive');
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
    checkRoutine.stop()
    const id = stringMessage
              .toString()
              .split("is")[1]
              .trim();
    resultSet.add(id);
    const findCoordinator = Array.from(resultSet).sort().reverse();
    coordinator = findCoordinator[0]
    console.log("Connected Id is "+Array.from(resultSet).join(' '))
    console.log(`Coordinator is ${coordinator}`)
    checkRoutine.start();
}

function setCoordinator(stringMessage) {
    clearTimeout(checkTimeout)
    const coordinator_id = stringMessage
                        .toString()
                        .split("is")[1]
                        .trim();
    coordinator = coordinator_id;
    coordinatorReceived = true;
    receivedElection = electionState = elected = false;
    electionSend = [];
    checkRoutine.start()
}

function sendCoordinator() {
    coordinator = hostname;
    let msg = `Coordinator_Coordinator is ${hostname}`
    console.log(msg);
    client.publish(topic, msg);
    clearTimeout(checkTimeout)
}

async function checkCoordinatorHealth () {
    try{
        const { data } = await axios.get(`http://${coordinator}/health`)
        console.log(data)
        coordinatorReceived = false
    }
    catch (err) {
        checkTimeout = setTimeout(checkElectLeader , Math.floor(Math.random() * 100))
    }
        
}

function checkElectLeader(){
    if(!coordinatorReceived && !receivedElection && !electionState) electLeader(); 
}

function electLeader() {
    receivedElection = false;
    electionState = true;
    let msg = `Election-Init_Election initiate by ${hostname}`
    console.log(msg);
    client.publish(topic, msg);
    checkRoutine.stop()
    electionSend = Array.from(resultSet).sort();
    electionSend.filter((item) => item < hostname);
    electionSend.forEach(item => console.log('Election sent to '+item));
    setTimeout(sendCoordinatorEvent, 100)
}

function sendCoordinatorEvent(){
    if(electionSend.length > 1) sendCoordinator();
}


async function getElectInitiator(stringMessage) {
    checkRoutine.stop();
    coordinator = null;
    receivedElection = electionState =  true;
    const initiator = stringMessage
                        .toString()
                        .split("by")[1]
                        .trim();
    console.log('Initiation Received from '+initiator)
    clearTimeout(checkTimeout)
    try {
        if(initiator < hostname && initiator != hostname) {
            console.log('postinggg..')
            const {data} = await axios.post(`http://${initiator}/reply`, {hostname: hostname})
            console.log(data);
            if(data == "Ok") {
                if(!elected) {
                    console.log('im electing myself')
                    elected = true;
                    electLeader(); 
                }  
            }
        }
    } catch (err) {
        console.log(err)
    }
}

const checkRoutine = cron.schedule("*/5 * * * * *", () => {
    checkCoordinatorHealth();
})


