// Require libraries
const cron = require("node-cron");
const os = require("os");

//Connect to the IP address of the mqtt container
var mqtt = require('mqtt');
const { send } = require("process");
var client  = mqtt.connect('mqtt://mosquitto')
const topic = "main-channel";
let coordinator;
let resultSet = new Set();
let coordinatorCheck = false;
let electionReceived = false;
let electionState = false;
let electionSend = [];
let hostname = os.hostname;

// client.on('connect', function () {
//     client.subscribe('main-channel', function(err) {
//         if(!err) {
//             console.log('connected');
//             const msg = `Discovery_My id is ${os.hostname}`;

//             console.log(msg)
//             client.publish(topic, msg);
//         } else {
//             console.log(err)
//         }
//     }) 
// })

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
    const msg = `Discovery_My id is ${os.hostname}`;

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
    if(type == 'Alive') coordinatorCheck = true;
    if(type == 'Coordinator') checkCoordinator(stringMessage.split('_')[1])
    if(type == 'Election') checkElection(stringMessage.split('_')[1]);
    if(type == 'Reply') checkReply(stringMessage.split('_')[1]);
})

function initLeader(stringMessage) {
    const id = stringMessage
              .toString()
              .split("is")[1]
              .trim();
    resultSet.add(id);

    if(resultSet.size > 0) {
        const msg = `Discovery-Reply_My id is ${os.hostname}`;

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

function checkReply(stringMessage) {
    const id = stringMessage
                .toString()
                .split("is")[1]
                .trim();
    electionSend.splice(electionSend.indexOf(id),1);
    console.log('reply received');
}

function checkElection(stringMessage) {
    electionReceived == true
    console.log('received election')
    const id = stringMessage
                .toString()
                .split("is")[1]
                .trim();
    console.log( os.hostname > id)
    if(os.hostname > id) {
        let msg = `Reply_i am ${os.hostname}}`
        console.log(msg)
        client.publish(topic, msg)
        electionReceived = false;
        sendElection()
    }
}

function keepAliveCoordinator() {
    if(os.hostname != coordinator) return
    const msg = `Alive_Coordinator is ${os.hostname}`;
    console.log(msg)
    client.publish(topic, msg)
}

function checkForFailure() {
    setTimeout(() => {}, 100)
    console.log('Coord id = '+coordinator)
    console.log(coordinatorCheck)
    if(coordinatorCheck == false) {
        electionState = true;
        console.log('coord died')
        resultSet.delete(coordinator);
        // const findCoordinator = Array.from(resultSet).sort().reverse();
        // if(findCoordinator == os.hostname) electSelfCoordinator();
       sendElection();
    }
}

function electSelfCoordinator() {
    electionState = electionReceived = false;
    coordinator = os.hostname;
    let msg = `Coordinator_coordinator is ${os.hostname}`
    console.log(msg)
    client.publish(topic, msg)
}

function sendElection() {
    console.log("send election")
    setTimeout(() => {}, Math.floor(Math.random * 200));
    if(!electionReceived){
        electionSend = Array.from(resultSet).sort();
        electionSend.filter((item) => item < os.hostname);
        electionSend.forEach(item => console.log('Election sent to '+item));
        let msg = `Election_election is from ${os.hostname}`
        console.log(msg)
        if(!electionReceived) {
            electionReceived = true;
            client.publish(topic, msg)
        }      
    }
}

function checkCoordinator(stringMessage) {
    const id = stringMessage
              .toString()
              .split("is")[1]
              .trim();
    coordinator = id;
    electionReceived = electionState = false;
}

function checkReplyFailed(){
    if(electionSend.size > 0) electSelfCoordinator();
}

cron.schedule("*/1 * * * * *", () => {
    if(electionReceived == false && electionState == false) keepAliveCoordinator()
    coordinatorCheck = false
    if(coordinator == os.hostname) coordinatorCheck = true
})

cron.schedule(`5 * * * * *`, () => {
    checkForFailure();
    if(electionState) checkReplyFailed()
    console.log("Connected ID is "+Array.from(resultSet).join(' '))
})


