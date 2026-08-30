const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Serve static files from the current directory
app.use(express.static(path.join(__dirname)));

// WebSocket signaling server logic
let operatorSocket = null;
let candidateSocket = null;

wss.on('connection', (ws) => {
    console.log('Client connected');

    ws.on('message', (messageAsString) => {
        const message = JSON.parse(messageAsString);

        switch (message.type) {
            case 'register-operator':
                operatorSocket = ws;
                console.log('Operator registered');
                if (candidateSocket) {
                    operatorSocket.send(JSON.stringify({ type: 'candidate-waiting' }));
                }
                break;

            case 'register-candidate':
                candidateSocket = ws;
                console.log('Candidate registered');
                // Notify operator if they are online
                if (operatorSocket) {
                    operatorSocket.send(JSON.stringify({ type: 'candidate-waiting' }));
                }
                break;

            case 'offer':
            case 'answer':
            case 'ice-candidate':
                // Relay messages between candidate and operator
                if (message.target === 'operator' && operatorSocket) {
                    operatorSocket.send(JSON.stringify(message));
                } else if (message.target === 'candidate' && candidateSocket) {
                    candidateSocket.send(JSON.stringify(message));
                }
                break;

            case 'verify-candidate':
                // Operator initiated verification, notify candidate
                if (candidateSocket) {
                    candidateSocket.send(JSON.stringify({ type: 'operator-ready' }));
                }
                break;
        }
    });

    ws.on('close', () => {
        if (ws === operatorSocket) {
            console.log('Operator disconnected');
            operatorSocket = null;
        } else if (ws === candidateSocket) {
            console.log('Candidate disconnected');
            candidateSocket = null;
            if (operatorSocket) {
                operatorSocket.send(JSON.stringify({ type: 'candidate-disconnected' }));
            }
        }
    });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
