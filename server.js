// written by smruti sourav sahoo
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(path.join(__dirname)));

// Space Complexity: O(N) where N is the number of active rooms.
// Using a Map provides O(1) time complexity for lookup, insertion, and deletion.
const rooms = new Map(); 

wss.on('connection', (ws) => {
    console.log('Client connected');

    ws.on('message', (messageAsString) => {
        let message;
        // 1. Error Boundary: Prevent process crash on bad payload
        try {
            message = JSON.parse(messageAsString);
        } catch (error) {
            console.error('Dropped malformed message:', messageAsString);
            return; 
        }

        const { type, roomId, target } = message;

        // Ensure room exists
        if (!rooms.has(roomId)) {
            rooms.set(roomId, { operator: null, candidate: null });
        }
        const room = rooms.get(roomId);

        switch (type) {
            case 'register-operator':
                room.operator = ws;
                // Attach roomId to the socket object for O(1) cleanup on disconnect
                ws.roomId = roomId; 
                ws.role = 'operator';
                if (room.candidate) {
                    room.operator.send(JSON.stringify({ type: 'candidate-waiting' }));
                }
                break;

            case 'register-candidate':
                room.candidate = ws;
                ws.roomId = roomId;
                ws.role = 'candidate';
                if (room.operator) {
                    room.operator.send(JSON.stringify({ type: 'candidate-waiting' }));
                }
                break;

            case 'offer':
            case 'answer':
            case 'ice-candidate':
                // Route strictly within the isolated Room context
                if (target === 'operator' && room.operator) {
                    room.operator.send(JSON.stringify(message));
                } else if (target === 'candidate' && room.candidate) {
                    room.candidate.send(JSON.stringify(message));
                }
                break;
        }
    });

    ws.on('close', () => {
        // 2. O(1) Cleanup based on properties attached during registration
        if (!ws.roomId) return; 
        
        const room = rooms.get(ws.roomId);
        if (!room) return;

        if (ws.role === 'operator') {
            room.operator = null;
        } else if (ws.role === 'candidate') {
            room.candidate = null;
            if (room.operator) {
                room.operator.send(JSON.stringify({ type: 'candidate-disconnected' }));
            }
        }

        // Garbage collection: If room is completely empty, delete it to free memory
        if (!room.operator && !room.candidate) {
            rooms.delete(ws.roomId);
        }
    });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
