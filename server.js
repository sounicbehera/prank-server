const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Cloud Native Requirement: Health check endpoint
app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

// Serve static files from the current directory
app.use(express.static(path.join(__dirname)));

// State Management (Multi-tenancy): Map<roomId, { operator: ws, candidate: ws }>
const rooms = new Map();

wss.on('connection', (ws) => {
    // Attach state to socket for O(1) cleanup
    ws.roomId = null;
    ws.role = null;

    ws.on('message', (messageAsString) => {
        let message;
        // Fault Tolerance: Wrap JSON.parse in try...catch
        try {
            message = JSON.parse(messageAsString);
        } catch (error) {
            console.error('Invalid JSON received, dropping packet:', error.message);
            return;
        }

        const roomId = message.roomId || 'global_room';

        // Ensure room exists in the Map
        if (!rooms.has(roomId)) {
            rooms.set(roomId, { operator: null, candidate: null });
        }
        const room = rooms.get(roomId);

        switch (message.type) {
            case 'register-operator':
                ws.roomId = roomId;
                ws.role = 'operator';
                room.operator = ws;
                console.log(`[Room: ${roomId}] Operator registered`);
                
                if (room.candidate) {
                    room.operator.send(JSON.stringify({ type: 'candidate-waiting' }));
                }
                break;

            case 'register-candidate':
                ws.roomId = roomId;
                ws.role = 'candidate';
                room.candidate = ws;
                console.log(`[Room: ${roomId}] Candidate registered`);
                
                if (room.operator) {
                    room.operator.send(JSON.stringify({ type: 'candidate-waiting' }));
                }
                break;

            case 'verify-candidate':
                if (room.candidate) {
                    room.candidate.send(JSON.stringify({ type: 'operator-ready' }));
                }
                break;

            case 'offer':
            case 'answer':
            case 'ice-candidate':
                // Strictly route payloads to the peer in the same room
                if (message.target === 'operator' && room.operator) {
                    room.operator.send(JSON.stringify(message));
                } else if (message.target === 'candidate' && room.candidate) {
                    room.candidate.send(JSON.stringify(message));
                }
                break;
                
            default:
                console.warn(`[Room: ${roomId}] Unknown message type dropped: ${message.type}`);
        }
    });

    ws.on('close', () => {
        // Memory Management: O(1) cleanup on close
        if (!ws.roomId || !ws.role) return;

        const room = rooms.get(ws.roomId);
        if (room) {
            if (ws.role === 'operator') {
                room.operator = null;
                console.log(`[Room: ${ws.roomId}] Operator disconnected`);
            } else if (ws.role === 'candidate') {
                room.candidate = null;
                console.log(`[Room: ${ws.roomId}] Candidate disconnected`);
                if (room.operator) {
                    room.operator.send(JSON.stringify({ type: 'candidate-disconnected' }));
                }
            }

            // Garbage Collection: Delete room to prevent O(N) leak
            if (!room.operator && !room.candidate) {
                rooms.delete(ws.roomId);
                console.log(`[Room: ${ws.roomId}] Room cleaned up and deleted`);
            }
        }
    });
});

const PORT = process.env.PORT || 3001;

// Cloud Native Requirement: Explicitly bind to '0.0.0.0'
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server is running on http://0.0.0.0:${PORT}`);
});
