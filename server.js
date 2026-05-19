// server.js
const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
  cors: { origin: "*" } // Allow your GitHub pages domain to hit the server
});

io.on('connection', (socket) => {
  const { room, role } = socket.handshake.query;
  socket.join(room);

  // Notify the rest of the stream that someone joined
  if (role === 'viewer') {
    socket.to(room).emit('viewer_joined', { id: socket.id });
  }

  // Handle immediate battle challenge routing
  socket.on('send_battle_invite', (data) => {
    socket.to(data.targetRoomId).emit('battle_invite_received', data);
  });

  socket.on('disconnect', () => {
    socket.leave(room);
  });
});

http.listen(process.env.PORT || 4000, () => {
  console.log("🚀 Socket signaling machine operational on port 4000");
});