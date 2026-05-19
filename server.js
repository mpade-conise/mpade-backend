const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
  cors: { origin: "*" } 
});

// A quick health-check route to help wake up or ping the Render container manually
app.get('/', (req, res) => {
  res.send('🚀 Mpade Socket Core Signaling Machine Operational');
});

io.on('connection', (socket) => {
  const { room, role } = socket.handshake.query;
  
  // Guard clause for malformed connection handshakes
  if (!room) {
    socket.disconnect();
    return;
  }

  socket.join(room);
  console.log(`🔌 Connection: Socket ${socket.id} joined room [${room}] as (${role})`);

  // Helper function to broadcast updated room presence lists to the host dashboard
  const broadcastRoomPresence = async (roomName) => {
    // Fetch all active sockets currently occupying this room cluster
    const sockets = await io.in(roomName).fetchSockets();
    // Filter out only the viewers to construct a clean participant array
    const viewersList = sockets
      .filter(s => s.handshake.query.role === 'viewer' || s.handshake.query.role === 'signal-viewer')
      .map(s => ({ socketId: s.id, username: s.handshake.query.username || 'Anonymous' }));
    
    // Broadcast the updated collection down to everyone in the room
    io.to(roomName).emit('room_presence_update', viewersList);
  };

  // 1. PRESENCE INITIALIZATION LOGIC
  if (role === 'viewer' || role === 'signal-viewer') {
    // Notify the host UI component to prompt alert banners or greeting overlays
    socket.to(room).emit('viewer_joined', { 
      id: socket.id, 
      username: socket.handshake.query.username 
    });
    
    // Refresh the live viewer tally layout immediately
    broadcastRoomPresence(room);
  }

  // 2. SOCIAL & INTERACTION ROUTING LOGIC
  socket.on('send_battle_invite', (data) => {
    socket.to(data.targetRoomId).emit('battle_invite_received', data);
  });

  socket.on('accept_battle_invite', (data) => {
    // Notify the challenger's dashboard room that the invite was accepted live
    socket.to(data.challengerRoomId).emit('battle_invite_accepted', data);
  });

  socket.on('send_reaction', (data) => {
    // Broadcast real-time stream heart/smile floating animations to all viewers in the room
    socket.to(room).emit('received_reaction', data);
  });

  // 3. LOW-LATENCY WEBRTC P2P SIGNALING PIPELINE
  socket.on('request_host_stream', ({ streamId }) => {
    // Tell the host client that a specific viewer socket ID is ready to parse a media offer
    socket.to(streamId).emit('viewer_requesting_stream', { viewerSocketId: socket.id });
  });

  socket.on('send_webrtc_offer', ({ streamId, offer, targetViewerId }) => {
    // Direct-route the host's generated SDP Offer safely to the target viewer
    io.to(targetViewerId).emit('webrtc_offer_received', { offer });
  });

  socket.on('send_webrtc_answer', ({ streamId, answer }) => {
    // Route the viewer's generated SDP Answer back up to the host stream room
    socket.to(streamId).emit('webrtc_answer_received', { answer });
  });

  socket.on('webrtc_ice_candidate', ({ streamId, candidate, senderType }) => {
    // Relay raw network routing candidates dynamically between alternative peer nodes
    socket.to(streamId).emit('incoming_ice_candidate', { candidate, senderType });
  });

  // 4. DISCONNECT / CLEANUP LOGIC
  socket.on('disconnect', () => {
    console.log(`❌ Disconnected: Socket ${socket.id} exited room [${room}]`);
    socket.leave(room);
    
    // If a viewer leaves, recount the active clients so the host's count updates immediately
    if (role === 'viewer' || role === 'signal-viewer') {
      broadcastRoomPresence(room);
    }
  });
});

const PORT = process.env.PORT || 4000;
http.listen(PORT, () => {
  console.log(`🚀 Socket signaling machine operational on port ${PORT}`);
});
