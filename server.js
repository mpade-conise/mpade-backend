const express = require('express');
const app = express();
const http = require('http').createServer(app);

// Initialize Socket.io with explicit CORS configuration for your Vercel client
const io = require('socket.io')(http, {
  cors: {
    origin: [
      "https://progress-lake.vercel.app", // Your live Vercel production deployment
      "http://localhost:5173"              // Your local Vite development server
    ],
    methods: ["GET", "POST"],
    credentials: true
  }
});

// A quick health-check route to help wake up or ping the Render container manually
app.get('/', (req, res) => {
  res.send('🚀 Mpade Socket Core Signaling Machine Operational');
});

io.on('connection', (socket) => {
  const { room, role } = socket.handshake.query;
  
  // Guard clause for malformed connection handshakes
  if (!room) {
    console.log(`⚠️ Connection blocked: Missing room parameter for socket ${socket.id}`);
    socket.disconnect();
    return;
  }

  socket.join(room);
  console.log(`🔌 Connection: Socket ${socket.id} joined room [${room}] as (${role})`);

  // Helper function to broadcast updated room presence lists to the host dashboard
  const broadcastRoomPresence = async (roomName) => {
    try {
      // Fetch all active sockets currently occupying this room cluster
      const sockets = await io.in(roomName).fetchSockets();
      // Filter out only the viewers to construct a clean participant array
      const viewersList = sockets
        .filter(s => s.handshake.query.role === 'viewer' || s.handshake.query.role === 'signal-viewer')
        .map(s => ({ socketId: s.id, username: s.handshake.query.username || 'Anonymous' }));
      
      // Broadcast the updated collection down to everyone in the room
      io.to(roomName).emit('room_presence_update', viewersList);
    } catch (err) {
      console.error("❌ Presence tracking error:", err);
    }
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

  // 3. LOW-LATENCY WEBRTC P2P SIGNALING PIPELINE (OPTIMIZED TARGET ROUTING)
  socket.on('request_host_stream', ({ streamId }) => {
    console.log(`📡 Forwarding explicit stream request from viewer (${socket.id}) to room channel [${streamId}]`);
    // Tell the host client that a specific viewer socket ID is ready to parse a media offer
    socket.to(streamId).emit('viewer_requesting_stream', { viewerSocketId: socket.id });
  });

  socket.on('send_webrtc_offer', ({ streamId, offer, targetViewerId }) => {
    console.log(`📤 Direct routing host SDP offer to target viewer socket: ${targetViewerId}`);
    // Direct-route the host's generated SDP Offer safely to the targeted viewer connection ID
    io.to(targetViewerId).emit('webrtc_offer_received', { offer, hostSocketId: socket.id });
  });

  socket.on('send_webrtc_answer', ({ streamId, answer }) => {
    console.log(`📥 Routing viewer SDP answer response back to room channel [${streamId}]`);
    // Route the viewer's generated SDP Answer back up to the host stream room ecosystem
    socket.to(streamId).emit('webrtc_answer_received', { answer, viewerSocketId: socket.id });
  });

  socket.on('webrtc_ice_candidate', ({ streamId, candidate, targetSocketId, senderType }) => {
    // If an explicit target destination is provided, bypass the room and send it straight to that node
    if (targetSocketId) {
      console.log(`⚡ Direct-routing ICE Candidate from (${senderType}) straight to socket target: ${targetSocketId}`);
      io.to(targetSocketId).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    } else {
      // Fallback: Broadcast candidates to the room matrix, ignoring the sender socket node
      console.log(`🗣️ Room broadcasting ICE Candidate from (${senderType}) inside channel: [${streamId}]`);
      socket.to(streamId).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    }
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

// server.js (Append to your existing Express / Socket.io server config)
const activeUsers = new Map(); // Tracks userId -> socketId lines maps

io.on('connection', (socket) => {
  console.log(`🔌 New client handshaking: ${socket.id}`);

  // 1. Authenticate user connection and store global presence mapping
  socket.on('user_going_online', (userId) => {
    socket.userId = userId;
    activeUsers.set(userId, socket.id);
    
    // Broadcast globally that this friend is active
    io.emit('friend_presence_changed', { userId, status: 'online' });
    console.log(`🟢 User ${userId} linked to session link ${socket.id}`);
  });

  // 2. Explicitly bind a client to a conversation channel room line
  socket.on('join_chat_room', ({ roomId }) => {
    socket.join(roomId);
    console.log(`🎯 Session ${socket.id} joined conversation pipeline room: ${roomId}`);
  });

  // 3. Realtime message multiplexing engine
  socket.on('send_chat_message', (messagePayload) => {
    const { id, room_id, sender_id, content, type, created_at } = messagePayload;

    // Instantly dispatch to all concurrent users listening inside that room channel
    socket.to(room_id).emit('received_chat_message', {
      id,
      room_id,
      sender_id,
      content,
      type,
      created_at,
      is_read: false
    });
  });

  // 4. Realtime indicator event layer (Facebook style "... is typing")
  socket.on('user_typing_state', ({ room_id, userId, isTyping }) => {
    socket.to(room_id).emit('peer_typing_state_changed', { userId, isTyping });
  });

  // 5. Connection Teardown Handler
  socket.on('disconnect', () => {
    if (socket.userId) {
      activeUsers.delete(socket.userId);
      io.emit('friend_presence_changed', { userId: socket.userId, status: 'offline' });
      console.log(`🔴 User ${socket.userId} went offline.`);
    }
  });
});
const PORT = process.env.PORT || 4000;
http.listen(PORT, () => {
  console.log(`🚀 Socket signaling machine operational on port ${PORT}`);
});
