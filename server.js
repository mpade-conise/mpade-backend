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

// Tracks global user sessions dynamically (userId -> socketId)
const activeUsers = new Map(); 

// A quick health-check route to help wake up or ping the Render container manually
app.get('/', (req, res) => {
  res.send('🚀 Mpade Socket Core Signaling Machine Operational');
});

io.on('connection', (socket) => {
  const { room, role } = socket.handshake.query;
  
  // Guard clause for malformed connection handshakes if checking live streaming routes
  if (room) {
    socket.join(room);
    console.log(`🔌 Connection: Socket ${socket.id} joined room [${room}] as (${role})`);
  } else {
    console.log(`🔌 New client handshaking without room parameter (General Session): ${socket.id}`);
  }

  // Helper function to broadcast updated room presence lists to the host dashboard
  const broadcastRoomPresence = async (roomName) => {
    try {
      const sockets = await io.in(roomName).fetchSockets();
      const viewersList = sockets
        .filter(s => s.handshake.query.role === 'viewer' || s.handshake.query.role === 'signal-viewer')
        .map(s => ({ socketId: s.id, username: s.handshake.query.username || 'Anonymous' }));
      
      io.to(roomName).emit('room_presence_update', viewersList);
    } catch (err) {
      console.error("❌ Presence tracking error:", err);
    }
  };

  // PRESENCE INITIALIZATION LOGIC
  if (room && (role === 'viewer' || role === 'signal-viewer')) {
    socket.to(room).emit('viewer_joined', { 
      id: socket.id, 
      username: socket.handshake.query.username 
    });
    broadcastRoomPresence(room);
  }

  // --- ALLIANCE MATCHMAKING SYSTEM CORES (FIXED EVENT KEYNAMES) ---
  
  // 1. Frontend fires send_cohost_invite -> Relay to targeted room
  socket.on('send_cohost_invite', (data) => {
    const destinationRoom = data.targetStreamId || data.targetRoomId;
    console.log(`⚔️ Relaying battle invitation from ${data.senderUsername} into target stream room: [${destinationRoom}]`);
    
    // Broadcast cleanly to the target stream's room cluster
    socket.to(destinationRoom).emit('battle_invite_received', {
      senderStreamId: data.hostRoomId,
      senderUsername: data.senderUsername,
      senderHostId: data.senderHostId
    });
  });

  // 2. Frontend fires accept_battle_invite -> Handshake back to the challenger
  socket.on('accept_battle_invite', (data) => {
    console.log(`✅ Battle invitation accepted between Host Room [${data.hostRoomId}] and Challenger Room [${data.challengerRoomId}]`);
    socket.to(data.challengerRoomId).emit('battle_invite_accepted', data);
  });

  // STREAM REACTION ENGINE
  socket.on('send_reaction', (data) => {
    if (room) socket.to(room).emit('received_reaction', data);
  });

  // --- LOW-LATENCY WEBRTC P2P SIGNALING PIPELINE ---
  socket.on('request_host_stream', ({ streamId }) => {
    console.log(`📡 Forwarding explicit stream request from viewer (${socket.id}) to room channel [${streamId}]`);
    socket.to(streamId).emit('viewer_requesting_stream', { viewerSocketId: socket.id });
  });

  socket.on('send_webrtc_offer', ({ streamId, offer, targetViewerId }) => {
    console.log(`📤 Direct routing host SDP offer to target viewer socket: ${targetViewerId}`);
    io.to(targetViewerId).emit('webrtc_offer_received', { offer, hostSocketId: socket.id });
  });

  socket.on('send_webrtc_answer', ({ streamId, answer }) => {
    console.log(`📥 Routing viewer SDP answer response back to room channel [${streamId}]`);
    socket.to(streamId).emit('webrtc_answer_received', { answer, viewerSocketId: socket.id });
  });

  socket.on('webrtc_ice_candidate', ({ streamId, candidate, targetSocketId, senderType }) => {
    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    } else {
      socket.to(streamId).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    }
  });

  // --- GLOBAL MESSAGING & FRIEND PRESENCE CORES ---
  socket.on('user_going_online', (userId) => {
    socket.userId = userId;
    activeUsers.set(userId, socket.id);
    io.emit('friend_presence_changed', { userId, status: 'online' });
    console.log(`🟢 User ${userId} linked to live session: ${socket.id}`);
  });

  socket.on('join_chat_room', ({ roomId }) => {
    socket.join(roomId);
    console.log(`🎯 Session ${socket.id} joined conversation pipeline room: ${roomId}`);
  });

  socket.on('send_chat_message', (messagePayload) => {
    const { id, room_id, sender_id, content, type, created_at } = messagePayload;
    socket.to(room_id).emit('received_chat_message', {
      id, room_id, sender_id, content, type, created_at, is_read: false
    });
  });

  socket.on('user_typing_state', ({ room_id, userId, isTyping }) => {
    socket.to(room_id).emit('peer_typing_state_changed', { userId, isTyping });
  });

  // DISCONNECT / CLEANUP LOGIC
  socket.on('disconnect', () => {
    console.log(`❌ Disconnected: Socket ${socket.id}`);
    
    if (room) {
      socket.leave(room);
      if (role === 'viewer' || role === 'signal-viewer') {
        broadcastRoomPresence(room);
      }
    }

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
