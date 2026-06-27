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
  const { room, role, streamId } = socket.handshake.query;
  
  // Guard clause for malformed connection handshakes if checking live streaming routes
  if (room) {
    socket.join(room);
    console.log(`🔌 Connection: Socket ${socket.id} joined room [${room}] as (${role})`);
    
    // BACKEND SEED FIX: Index host components globally using their active room stream context 
    if (role === 'cohost_master' || role === 'host') {
      const hostIdentifier = streamId || room;
      activeUsers.set(hostIdentifier, socket.id);
      socket.hostIdentifier = hostIdentifier; // Bind reference context to socket for clean disconnect trace
      console.log(`📡 Registered Host globally in active reference index: [${hostIdentifier}] -> Socket ${socket.id}`);
    }
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

  // --- PASSIVE CALL SESSION & APP LAYER SIGNALING HOOKS ---
  socket.on('register_user_session', ({ userId }) => {
    socket.userId = userId;
    activeUsers.set(userId, socket.id);
    io.emit('friend_presence_changed', { userId, status: 'online' });
    console.log(`🟢 User ${userId} bound to notification session map: ${socket.id}`);
  });

  socket.on('join_call_room', ({ roomId, userId }) => {
    socket.join(roomId);
    console.log(`📞 Socket ${socket.id} joined dedicated P2P WebRTC call room: ${roomId}`);
    
    // Extract target peer from composite room signature (e.g., "userA-userB")
    const userIds = roomId.split("-");
    const peerUserId = userIds.find(id => id !== userId);
    const targetSocketId = activeUsers.get(peerUserId);

    if (targetSocketId) {
      console.log(`🔔 Forwarding real-time incoming call alert from ${userId} to target socket ${targetSocketId}`);
      io.to(targetSocketId).emit('incoming_call_signal', {
        fromUserId: userId,
        roomId: roomId
      });
    }
  });

  socket.on('reject_incoming_call', ({ roomId, to }) => {
    const targetSocketId = activeUsers.get(to);
    if (targetSocketId) {
      console.log(`🚫 Call rejected by peer. Notifying origin socket: ${targetSocketId}`);
      io.to(targetSocketId).emit('peer_hung_up');
    }
  });

  // --- CROSS-ROOM CO-HOST INVITE ROUTER DISPATCHERS ---
  socket.on('send_cohost_invite', (data) => {
    // data payload shape: { room, targetUserId, fromHostId, inviteFrom }
    const targetSocketId = activeUsers.get(data.targetUserId);

    if (targetSocketId) {
      console.log(`✉️ Cross-Room Signal: Routing invitation from Room [${data.room}] directly to Target Socket ID [${targetSocketId}]`);
      
      // Bypasses room boundaries and targets the absolute socket channel of the invitee!
      io.to(targetSocketId).emit('cohost_invite_received', {
        room: data.room,               // Room ID where the request originated
        fromHostId: data.fromHostId,   // Host string who requested the split feed
        inviteFrom: data.inviteFrom
      });
    } else {
      console.log(`⚠️ Signal Routing Aborted: Target Host ${data.targetUserId} is not active in the global session map.`);
    }
  });

  socket.on('respond_cohost_invite', (data) => {
    // data payload shape: { room, targetUserId, status } ('accepted' / 'declined')
    const originHostSocketId = activeUsers.get(data.targetUserId);

    if (originHostSocketId) {
      console.log(`📥 Routing invite response status [${data.status}] back to origin room host socket: ${originHostSocketId}`);
      io.to(originHostSocketId).emit('cohost_invite_accepted', {
        room: data.room,
        status: data.status
      });
    }
  });

  // --- STREAM REACTION ENGINE ---
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

    // Clean up host entries out of the allocation map safely
    if (socket.hostIdentifier) {
      activeUsers.delete(socket.hostIdentifier);
      console.log(`🛑 Removed Host mapping trace from indexing arrays: ${socket.hostIdentifier}`);
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
