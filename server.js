const express = require('express');
const app = express();
const http = require('http').createServer(app);

// Initialize Socket.io with explicit CORS configuration for your frontend deployments
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

  // --- EXTENDED CHAT REAL-TIME CALL SIGNALING MAPPER ---
  socket.on('initiate_call_signal', (callPayload) => {
    // Expected payload shape: { receiverId, callerId, callerName, callType }
    const targetSocketId = activeUsers.get(callPayload.receiverId);
    if (targetSocketId) {
      console.log(`📞 Routing direct incoming ${callPayload.callType} call signal to target client: ${targetSocketId}`);
      io.to(targetSocketId).emit('incoming_call_signal', callPayload);
    }
  });

  socket.on('decline_call', ({ callerId }) => {
    const originCallerSocketId = activeUsers.get(callerId);
    if (originCallerSocketId) {
      console.log(`🚫 Call declined by receiver. Notifying origin socket: ${originCallerSocketId}`);
      io.to(originCallerSocketId).emit('call_cancelled_by_caller');
    }
  });

  // FIXED: Supports explicit targetPeerId to handle complex hyphenated UUID room names safely
  socket.on('join_call_room', ({ roomId, userId, targetPeerId }) => {
    socket.join(roomId);
    console.log(`📞 Socket ${socket.id} joined dedicated P2P WebRTC call room: ${roomId}`);
    
    // Explicit targeting avoids buggy splitting loops when UUIDs contain internal hyphens
    let peerUserId = targetPeerId;
    
    if (!peerUserId) {
      const userIds = roomId.split("-");
      // Fallback fallback if only standard text strings are utilized
      peerUserId = userIds.find(id => id !== userId);
    }

    if (peerUserId) {
      const targetSocketId = activeUsers.get(peerUserId);
      if (targetSocketId) {
        console.log(`🔔 Forwarding real-time incoming call alert from ${userId} to target socket ${targetSocketId}`);
        io.to(targetSocketId).emit('incoming_call_signal', {
          callerId: userId,
          roomId: roomId
        });
      } else {
        console.log(`📡 Target peer registration status lookup failed for: ${peerUserId}`);
      }
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
    const targetSocketId = activeUsers.get(data.targetUserId);
    if (targetSocketId) {
      console.log(`✉️ Cross-Room Signal: Routing invitation from Room [${data.room}] directly to Target Socket ID [${targetSocketId}]`);
      io.to(targetSocketId).emit('cohost_invite_received', {
        room: data.room,
        fromHostId: data.fromHostId,
        inviteFrom: data.inviteFrom
      });
    } else {
      console.log(`⚠️ Signal Routing Aborted: Target Host ${data.targetUserId} is not active in the global session map.`);
    }
  });

  socket.on('respond_cohost_invite', (data) => {
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

  socket.on('send_chat_message', (messagePayload) => {
    const targetSocketId = activeUsers.get(messagePayload.receiver_id);
    if (targetSocketId) {
      io.to(targetSocketId).emit('received_chat_message', messagePayload);
    }
  });

  socket.on('broadcast_message_update', (updatedPayload) => {
    const targetSocketId = activeUsers.get(updatedPayload.receiver_id);
    if (targetSocketId) {
      io.to(targetSocketId).emit('message_updated_realtime', updatedPayload);
    }
  });

  socket.on('user_typing_state', ({ userId, isTyping, mode }) => {
    socket.broadcast.emit('peer_typing_state_changed', { userId, isTyping, mode });
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
