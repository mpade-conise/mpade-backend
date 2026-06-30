const express = require('express');
const cors = require('cors'); // 1. Import the cors package
const app = express();
const http = require('http').createServer(app);
const path = require('path');
const fs = require('fs'); // Required for clean-up and permission validation
const os = require('os'); // Required for securing platform-independent temp folders

// 2. Enable global CORS middleware explicitly for Express HTTP router pathways
// Updated to support automatic handling of preflight OPTIONS requests cleanly
app.use(cors({
  origin: [
    "https://progress-lake.vercel.app", // Your live Vercel production deployment
    "http://localhost:5173"              // Your local Vite development server
  ],
  methods: ["GET", "POST", "OPTIONS"],
  credentials: true
}));

// 3. Import the core fluent-ffmpeg package
const ffmpeg = require('fluent-ffmpeg');

// 4. Fall back seamlessly to Render's stable system-level binary path
const systemFfmpegPath = '/usr/bin/ffmpeg';
if (fs.existsSync(systemFfmpegPath)) {
  ffmpeg.setFfmpegPath(systemFfmpegPath);
  console.log(`🚀 FFmpeg path successfully mapped to system production binary: ${systemFfmpegPath}`);
} else {
  console.log('ℹ️ Local environment detected or custom package path applied.');
}

// Add Express JSON parsing middleware for the inbound download request body
app.use(express.json());

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

// --- UPDATED DYNAMIC VIDEO/AUDIO STREAM MERGE ENDPOINT ---
app.post('/api/merge-video', async (req, res) => {
  const { videoUrl, audioUrl } = req.body;

  if (!videoUrl) {
    return res.status(400).json({ error: "Missing source videoUrl field." });
  }

  // Generate a secure temporary path local to the host container environment
  const outputFilename = `merged_${Date.now()}_${Math.floor(Math.random() * 1000)}.mp4`;
  const outputPath = path.join(os.tmpdir(), outputFilename);

  // Initialize fluent-ffmpeg targeting the remote source video URL
  let ffmpegCommand = ffmpeg().input(videoUrl).inputOptions(['-protocol_whitelist file,http,https,tcp,tls,crypto']);

  // Strict sanitization: check if the incoming tracking link is an actual URL string or empty/null
  const rawAudioStr = String(audioUrl).trim().toLowerCase();
  const hasCustomAudio = audioUrl && rawAudioStr !== 'null' && rawAudioStr !== 'undefined' && rawAudioStr !== '';

  if (hasCustomAudio) {
    // CASE 1: Video uses an embedded sound link from the database (music_url is populated)
    console.log(`🎵 Custom embedded audio track detected (${audioUrl}). Multiplexing audio stream layers...`);
    ffmpegCommand
      .input(audioUrl)
      .inputOptions(['-protocol_whitelist file,http,https,tcp,tls,crypto'])
      .outputOptions([
        '-c:v copy',             // Copy video frames instantly without expensive re-encoding
        '-c:a aac',              // Explicitly re-encode custom track to native AAC for container alignment
        '-b:a 128k',             // Secure a stable audio bit rate for standard media players
        '-map 0:v:0',            // Map the first input's raw video channel
        '-map 1:a:0',            // Map the second input's raw audio channel
        '-movflags +faststart',  // Relocates index metadata to the front so the file plays instantly
        '-shortest'              // Clip video/audio timeline to whichever finishes first
      ]);
  } else {
    // CASE 2: audioUrl is NULL/missing. Video relies entirely on its ORIGINAL NATIVE SOUND
    console.log('🗣️ Original native sound verified. Copying source media tracks directly...');
    ffmpegCommand
      .outputOptions([
        '-c:v copy',             // Pass video straight through without touch re-encoding
        '-c:a copy',             // COPY ORIGINAL NATIVE AUDIO STREAM DIRECTLY WITHOUT ALTERING IT
        '-movflags +faststart'   // Relocates index metadata to the front for smooth streaming downloads
      ]);
  }

  // Executes native backend processing by writing out to a workspace file on disk
  ffmpegCommand
    .toFormat('mp4')
    .on('start', (cmd) => {
      console.log('🎬 Started Video Pipeline Processing to Disk...');
    })
    .on('error', (err) => {
      console.error('❌ Server-Side Video Processing Pipeline Error:', err.message);
      if (!res.headersSent) {
        res.status(500).json({ error: `Video generation pipeline encountered an issue: ${err.message}` });
      }
      if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
    })
    .on('end', () => {
      console.log('✅ Video successfully generated on disk. Initializing download pipeline transfer...');
      
      // Serve the local processed file cleanly as an authorized binary attachment down to the browser
      res.download(outputPath, 'Mpade_Export.mp4', (downloadErr) => {
        if (downloadErr) {
          console.error('❌ Error during transmission file transfer:', downloadErr);
        }
        // Always clean up the temporary workspace file to prevent storage leakage
        if (fs.existsSync(outputPath)) {
          fs.unlinkSync(outputPath);
          console.log('🗑️ Cleaned up temporary processing file from disk.');
        }
      });
    })
    .save(outputPath); // Write directly to system disk first to guarantee cross-origin stream stability
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
    
    // Bind session parameters on fallback if the client is registering via direct call screen
    if (userId && !socket.userId) {
      socket.userId = userId;
      activeUsers.set(userId, socket.id);
    }

    let peerUserId = targetPeerId;
    if (!peerUserId) {
      const userIds = roomId.split("-");
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

  // --- LOW-LATENCY WEBRTC P2P SIGNALING PIPELINE (HYBRID AUDIO/VIDEO ROUTING) ---
  socket.on('request_host_stream', ({ streamId }) => {
    console.log(`📡 Forwarding explicit stream request from viewer (${socket.id}) to room channel [${streamId}]`);
    socket.to(streamId).emit('viewer_requesting_stream', { viewerSocketId: socket.id });
  });

  socket.on('send_webrtc_offer', (data) => {
    const { streamId, roomId, offer, targetViewerId, to } = data;
    const targetId = targetViewerId || to;
    
    // Smart P2P Lookup: Check if targetId matches an active logged-in User Session ID first
    const directUserSocket = activeUsers.get(targetId);
    
    if (directUserSocket) {
      console.log(`📤 Private Route: Forwarding WebRTC offer direct to Peer Socket: ${directUserSocket}`);
      io.to(directUserSocket).emit('webrtc_offer_received', { offer, hostSocketId: socket.id });
      io.to(directUserSocket).emit('webrtc_offer', { offer, hostSocketId: socket.id });
    } else if (targetId) {
      console.log(`📤 Stream Route: Direct routing host SDP offer to target viewer socket identifier: ${targetId}`);
      io.to(targetId).emit('webrtc_offer_received', { offer, hostSocketId: socket.id });
    } else {
      const activeRoom = roomId || streamId;
      console.log(`📤 Room Broadcast Route: Transmitting offer out to channel: ${activeRoom}`);
      socket.to(activeRoom).emit('webrtc_offer_received', { offer, hostSocketId: socket.id });
    }
  });

  socket.on('send_webrtc_answer', (data) => {
    const { streamId, roomId, answer, to } = data;
    
    const directUserSocket = activeUsers.get(to);
    if (directUserSocket) {
      console.log(`📥 Private Route: Forwarding WebRTC answer direct to Peer Socket: ${directUserSocket}`);
      io.to(directUserSocket).emit('webrtc_answer_received', { answer, viewerSocketId: socket.id });
      io.to(directUserSocket).emit('webrtc_answer', { answer, viewerSocketId: socket.id });
    } else {
      const activeRoom = streamId || roomId;
      console.log(`📥 Room Route: Routing WebRTC answer back to channel room context [${activeRoom}]`);
      socket.to(activeRoom).emit('webrtc_answer_received', { answer, viewerSocketId: socket.id });
      socket.to(activeRoom).emit('webrtc_answer', { answer, viewerSocketId: socket.id });
    }
  });

  socket.on('webrtc_ice_candidate', (data) => {
    const { streamId, roomId, candidate, targetSocketId, to, senderType } = data;
    const destinationUser = to;
    
    const directUserSocket = activeUsers.get(destinationUser);
    const targetId = targetSocketId || directUserSocket;
    
    if (targetId) {
      console.log(`🧊 Targeted Route: Shipping ICE Candidate directly to target socket instance: ${targetId}`);
      io.to(targetId).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
      io.to(targetId).emit('webrtc_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    } else {
      const activeRoom = streamId || roomId;
      console.log(`🧊 Room Broadcast Route: Distributing candidate across room channels: ${activeRoom}`);
      socket.to(activeRoom).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
      socket.to(activeRoom).emit('webrtc_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    }
  });

  // Legacy events alias alignment mappings
  socket.on('webrtc_offer', (data) => socket.emit('send_webrtc_offer', data));
  socket.on('webrtc_answer', (data) => socket.emit('send_webrtc_answer', data));
  socket.on('send_ice_candidate', (data) => socket.emit('webrtc_ice_candidate', data));

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
