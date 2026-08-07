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
    "http://localhost:5173"               // Your local Vite development server
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
      "http://localhost:5173"               // Your local Vite development server
    ],
    methods: ["GET", "POST"],
    credentials: true
  }
});

// Tracks global user sessions dynamically (userId -> socketId)
const activeUsers = new Map(); 

// Tracks multi-panel stream room states (streamId -> { hostSocketId, guestPanels: Map(guestId -> socketId) })
const streamRooms = new Map();

// A quick health-check route to help wake up or ping the Render container manually
app.get('/', (req, res) => {
  res.send('🚀 Mpade Socket Core Signaling Machine Operational');
});

// --- REINFORCED DYNAMIC VIDEO/AUDIO STREAM MERGE ENDPOINT ---
app.post('/api/merge-video', async (req, res) => {
  const { videoUrl, audioUrl } = req.body;

  if (!videoUrl) {
    return res.status(400).json({ error: "Missing source videoUrl field." });
  }

  // Generate a secure temporary path local to the host container environment
  const outputFilename = `merged_${Date.now()}_${Math.floor(Math.random() * 1000)}.mp4`;
  const outputPath = path.join(os.tmpdir(), outputFilename);

  // Initialize fluent-ffmpeg targeting the remote source video URL
  let ffmpegCommand = ffmpeg()
    .input(videoUrl)
    .inputOptions([
      '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
      '-fflags', '+genpts'
    ]);

  let hasCustomAudio = false;
  if (audioUrl) {
    const cleanAudioStr = String(audioUrl).trim().toLowerCase();
    if (cleanAudioStr !== '' && cleanAudioStr !== 'null' && cleanAudioStr !== 'undefined') {
      hasCustomAudio = true;
    }
  }

  if (hasCustomAudio) {
    console.log(`🎵 Custom embedded audio track detected (${audioUrl}). Multiplexing audio stream layers...`);
    ffmpegCommand
      .input(audioUrl)
      .inputOptions([
        '-protocol_whitelist', 'file,http,https,tcp,tls,crypto',
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      ])
      .outputOptions([
        '-c:v copy',             // Copy video frames instantly without re-encoding
        '-c:a aac',              // Explicitly re-encode custom track to native AAC
        '-b:a 128k',             // Secure a stable audio bit rate
        '-map 0:v:0',            // Map the first input's raw video channel
        '-map 1:a:0',            // Map the second input's raw audio channel
        '-map_metadata -1',      // Clears container sync blockages
        '-movflags +faststart',  // Faststart for instant streaming
        '-shortest'              // Clip timeline to shorter input
      ]);
  } else {
    console.log('🗣️ Original native sound verified. Copying source media tracks directly...');
    ffmpegCommand
      .outputOptions([
        '-c:v copy',
        '-c:a copy',
        '-movflags +faststart'
      ]);
  }

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
      console.log('✅ Video successfully generated on disk. Initializing download pipeline transfer... ');
      res.download(outputPath, 'Mpade_Export.mp4', (downloadErr) => {
        if (downloadErr) {
          console.error('❌ Error during transmission file transfer:', downloadErr);
        }
        if (fs.existsSync(outputPath)) {
          fs.unlinkSync(outputPath);
          console.log('🗑️ Cleaned up temporary processing file from disk.');
        }
      });
    })
    .save(outputPath);
});

io.on('connection', (socket) => {
  const { room, role, streamId } = socket.handshake.query;
  
  if (room) {
    socket.join(room);
    console.log(`🔌 Connection: Socket ${socket.id} joined room [${room}] as (${role})`);
    
    if (role === 'cohost_master' || role === 'host') {
      const hostIdentifier = streamId || room;
      activeUsers.set(hostIdentifier, socket.id);
      socket.hostIdentifier = hostIdentifier;

      // Register host room in multi-panel registry
      if (!streamRooms.has(hostIdentifier)) {
        streamRooms.set(hostIdentifier, {
          hostSocketId: socket.id,
          guestPanels: new Map()
        });
      } else {
        streamRooms.get(hostIdentifier).hostSocketId = socket.id;
      }

      console.log(`📡 Registered Host globally in active reference index: [${hostIdentifier}] -> Socket ${socket.id}`);
    }
  } else {
    console.log(`🔌 New client handshaking without room parameter (General Session): ${socket.id}`);
  }

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

  if (room && (role === 'viewer' || role === 'signal-viewer')) {
    socket.to(room).emit('viewer_joined', { 
      id: socket.id, 
      username: socket.handshake.query.username 
    });
    broadcastRoomPresence(room);
  }

  socket.on('register_user_session', ({ userId }) => {
    if (!userId) return;
    socket.userId = userId;
    activeUsers.set(userId, socket.id);
    io.emit('friend_presence_changed', { userId, status: 'online' });
    console.log(`🟢 User ${userId} bound to notification session map: ${socket.id}`);
  });

  // --- 1-ON-1 DIRECT VIDEO/AUDIO CALL ROUTING (PRESERVED) ---

  socket.on('initiate_call_signal', (callPayload) => {
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

  socket.on('join_call_room', ({ roomId, userId, targetPeerId }) => {
    socket.join(roomId);
    console.log(`📞 Socket ${socket.id} joined dedicated P2P WebRTC call room: ${roomId}`);
    
    if (userId) {
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
      }
    }
  });

  socket.on('peer_ready', ({ roomId, userId }) => {
    console.log(`⚡ Receiver/Peer (${userId || socket.id}) is mounted and ready in room: ${roomId}`);
    socket.to(roomId).emit('peer_ready', { userId, socketId: socket.id });
  });

  socket.on('reject_incoming_call', ({ roomId, to }) => {
    const targetSocketId = activeUsers.get(to);
    if (targetSocketId) {
      console.log(`🚫 Call rejected by peer. Notifying origin socket: ${targetSocketId}`);
      io.to(targetSocketId).emit('peer_hung_up');
    } else if (roomId) {
      socket.to(roomId).emit('peer_hung_up');
    }
  });

  // --- MULTI-PANEL LIVE STREAMING INGEST & RELAY EVENTS ---

  socket.on('publish_guest_feed', ({ streamId, guestId, targetHostId, sdpOffer, mode }) => {
    const targetHostSocketId = activeUsers.get(targetHostId) || streamRooms.get(streamId)?.hostSocketId;

    if (!streamRooms.has(streamId)) {
      streamRooms.set(streamId, { hostSocketId: targetHostSocketId, guestPanels: new Map() });
    }
    const room = streamRooms.get(streamId);
    room.guestPanels.set(guestId, socket.id);
    socket.data = { ...socket.data, isGuestPanel: true, guestId, streamId };

    console.log(`🎥 [MULTI-PANEL INGEST] Guest ${guestId} sending ${mode} stream feed to Host ${targetHostId}`);

    if (targetHostSocketId) {
      io.to(targetHostSocketId).emit('incoming_guest_panel_feed', {
        guestId,
        guestSocketId: socket.id,
        sdpOffer,
        mode
      });
    } else {
      socket.to(streamId).emit('incoming_guest_panel_feed', {
        guestId,
        guestSocketId: socket.id,
        sdpOffer,
        mode
      });
    }
  });

  socket.on('host_ack_guest_feed', ({ guestSocketId, sdpAnswer, guestId }) => {
    console.log(`✅ [MULTI-PANEL ACK] Host accepted panel stream from guest ${guestId}`);
    io.to(guestSocketId).emit('broadcast_ack_received', { sdpAnswer });
  });

  socket.on('guest_ice_candidate', ({ streamId, candidate, to }) => {
    const targetHostSocketId = activeUsers.get(to) || streamRooms.get(streamId)?.hostSocketId;
    if (targetHostSocketId) {
      io.to(targetHostSocketId).emit('incoming_guest_ice', { candidate, fromGuestSocketId: socket.id });
    } else if (streamId) {
      socket.to(streamId).emit('incoming_guest_ice', { candidate, fromGuestSocketId: socket.id });
    }
  });

  socket.on('host_ice_candidate', ({ targetGuestSocketId, candidate }) => {
    if (targetGuestSocketId) {
      io.to(targetGuestSocketId).emit('incoming_host_ice', { candidate });
    }
  });

  socket.on('remove_guest_panel', ({ streamId, guestId }) => {
    const room = streamRooms.get(streamId);
    if (room && room.guestPanels.has(guestId)) {
      const guestSocketId = room.guestPanels.get(guestId);
      io.to(guestSocketId).emit('removed_from_panel');
      room.guestPanels.delete(guestId);
      console.log(`🚫 [MULTI-PANEL REMOVED] Guest ${guestId} removed from multi-panel layout`);
    }
  });

  // --- CO-HOST / GUEST APPROVAL & MANAGEMENT HANDLERS ---

  socket.on('approve_cohost', ({ streamId, guestId, mode }) => {
    console.log(`✅ [COHOST] Host approved guest ${guestId} for stream ${streamId} in ${mode} mode`);
    io.to(streamId).emit('cohost_approved', { streamId, guestId, mode });
    
    const targetGuestSocketId = activeUsers.get(guestId);
    if (targetGuestSocketId) {
      io.to(targetGuestSocketId).emit('cohost_approved', { streamId, guestId, mode });
    }
  });

  socket.on('kick_cohost', ({ streamId, guestId }) => {
    console.log(`🚫 [COHOST] Host kicked guest ${guestId} from stream ${streamId}`);
    io.to(streamId).emit('cohost_kicked', { streamId, guestId });
    
    const targetGuestSocketId = activeUsers.get(guestId);
    if (targetGuestSocketId) {
      io.to(targetGuestSocketId).emit('cohost_kicked', { streamId, guestId });
    }
  });

  // --- GENERAL STREAMING & SIGNALING EVENTS ---

  socket.on('send_cohost_invite', (data) => {
    const targetSocketId = activeUsers.get(data.targetUserId);
    if (targetSocketId) {
      console.log(`✉️ Cross-Room Signal: Routing invitation from Room [${data.room}] directly to Target Socket ID [${targetSocketId}]`);
      io.to(targetSocketId).emit('cohost_invite_received', {
        room: data.room,
        fromHostId: data.fromHostId,
        inviteFrom: data.inviteFrom
      });
    }
  });

  socket.on('respond_cohost_invite', (data) => {
    const originHostSocketId = activeUsers.get(data.targetUserId);
    if (originHostSocketId) {
      console.log(`📥 Routing invite response status [${data.status}] back to origin room host socket: ${originHostHostSocketId || originHostSocketId}`);
      io.to(originHostSocketId).emit('cohost_invite_accepted', {
        room: data.room,
        status: data.status
      });
    }
  });

  socket.on('send_reaction', (data) => {
    if (room) socket.to(room).emit('received_reaction', data);
  });

  socket.on('request_host_stream', ({ streamId }) => {
    console.log(`📡 Forwarding explicit stream request from viewer (${socket.id}) to room channel [${streamId}]`);
    socket.to(streamId).emit('viewer_requesting_stream', { viewerSocketId: socket.id });
  });

  // --- REINFORCED WEBRTC SIGNALING HANDLERS WITH ROOM FALLBACKS ---

  socket.on('send_webrtc_offer', (data) => {
    const { streamId, roomId, offer, targetViewerId, to, guestId, mode } = data;
    const activeRoom = roomId || streamId;
    const targetId = targetViewerId || to;
    const targetSocketId = targetId ? activeUsers.get(targetId) : null;

    console.log(`📤 WebRTC Offer from ${socket.id} -> Target: ${targetId || activeRoom}`);

    const offerPayload = {
      offer,
      guestId: guestId || socket.userId || socket.id,
      mode: mode || 'video',
      hostSocketId: socket.id,
      senderSocketId: socket.id
    };

    if (targetSocketId) {
      io.to(targetSocketId).emit('send_webrtc_offer', offerPayload);
      io.to(targetSocketId).emit('webrtc_offer_received', offerPayload);
    } else if (activeRoom) {
      socket.to(activeRoom).emit('send_webrtc_offer', offerPayload);
      socket.to(activeRoom).emit('webrtc_offer_received', offerPayload);
    }
  });

  socket.on('send_webrtc_answer', (data) => {
    const { streamId, roomId, answer, to } = data;
    const activeRoom = roomId || streamId;
    const targetSocketId = to ? activeUsers.get(to) : null;

    console.log(`📥 Answer from ${socket.id} -> Target ID: ${to} | Room: ${activeRoom}`);

    if (targetSocketId) {
      io.to(targetSocketId).emit('webrtc_answer_received', { answer, viewerSocketId: socket.id, senderSocketId: socket.id });
    } else if (activeRoom) {
      socket.to(activeRoom).emit('webrtc_answer_received', { answer, viewerSocketId: socket.id, senderSocketId: socket.id });
    }
  });

  socket.on('webrtc_ice_candidate', (data) => {
    const { streamId, roomId, candidate, targetSocketId, to, senderType } = data;
    const activeRoom = roomId || streamId;
    const destinationUser = to || targetSocketId;
    const targetSocket = destinationUser ? activeUsers.get(destinationUser) : null;

    if (targetSocket) {
      io.to(targetSocket).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    } else if (activeRoom) {
      socket.to(activeRoom).emit('incoming_ice_candidate', { candidate, senderType, senderSocketId: socket.id });
    }
  });

  // Legacy event aliases mapping directly to standard handlers
  socket.on('webrtc_offer', (data) => socket.emit('send_webrtc_offer', data));
  socket.on('webrtc_answer', (data) => socket.emit('send_webrtc_answer', data));
  socket.on('send_ice_candidate', (data) => socket.emit('webrtc_ice_candidate', data));

  // --- CHAT & USER PRESENCE ---

  socket.on('user_going_online', (userId) => {
    socket.userId = userId;
    activeUsers.set(userId, socket.id);
    io.emit('friend_presence_changed', { userId, status: 'online' });
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

  socket.on('disconnect', () => {
    console.log(`❌ Disconnected: Socket ${socket.id}`);
    if (room) {
      socket.leave(room);
      if (role === 'viewer' || role === 'signal-viewer') {
        broadcastRoomPresence(room);
      }
    }

    if (socket.data?.isGuestPanel && socket.data?.streamId) {
      const roomState = streamRooms.get(socket.data.streamId);
      if (roomState) {
        roomState.guestPanels.delete(socket.data.guestId);
        if (roomState.hostSocketId) {
          io.to(roomState.hostSocketId).emit('guest_panel_disconnected', { guestId: socket.data.guestId });
        }
      }
    }

    if (socket.hostIdentifier) {
      activeUsers.delete(socket.hostIdentifier);
      streamRooms.delete(socket.hostIdentifier);
    }
    if (socket.userId) {
      activeUsers.delete(socket.userId);
      io.emit('friend_presence_changed', { userId: socket.userId, status: 'offline' });
    }
  });
});

const PORT = process.env.PORT || 4000;
http.listen(PORT, () => {
  console.log(`🚀 Socket signaling machine operational on port ${PORT}`);
});
