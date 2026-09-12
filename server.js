const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const os = require('os');

const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand
} = require('@aws-sdk/client-s3');

const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const ffmpeg = require('fluent-ffmpeg');

const app = express();
const http = require('http').createServer(app);

const ALLOWED_ORIGINS = [
  'https://progress-lake.vercel.app',
  'http://localhost:5173'
];

const ALLOWED_STORAGE_FOLDERS = new Set([
  'videos',
  'avatars',
  'covers',
  'sounds',
  'attachments',
  'models'
]);

const MAX_UPLOAD_SIZE = 1024 * 1024 * 1024;

app.use(cors({
  origin: ALLOWED_ORIGINS,
  methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
  credentials: true
}));

app.use(express.json({ limit: '2mb' }));

const systemFfmpegPath = '/usr/bin/ffmpeg';

if (fs.existsSync(systemFfmpegPath)) {
  ffmpeg.setFfmpegPath(systemFfmpegPath);
  console.log(`🚀 FFmpeg path successfully mapped to system production binary: ${systemFfmpegPath}`);
} else {
  console.log('ℹ️ Local environment detected or custom package path applied.');
}

const io = require('socket.io')(http, {
  cors: {
    origin: ALLOWED_ORIGINS,
    methods: ['GET', 'POST'],
    credentials: true
  }
});

/* =========================================================
   BACKBLAZE B2 / S3 CONFIGURATION
========================================================= */

const b2Configured = Boolean(
  process.env.B2_ENDPOINT &&
  process.env.B2_BUCKET &&
  process.env.B2_KEY_ID &&
  process.env.B2_APPLICATION_KEY
);

let b2 = null;

if (b2Configured) {
  b2 = new S3Client({
    endpoint: process.env.B2_ENDPOINT,
    region: process.env.B2_REGION || 'us-east-005',
    credentials: {
      accessKeyId: process.env.B2_KEY_ID,
      secretAccessKey: process.env.B2_APPLICATION_KEY
    },
    forcePathStyle: true
  });

  console.log('☁️ Backblaze B2 storage client initialized.');
} else {
  console.warn(
    '⚠️ Backblaze B2 environment variables are incomplete. B2 storage routes will remain unavailable.'
  );
}

/* =========================================================
   SUPABASE AUTHENTICATION
========================================================= */

const getBearerToken = (req) => {
  const authorization = req.headers.authorization || '';

  if (!authorization.toLowerCase().startsWith('bearer ')) {
    return null;
  }

  return authorization.slice(7).trim() || null;
};

const authenticateSupabaseUser = async (req, res, next) => {
  try {
    const token = getBearerToken(req);

    if (!token) {
      return res.status(401).json({
        error: 'Authentication required.',
        code: 'AUTH_REQUIRED'
      });
    }

    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
      console.error(
        '❌ SUPABASE_URL or SUPABASE_ANON_KEY is missing on backend.'
      );

      return res.status(503).json({
        error: 'Storage authentication is not configured on the server.',
        code: 'AUTH_CONFIG_MISSING'
      });
    }

    const supabaseUrl = process.env.SUPABASE_URL.replace(/\/+$/, '');

    const response = await fetch(`${supabaseUrl}/auth/v1/user`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
        apikey: process.env.SUPABASE_ANON_KEY
      }
    });

    if (!response.ok) {
      return res.status(401).json({
        error: 'Invalid or expired authentication session.',
        code: 'INVALID_SESSION'
      });
    }

    const user = await response.json();

    if (!user?.id) {
      return res.status(401).json({
        error: 'Unable to identify authenticated user.',
        code: 'USER_NOT_FOUND'
      });
    }

    req.authUser = user;

    next();
  } catch (error) {
    console.error(
      '❌ Supabase authentication error:',
      error.message
    );

    return res.status(500).json({
      error: 'Authentication service temporarily unavailable.',
      code: 'AUTH_SERVICE_ERROR'
    });
  }
};

const requireB2 = (req, res, next) => {
  if (!b2Configured || !b2) {
    return res.status(503).json({
      error: 'Backblaze B2 storage is not configured.',
      code: 'B2_NOT_CONFIGURED'
    });
  }

  next();
};

/* =========================================================
   B2 HELPERS
========================================================= */

const sanitizeFileName = (fileName) => {
  const original = String(fileName || 'file')
    .split(/[\\/]/)
    .pop()
    .trim();

  const sanitized = original
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/_+/g, '_')
    .slice(0, 180);

  return sanitized || 'file';
};

const getExtension = (fileName) => {
  const ext = path.extname(String(fileName || '')).toLowerCase();

  if (!ext || ext.length > 12) {
    return '';
  }

  return ext;
};

const isValidContentType = (contentType) => {
  if (!contentType || typeof contentType !== 'string') {
    return false;
  }

  return /^[a-zA-Z0-9!#$&^*.+-]+\/[a-zA-Z0-9!#$&^*.+-]+$/.test(
    contentType
  );
};

const createStorageObjectKey = (
  userId,
  folder,
  fileName
) => {
  const safeFolder = String(folder).toLowerCase();
  const safeName = sanitizeFileName(fileName);
  const extension = getExtension(safeName);
  const randomId = crypto.randomUUID();

  return `${safeFolder}/${userId}/${Date.now()}-${randomId}${extension}`;
};

const parseStorageObjectKey = (objectKey) => {
  const normalizedKey = String(objectKey || '')
    .replace(/^\/+/, '')
    .trim();

  const parts = normalizedKey.split('/');

  if (parts.length < 3) {
    return null;
  }

  const [folder, ownerId] = parts;

  if (!ALLOWED_STORAGE_FOLDERS.has(folder)) {
    return null;
  }

  if (!ownerId) {
    return null;
  }

  return {
    normalizedKey,
    folder,
    ownerId
  };
};

/* =========================================================
   HEALTH CHECK
========================================================= */

app.get('/', (req, res) => {
  res.json({
    status: 'online',
    service: 'Mpade Socket Core Signaling Machine',
    storage: b2Configured
      ? 'backblaze-b2-ready'
      : 'backblaze-b2-not-configured',
    timestamp: new Date().toISOString()
  });
});

/* =========================================================
   B2 HEALTH CHECK
========================================================= */

app.get('/api/storage/status', (req, res) => {
  res.json({
    configured: b2Configured,
    provider: 'Backblaze B2',
    bucket: b2Configured ? process.env.B2_BUCKET : null,
    region: b2Configured
      ? process.env.B2_REGION || 'us-east-005'
      : null
  });
});

/* =========================================================
   B2 SIGNED UPLOAD URL

   React requests a temporary signed URL.
   Browser uploads directly to B2.
   B2 credentials NEVER reach React/Vercel.
========================================================= */

app.post(
  '/api/storage/upload-url',
  authenticateSupabaseUser,
  requireB2,
  async (req, res) => {
    try {
      const {
        fileName,
        contentType,
        folder = 'videos',
        fileSize
      } = req.body || {};

      if (!fileName) {
        return res.status(400).json({
          error: 'Missing fileName.'
        });
      }

      if (!isValidContentType(contentType)) {
        return res.status(400).json({
          error: 'Invalid contentType.'
        });
      }

      const normalizedFolder = String(folder).toLowerCase();

      if (!ALLOWED_STORAGE_FOLDERS.has(normalizedFolder)) {
        return res.status(400).json({
          error: 'Invalid storage folder.',
          allowedFolders: Array.from(ALLOWED_STORAGE_FOLDERS)
        });
      }

      if (
        fileSize !== undefined &&
        fileSize !== null
      ) {
        const numericSize = Number(fileSize);

        if (
          !Number.isFinite(numericSize) ||
          numericSize <= 0 ||
          numericSize > MAX_UPLOAD_SIZE
        ) {
          return res.status(400).json({
            error: 'Invalid file size.',
            maxBytes: MAX_UPLOAD_SIZE
          });
        }
      }

      const userId = req.authUser.id;

      const objectKey = createStorageObjectKey(
        userId,
        normalizedFolder,
        fileName
      );

      const command = new PutObjectCommand({
        Bucket: process.env.B2_BUCKET,
        Key: objectKey,
        ContentType: contentType,
        Metadata: {
          uploadedby: userId,
          originalname: sanitizeFileName(fileName)
        }
      });

      const uploadUrl = await getSignedUrl(
        b2,
        command,
        {
          expiresIn: 900
        }
      );

      return res.json({
        success: true,
        uploadUrl,
        objectKey,
        expiresIn: 900,
        bucket: process.env.B2_BUCKET,
        contentType,
        folder: normalizedFolder
      });
    } catch (error) {
      console.error(
        '❌ B2 upload URL error:',
        error
      );

      return res.status(500).json({
        error: 'Unable to create B2 upload URL.',
        details: error.message
      });
    }
  }
);

/* =========================================================
   B2 SIGNED DOWNLOAD URL

   Currently restricted to the authenticated owner.
========================================================= */

app.post(
  '/api/storage/download-url',
  authenticateSupabaseUser,
  requireB2,
  async (req, res) => {
    try {
      const { objectKey } = req.body || {};

      if (
        !objectKey ||
        typeof objectKey !== 'string'
      ) {
        return res.status(400).json({
          error: 'Missing objectKey.'
        });
      }

      const parsed = parseStorageObjectKey(
        objectKey
      );

      if (!parsed) {
        return res.status(400).json({
          error: 'Invalid object key.'
        });
      }

      const userId = req.authUser.id;

      if (parsed.ownerId !== userId) {
        return res.status(403).json({
          error:
            'You do not have permission to access this object.',
          code: 'OBJECT_ACCESS_DENIED'
        });
      }

      const command = new GetObjectCommand({
        Bucket: process.env.B2_BUCKET,
        Key: parsed.normalizedKey
      });

      const downloadUrl = await getSignedUrl(
        b2,
        command,
        {
          expiresIn: 900
        }
      );

      return res.json({
        success: true,
        downloadUrl,
        objectKey: parsed.normalizedKey,
        expiresIn: 900
      });
    } catch (error) {
      console.error(
        '❌ B2 download URL error:',
        error
      );

      return res.status(500).json({
        error: 'Unable to create B2 download URL.',
        details: error.message
      });
    }
  }
);

/* =========================================================
   B2 DELETE OBJECT
========================================================= */

app.delete(
  '/api/storage/object',
  authenticateSupabaseUser,
  requireB2,
  async (req, res) => {
    try {
      const { objectKey } = req.body || {};

      if (
        !objectKey ||
        typeof objectKey !== 'string'
      ) {
        return res.status(400).json({
          error: 'Missing objectKey.'
        });
      }

      const parsed = parseStorageObjectKey(
        objectKey
      );

      if (!parsed) {
        return res.status(400).json({
          error: 'Invalid object key.'
        });
      }

      if (parsed.ownerId !== req.authUser.id) {
        return res.status(403).json({
          error:
            'You do not have permission to delete this object.',
          code: 'OBJECT_DELETE_DENIED'
        });
      }

      await b2.send(
        new DeleteObjectCommand({
          Bucket: process.env.B2_BUCKET,
          Key: parsed.normalizedKey
        })
      );

      return res.json({
        success: true,
        deleted: true,
        objectKey: parsed.normalizedKey
      });
    } catch (error) {
      console.error(
        '❌ B2 delete error:',
        error
      );

      return res.status(500).json({
        error: 'Unable to delete B2 object.',
        details: error.message
      });
    }
  }
);

/* =========================================================
   VIDEO / AUDIO MERGE ENDPOINT

   Existing functionality preserved.
========================================================= */

app.post('/api/merge-video', async (req, res) => {
  const {
    videoUrl,
    audioUrl
  } = req.body || {};

  if (!videoUrl) {
    return res.status(400).json({
      error: 'Missing source videoUrl field.'
    });
  }

  const outputFilename =
    `merged_${Date.now()}_${Math.floor(Math.random() * 1000)}.mp4`;

  const outputPath = path.join(
    os.tmpdir(),
    outputFilename
  );

  let ffmpegCommand;

  const cleanupOutput = () => {
    try {
      if (fs.existsSync(outputPath)) {
        fs.unlinkSync(outputPath);
        console.log(
          '🗑️ Cleaned up temporary FFmpeg file.'
        );
      }
    } catch (cleanupError) {
      console.error(
        '❌ Temporary FFmpeg cleanup failed:',
        cleanupError.message
      );
    }
  };

  try {
    ffmpegCommand = ffmpeg()
      .input(videoUrl)
      .inputOptions([
        '-protocol_whitelist',
        'file,http,https,tcp,tls,crypto',
        '-fflags',
        '+genpts'
      ]);

    let hasCustomAudio = false;

    if (audioUrl) {
      const cleanAudioStr = String(audioUrl)
        .trim()
        .toLowerCase();

      if (
        cleanAudioStr !== '' &&
        cleanAudioStr !== 'null' &&
        cleanAudioStr !== 'undefined'
      ) {
        hasCustomAudio = true;
      }
    }

    if (hasCustomAudio) {
      console.log(
        '🎵 Custom embedded audio track detected. Multiplexing audio stream layers...'
      );

      ffmpegCommand
        .input(audioUrl)
        .inputOptions([
          '-protocol_whitelist',
          'file,http,https,tcp,tls,crypto',
          '-user_agent',
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36'
        ])
        .outputOptions([
          '-c:v',
          'copy',
          '-c:a',
          'aac',
          '-b:a',
          '128k',
          '-map',
          '0:v:0',
          '-map',
          '1:a:0',
          '-map_metadata',
          '-1',
          '-movflags',
          '+faststart',
          '-shortest'
        ]);
    } else {
      console.log(
        '🗣️ Original native sound verified. Copying source media tracks directly...'
      );

      ffmpegCommand.outputOptions([
        '-c:v',
        'copy',
        '-c:a',
        'copy',
        '-movflags',
        '+faststart'
      ]);
    }

    ffmpegCommand
      .toFormat('mp4')
      .on('start', () => {
        console.log(
          '🎬 Started Video Pipeline Processing to Disk...'
        );
      })
      .on('error', (err) => {
        console.error(
          '❌ Server-Side Video Processing Pipeline Error:',
          err.message
        );

        cleanupOutput();

        if (!res.headersSent) {
          res.status(500).json({
            error:
              `Video generation pipeline encountered an issue: ${err.message}`
          });
        }
      })
      .on('end', () => {
        console.log(
          '✅ Video successfully generated on disk. Initializing download pipeline transfer...'
        );

        res.download(
          outputPath,
          'Mpade_Export.mp4',
          (downloadErr) => {
            if (downloadErr) {
              console.error(
                '❌ Error during transmission file transfer:',
                downloadErr
              );
            }

            cleanupOutput();
          }
        );
      })
      .save(outputPath);
  } catch (error) {
    console.error(
      '❌ Failed to initialize FFmpeg pipeline:',
      error
    );

    cleanupOutput();

    if (!res.headersSent) {
      return res.status(500).json({
        error:
          'Unable to initialize video processing pipeline.'
      });
    }
  }
});

/* =========================================================
   GLOBAL USER / STREAM STATE
========================================================= */

const activeUsers = new Map();
const streamRooms = new Map();

/* =========================================================
   SOCKET.IO
========================================================= */

io.on('connection', (socket) => {
  const {
    room,
    role,
    streamId
  } = socket.handshake.query;

  if (room) {
    socket.join(room);

    console.log(
      `🔌 Connection: Socket ${socket.id} joined room [${room}] as (${role})`
    );

    if (
      role === 'cohost_master' ||
      role === 'host'
    ) {
      const hostIdentifier =
        streamId || room;

      activeUsers.set(
        hostIdentifier,
        socket.id
      );

      socket.hostIdentifier =
        hostIdentifier;

      if (!streamRooms.has(hostIdentifier)) {
        streamRooms.set(
          hostIdentifier,
          {
            hostSocketId: socket.id,
            guestPanels: new Map()
          }
        );
      } else {
        streamRooms.get(
          hostIdentifier
        ).hostSocketId = socket.id;
      }

      console.log(
        `📡 Registered Host globally in active reference index: [${hostIdentifier}] -> Socket ${socket.id}`
      );
    }
  } else {
    console.log(
      `🔌 New client handshaking without room parameter (General Session): ${socket.id}`
    );
  }

  const broadcastRoomPresence = async (
    roomName
  ) => {
    try {
      const sockets =
        await io
          .in(roomName)
          .fetchSockets();

      const viewersList = sockets
        .filter(
          (s) =>
            s.handshake.query.role === 'viewer' ||
            s.handshake.query.role === 'signal-viewer'
        )
        .map((s) => ({
          socketId: s.id,
          username:
            s.handshake.query.username ||
            'Anonymous'
        }));

      io.to(roomName).emit(
        'room_presence_update',
        viewersList
      );
    } catch (err) {
      console.error(
        '❌ Presence tracking error:',
        err
      );
    }
  };

  if (
    room &&
    (
      role === 'viewer' ||
      role === 'signal-viewer'
    )
  ) {
    socket.to(room).emit(
      'viewer_joined',
      {
        id: socket.id,
        username:
          socket.handshake.query.username
      }
    );

    broadcastRoomPresence(room);
  }

  /* =======================================================
     USER SESSION
  ======================================================= */

  socket.on(
    'register_user_session',
    ({ userId } = {}) => {
      if (!userId) return;

      socket.userId = userId;

      activeUsers.set(
        userId,
        socket.id
      );

      io.emit(
        'friend_presence_changed',
        {
          userId,
          status: 'online'
        }
      );

      console.log(
        `🟢 User ${userId} bound to notification session map: ${socket.id}`
      );
    }
  );

  /* =======================================================
     1-ON-1 DIRECT CALLS
  ======================================================= */

  socket.on(
    'initiate_call_signal',
    (callPayload = {}) => {
      const targetSocketId =
        activeUsers.get(
          callPayload.receiverId
        );

      if (targetSocketId) {
        console.log(
          `📞 Routing direct incoming ${callPayload.callType} call signal to target client: ${targetSocketId}`
        );

        io.to(targetSocketId).emit(
          'incoming_call_signal',
          callPayload
        );
      }
    }
  );

  socket.on(
    'decline_call',
    ({ callerId } = {}) => {
      const originCallerSocketId =
        activeUsers.get(callerId);

      if (originCallerSocketId) {
        console.log(
          `🚫 Call declined by receiver. Notifying origin socket: ${originCallerSocketId}`
        );

        io.to(
          originCallerSocketId
        ).emit(
          'call_cancelled_by_caller'
        );
      }
    }
  );

  socket.on(
    'join_call_room',
    ({
      roomId,
      userId,
      targetPeerId
    } = {}) => {
      if (!roomId) return;

      socket.join(roomId);

      console.log(
        `📞 Socket ${socket.id} joined dedicated P2P WebRTC call room: ${roomId}`
      );

      if (userId) {
        socket.userId = userId;

        activeUsers.set(
          userId,
          socket.id
        );
      }

      let peerUserId =
        targetPeerId;

      if (!peerUserId) {
        const userIds =
          roomId.split('-');

        peerUserId =
          userIds.find(
            (id) => id !== userId
          );
      }

      if (peerUserId) {
        const targetSocketId =
          activeUsers.get(
            peerUserId
          );

        if (targetSocketId) {
          console.log(
            `🔔 Forwarding real-time incoming call alert from ${userId} to target socket ${targetSocketId}`
          );

          io.to(
            targetSocketId
          ).emit(
            'incoming_call_signal',
            {
              callerId: userId,
              roomId
            }
          );
        }
      }
    }
  );

  socket.on(
    'peer_ready',
    ({
      roomId,
      userId
    } = {}) => {
      if (!roomId) return;

      console.log(
        `⚡ Receiver/Peer (${userId || socket.id}) is mounted and ready in room: ${roomId}`
      );

      socket.to(roomId).emit(
        'peer_ready',
        {
          userId,
          socketId: socket.id
        }
      );
    }
  );

  socket.on(
    'reject_incoming_call',
    ({
      roomId,
      to
    } = {}) => {
      const targetSocketId =
        activeUsers.get(to);

      if (targetSocketId) {
        console.log(
          `🚫 Call rejected by peer. Notifying origin socket: ${targetSocketId}`
        );

        io.to(
          targetSocketId
        ).emit(
          'peer_hung_up'
        );
      } else if (roomId) {
        socket.to(roomId).emit(
          'peer_hung_up'
        );
      }
    }
  );

  /* =======================================================
     MULTI-PANEL LIVE STREAM INGEST
  ======================================================= */

  socket.on(
    'publish_guest_feed',
    ({
      streamId,
      guestId,
      targetHostId,
      sdpOffer,
      mode
    } = {}) => {
      if (!streamId || !guestId) {
        return;
      }

      const targetHostSocketId =
        activeUsers.get(targetHostId) ||
        streamRooms.get(
          streamId
        )?.hostSocketId;

      if (!streamRooms.has(streamId)) {
        streamRooms.set(
          streamId,
          {
            hostSocketId:
              targetHostSocketId,
            guestPanels:
              new Map()
          }
        );
      }

      const roomState =
        streamRooms.get(streamId);

      roomState.guestPanels.set(
        guestId,
        socket.id
      );

      socket.data = {
        ...socket.data,
        isGuestPanel: true,
        guestId,
        streamId
      };

      console.log(
        `🎥 [MULTI-PANEL INGEST] Guest ${guestId} sending ${mode} stream feed to Host ${targetHostId}`
      );

      if (targetHostSocketId) {
        io.to(
          targetHostSocketId
        ).emit(
          'incoming_guest_panel_feed',
          {
            guestId,
            guestSocketId:
              socket.id,
            sdpOffer,
            mode
          }
        );
      } else {
        socket.to(streamId).emit(
          'incoming_guest_panel_feed',
          {
            guestId,
            guestSocketId:
              socket.id,
            sdpOffer,
            mode
          }
        );
      }
    }
  );

  socket.on(
    'host_ack_guest_feed',
    ({
      guestSocketId,
      sdpAnswer,
      guestId
    } = {}) => {
      if (!guestSocketId) {
        return;
      }

      console.log(
        `✅ [MULTI-PANEL ACK] Host accepted panel stream from guest ${guestId}`
      );

      io.to(
        guestSocketId
      ).emit(
        'broadcast_ack_received',
        {
          sdpAnswer
        }
      );
    }
  );

  socket.on(
    'guest_ice_candidate',
    ({
      streamId,
      candidate,
      to
    } = {}) => {
      const targetHostSocketId =
        activeUsers.get(to) ||
        streamRooms.get(
          streamId
        )?.hostSocketId;

      if (targetHostSocketId) {
        io.to(
          targetHostSocketId
        ).emit(
          'incoming_guest_ice',
          {
            candidate,
            fromGuestSocketId:
              socket.id
          }
        );
      } else if (streamId) {
        socket.to(streamId).emit(
          'incoming_guest_ice',
          {
            candidate,
            fromGuestSocketId:
              socket.id
          }
        );
      }
    }
  );

  socket.on(
    'host_ice_candidate',
    ({
      targetGuestSocketId,
      candidate
    } = {}) => {
      if (targetGuestSocketId) {
        io.to(
          targetGuestSocketId
        ).emit(
          'incoming_host_ice',
          {
            candidate
          }
        );
      }
    }
  );

  socket.on(
    'remove_guest_panel',
    ({
      streamId,
      guestId
    } = {}) => {
      const roomState =
        streamRooms.get(
          streamId
        );

      if (
        roomState &&
        roomState.guestPanels.has(
          guestId
        )
      ) {
        const guestSocketId =
          roomState.guestPanels.get(
            guestId
          );

        io.to(
          guestSocketId
        ).emit(
          'removed_from_panel'
        );

        roomState.guestPanels.delete(
          guestId
        );

        console.log(
          `🚫 [MULTI-PANEL REMOVED] Guest ${guestId} removed from multi-panel layout`
        );
      }
    }
  );

  /* =======================================================
     COHOST MANAGEMENT
  ======================================================= */

  socket.on(
    'approve_cohost',
    ({
      streamId,
      guestId,
      mode
    } = {}) => {
      if (!streamId || !guestId) {
        return;
      }

      console.log(
        `✅ [COHOST] Host approved guest ${guestId} for stream ${streamId} in ${mode} mode`
      );

      io.to(streamId).emit(
        'cohost_approved',
        {
          streamId,
          guestId,
          mode
        }
      );

      const targetGuestSocketId =
        activeUsers.get(
          guestId
        );

      if (targetGuestSocketId) {
        io.to(
          targetGuestSocketId
        ).emit(
          'cohost_approved',
          {
            streamId,
            guestId,
            mode
          }
        );
      }
    }
  );

  socket.on(
    'kick_cohost',
    ({
      streamId,
      guestId
    } = {}) => {
      if (!streamId || !guestId) {
        return;
      }

      console.log(
        `🚫 [COHOST] Host kicked guest ${guestId} from stream ${streamId}`
      );

      io.to(streamId).emit(
        'cohost_kicked',
        {
          streamId,
          guestId
        }
      );

      const targetGuestSocketId =
        activeUsers.get(
          guestId
        );

      if (targetGuestSocketId) {
        io.to(
          targetGuestSocketId
        ).emit(
          'cohost_kicked',
          {
            streamId,
            guestId
          }
        );
      }
    }
  );

  socket.on(
    'send_cohost_invite',
    (data = {}) => {
      const targetSocketId =
        activeUsers.get(
          data.targetUserId
        );

      if (targetSocketId) {
        console.log(
          `✉️ Cross-Room Signal: Routing invitation from Room [${data.room}] directly to Target Socket ID [${targetSocketId}]`
        );

        io.to(
          targetSocketId
        ).emit(
          'cohost_invite_received',
          {
            room: data.room,
            fromHostId:
              data.fromHostId,
            inviteFrom:
              data.inviteFrom
          }
        );
      }
    }
  );

  socket.on(
    'respond_cohost_invite',
    (data = {}) => {
      const originHostSocketId =
        activeUsers.get(
          data.targetUserId
        );

      if (originHostSocketId) {
        console.log(
          `📥 Routing invite response status [${data.status}] back to origin room host socket: ${originHostSocketId}`
        );

        io.to(
          originHostSocketId
        ).emit(
          'cohost_invite_accepted',
          {
            room: data.room,
            status: data.status
          }
        );
      }
    }
  );

  /* =======================================================
     REACTIONS
  ======================================================= */

  socket.on(
    'send_reaction',
    (data = {}) => {
      if (room) {
        socket.to(room).emit(
          'received_reaction',
          data
        );
      }
    }
  );

  socket.on(
    'request_host_stream',
    ({ streamId } = {}) => {
      if (!streamId) return;

      console.log(
        `📡 Forwarding explicit stream request from viewer (${socket.id}) to room channel [${streamId}]`
      );

      socket.to(streamId).emit(
        'viewer_requesting_stream',
        {
          viewerSocketId:
            socket.id
        }
      );
    }
  );

  /* =======================================================
     WEBRTC SIGNALING
  ======================================================= */

  socket.on(
    'send_webrtc_offer',
    (data = {}) => {
      const {
        streamId,
        roomId,
        offer,
        targetViewerId,
        to,
        guestId,
        mode
      } = data;

      const activeRoom =
        roomId || streamId;

      const targetId =
        targetViewerId || to;

      const targetSocketId =
        targetId
          ? activeUsers.get(
              targetId
            )
          : null;

      console.log(
        `📤 WebRTC Offer from ${socket.id} -> Target: ${targetId || activeRoom}`
      );

      const offerPayload = {
        offer,
        guestId:
          guestId ||
          socket.userId ||
          socket.id,
        mode:
          mode || 'video',
        hostSocketId:
          socket.id,
        senderSocketId:
          socket.id
      };

      if (targetSocketId) {
        io.to(
          targetSocketId
        ).emit(
          'send_webrtc_offer',
          offerPayload
        );

        io.to(
          targetSocketId
        ).emit(
          'webrtc_offer_received',
          offerPayload
        );
      } else if (activeRoom) {
        socket.to(activeRoom).emit(
          'send_webrtc_offer',
          offerPayload
        );

        socket.to(activeRoom).emit(
          'webrtc_offer_received',
          offerPayload
        );
      }
    }
  );

  socket.on(
    'send_webrtc_answer',
    (data = {}) => {
      const {
        streamId,
        roomId,
        answer,
        to
      } = data;

      const activeRoom =
        roomId || streamId;

      const targetSocketId =
        to
          ? activeUsers.get(to)
          : null;

      console.log(
        `📥 Answer from ${socket.id} -> Target ID: ${to || 'room'} | Room: ${activeRoom || 'none'}`
      );

      if (targetSocketId) {
        io.to(
          targetSocketId
        ).emit(
          'webrtc_answer_received',
          {
            answer,
            viewerSocketId:
              socket.id,
            senderSocketId:
              socket.id
          }
        );
      } else if (activeRoom) {
        socket.to(activeRoom).emit(
          'webrtc_answer_received',
          {
            answer,
            viewerSocketId:
              socket.id,
            senderSocketId:
              socket.id
          }
        );
      }
    }
  );

  socket.on(
    'webrtc_ice_candidate',
    (data = {}) => {
      const {
        streamId,
        roomId,
        candidate,
        targetSocketId,
        to,
        senderType
      } = data;

      const activeRoom =
        roomId || streamId;

      const destinationUser =
        to || targetSocketId;

      const targetSocket =
        destinationUser
          ? activeUsers.get(
              destinationUser
            )
          : null;

      if (targetSocket) {
        io.to(
          targetSocket
        ).emit(
          'incoming_ice_candidate',
          {
            candidate,
            senderType,
            senderSocketId:
              socket.id
          }
        );
      } else if (activeRoom) {
        socket.to(activeRoom).emit(
          'incoming_ice_candidate',
          {
            candidate,
            senderType,
            senderSocketId:
              socket.id
          }
        );
      }
    }
  );

  /* =======================================================
     LEGACY WEBRTC ALIASES
  ======================================================= */

  socket.on(
    'webrtc_offer',
    (data = {}) => {
      socket.emit(
        'send_webrtc_offer',
        data
      );
    }
  );

  socket.on(
    'webrtc_answer',
    (data = {}) => {
      socket.emit(
        'send_webrtc_answer',
        data
      );
    }
  );

  socket.on(
    'send_ice_candidate',
    (data = {}) => {
      socket.emit(
        'webrtc_ice_candidate',
        data
      );
    }
  );

  /* =======================================================
     CHAT / PRESENCE
  ======================================================= */

  socket.on(
    'user_going_online',
    (userId) => {
      if (!userId) return;

      socket.userId = userId;

      activeUsers.set(
        userId,
        socket.id
      );

      io.emit(
        'friend_presence_changed',
        {
          userId,
          status: 'online'
        }
      );
    }
  );

  socket.on(
    'send_chat_message',
    (messagePayload = {}) => {
      const targetSocketId =
        activeUsers.get(
          messagePayload.receiver_id
        );

      if (targetSocketId) {
        io.to(
          targetSocketId
        ).emit(
          'received_chat_message',
          messagePayload
        );
      }
    }
  );

  socket.on(
    'broadcast_message_update',
    (updatedPayload = {}) => {
      const targetSocketId =
        activeUsers.get(
          updatedPayload.receiver_id
        );

      if (targetSocketId) {
        io.to(
          targetSocketId
        ).emit(
          'message_updated_realtime',
          updatedPayload
        );
      }
    }
  );

  socket.on(
    'user_typing_state',
    ({
      userId,
      isTyping,
      mode
    } = {}) => {
      socket.broadcast.emit(
        'peer_typing_state_changed',
        {
          userId,
          isTyping,
          mode
        }
      );
    }
  );

  /* =======================================================
     DISCONNECT CLEANUP
  ======================================================= */

  socket.on(
    'disconnect',
    () => {
      console.log(
        `❌ Disconnected: Socket ${socket.id}`
      );

      if (room) {
        socket.leave(room);

        if (
          role === 'viewer' ||
          role === 'signal-viewer'
        ) {
          broadcastRoomPresence(room);
        }
      }

      if (
        socket.data?.isGuestPanel &&
        socket.data?.streamId
      ) {
        const roomState =
          streamRooms.get(
            socket.data.streamId
          );

        if (roomState) {
          roomState.guestPanels.delete(
            socket.data.guestId
          );

          if (roomState.hostSocketId) {
            io.to(
              roomState.hostSocketId
            ).emit(
              'guest_panel_disconnected',
              {
                guestId:
                  socket.data.guestId
              }
            );
          }
        }
      }

      if (socket.hostIdentifier) {
        const registeredSocket =
          activeUsers.get(
            socket.hostIdentifier
          );

        if (
          registeredSocket === socket.id
        ) {
          activeUsers.delete(
            socket.hostIdentifier
          );
        }

        const roomState =
          streamRooms.get(
            socket.hostIdentifier
          );

        if (
          roomState &&
          roomState.hostSocketId ===
            socket.id
        ) {
          streamRooms.delete(
            socket.hostIdentifier
          );
        }
      }

      if (socket.userId) {
        const registeredSocket =
          activeUsers.get(
            socket.userId
          );

        if (
          registeredSocket ===
          socket.id
        ) {
          activeUsers.delete(
            socket.userId
          );

          io.emit(
            'friend_presence_changed',
            {
              userId:
                socket.userId,
              status: 'offline'
            }
          );
        }
      }
    }
  );
});

/* =========================================================
   SERVER START
========================================================= */

const PORT =
  process.env.PORT || 4000;

http.listen(PORT, () => {
  console.log(
    `🚀 Socket signaling machine operational on port ${PORT}`
  );

  console.log(
    `☁️ B2 storage: ${
      b2Configured
        ? 'READY'
        : 'NOT CONFIGURED'
    }`
  );

  console.log(
    `🌐 Allowed origins: ${ALLOWED_ORIGINS.join(', ')}`
  );
});