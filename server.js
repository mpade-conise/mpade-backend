const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  HeadObjectCommand
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const ffmpeg = require('fluent-ffmpeg');

const app = express();
const http = require('http').createServer(app);

app.disable('x-powered-by');

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
const MAX_VIDEO_UPLOAD_SIZE = 50 * 1024 * 1024;

const corsOptions = {
  origin: ALLOWED_ORIGINS,
  methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true,
  optionsSuccessStatus: 204
};

app.use(cors(corsOptions));
app.options(/.*/, cors(corsOptions));
app.use(express.json({ limit: '2mb' }));

/* =========================================================
   FFMPEG
========================================================= */

const systemFfmpegPath = '/usr/bin/ffmpeg';

if (fs.existsSync(systemFfmpegPath)) {
  ffmpeg.setFfmpegPath(systemFfmpegPath);
  console.log(`🚀 FFmpeg mapped to ${systemFfmpegPath}`);
} else {
  console.log(
    'ℹ️ System FFmpeg not found; using fluent-ffmpeg default configuration.'
  );
}

/* =========================================================
   SOCKET.IO
========================================================= */

const io = require('socket.io')(http, {
  cors: {
    origin: ALLOWED_ORIGINS,
    methods: ['GET', 'POST'],
    credentials: true
  }
});

const { attachLiveSFU } = require('./live-sfu');
attachLiveSFU(io);

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
    forcePathStyle: true,
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED'
  });

  console.log('☁️ Backblaze B2 storage client initialized.');
} else {
  console.warn('⚠️ Backblaze B2 environment variables are incomplete.');
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
      console.error('❌ SUPABASE_URL or SUPABASE_ANON_KEY is missing.');

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

    return next();
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

/* =========================================================
   ACCOUNT LIFECYCLE / SUPABASE ADMIN
========================================================= */

const requireSupabaseAdmin = (req, res, next) => {
  if (!process.env.SUPABASE_SERVICE_ROLE_KEY) {
    return res.status(503).json({
      error: 'Account lifecycle administration is not configured.',
      code: 'SUPABASE_ADMIN_NOT_CONFIGURED'
    });
  }

  return next();
};

const supabaseAdminRequest = async (method, pathname, body) => {
  const supabaseUrl = process.env.SUPABASE_URL?.replace(/\/+$/, '');
  if (!supabaseUrl || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('Supabase admin configuration is missing.');
  }

  const response = await fetch(`${supabaseUrl}/auth/v1${pathname}`, {
    method,
    headers: {
      Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
      apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
      'Content-Type': 'application/json'
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) })
  });

  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { message: text }; }

  if (!response.ok) {
    const error = new Error(data?.msg || data?.message || data?.error_description || 'Supabase admin request failed.');
    error.status = response.status;
    throw error;
  }

  return data;
};

const setAccountLifecycle = async (userId, status) => {
  if (status === 'active') {
    return supabaseAdminRequest('PUT', `/admin/users/${encodeURIComponent(userId)}`, {
      ban_duration: 'none',
      user_metadata: { account_status: 'active' }
    });
  }

  return supabaseAdminRequest('PUT', `/admin/users/${encodeURIComponent(userId)}`, {
    ban_duration: '876000h',
    user_metadata: { account_status: status }
  });
};

app.post('/api/account/deactivate', authenticateSupabaseUser, requireSupabaseAdmin, async (req, res) => {
  try {
    const userId = req.authUser.id;
    await setAccountLifecycle(userId, 'deactivated');

    return res.json({ success: true, status: 'deactivated' });
  } catch (error) {
    console.error('❌ Account deactivation error:', error.message);
    return res.status(error.status || 500).json({
      error: error.message || 'Unable to deactivate account.',
      code: 'ACCOUNT_DEACTIVATION_FAILED'
    });
  }
});

app.post('/api/account/reactivate', authenticateSupabaseUser, requireSupabaseAdmin, async (req, res) => {
  try {
    const userId = req.authUser.id;
    await setAccountLifecycle(userId, 'active');

    if (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY) {
      const supabaseUrl = process.env.SUPABASE_URL.replace(/\/+$/, '');
      await fetch(`${supabaseUrl}/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}`, {
        method: 'PATCH',
        headers: {
          Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
          apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
          'Content-Type': 'application/json',
          Prefer: 'return=minimal'
        },
        body: JSON.stringify({ account_status: 'active' })
      });
    }

    return res.json({ success: true, status: 'active' });
  } catch (error) {
    console.error('❌ Account reactivation error:', error.message);
    return res.status(error.status || 500).json({
      error: error.message || 'Unable to reactivate account.',
      code: 'ACCOUNT_REACTIVATION_FAILED'
    });
  }
});

app.delete('/api/account', authenticateSupabaseUser, requireSupabaseAdmin, async (req, res) => {
  try {
    const userId = req.authUser.id;
    const supabaseUrl = process.env.SUPABASE_URL.replace(/\/+$/, '');
    const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    await supabaseAdminRequest('DELETE', `/admin/users/${encodeURIComponent(userId)}`);

    return res.json({ success: true, deleted: true });
  } catch (error) {
    console.error('❌ Account deletion error:', error.message);
    return res.status(error.status || 500).json({
      error: error.message || 'Unable to delete account.',
      code: 'ACCOUNT_DELETION_FAILED'
    });
  }
});

const requireB2 = (req, res, next) => {
  if (!b2Configured || !b2) {
    return res.status(503).json({
      error: 'Backblaze B2 storage is not configured.',
      code: 'B2_NOT_CONFIGURED'
    });
  }

  return next();
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

  return /^[a-zA-Z0-9!#$&^_.+-]+\/[a-zA-Z0-9!#$&^_.+-]+$/.test(
    contentType
  );
};

const createStorageObjectKey = (userId, folder, fileName) => {
  const safeUserId = String(userId || '').replace(
    /[^a-zA-Z0-9_-]/g,
    ''
  );

  const safeFolder = String(folder || '')
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '');

  const safeName = sanitizeFileName(fileName);
  const extension = getExtension(safeName);
  const randomId = crypto.randomUUID();

  return `${safeFolder}/${safeUserId}/${Date.now()}-${randomId}${extension}`;
};

const parseStorageObjectKey = (objectKey) => {
  const normalizedKey = String(objectKey || '')
    .replace(/^\/+/, '')
    .trim();

  const parts = normalizedKey.split('/');

  if (
    parts.length < 3 ||
    parts.some(
      (part) =>
        !part ||
        part === '.' ||
        part === '..'
    )
  ) {
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
    valid: true,
    normalizedKey,
    objectKey: normalizedKey,
    folder,
    ownerId
  };
};

const getPublicObjectUrl = (objectKey) => {
  const base = String(
    process.env.B2_PUBLIC_URL_BASE ||
    process.env.MEDIA_PUBLIC_URL_BASE ||
    ''
  ).replace(/\/+$/, '');

  if (!base) return null;

  return `${base}/${String(objectKey).split('/').map(encodeURIComponent).join('/')}`;
};

const getFolderMaxUploadSize = folder =>
  folder === 'videos' ? MAX_VIDEO_UPLOAD_SIZE : MAX_UPLOAD_SIZE;

const isHttpUrl = (value) => {
  if (!value || typeof value !== 'string') {
    return false;
  }

  try {
    const parsed = new URL(value);

    return (
      parsed.protocol === 'http:' ||
      parsed.protocol === 'https:'
    );
  } catch {
    return false;
  }
};

/* =========================================================
   TEMP FILE HELPERS
========================================================= */

const safeUnlink = (filePath) => {
  if (!filePath) {
    return;
  }

  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  } catch (error) {
    console.warn(
      `⚠️ Could not remove temporary file ${filePath}: ${error.message}`
    );
  }
};

const cleanupTempFiles = (...filePaths) => {
  for (const filePath of filePaths) {
    safeUnlink(filePath);
  }
};

/* =========================================================
   DOWNLOAD PRIVATE B2 OBJECT
========================================================= */

const downloadB2ObjectToFile = async (
  objectKey,
  outputPath
) => {
  const parsed = parseStorageObjectKey(objectKey);

  if (!parsed) {
    throw new Error('Invalid B2 object key.');
  }

  const command = new GetObjectCommand({
    Bucket: process.env.B2_BUCKET,
    Key: parsed.normalizedKey
  });

  const response = await b2.send(command);

  if (!response.Body) {
    throw new Error('B2 returned an empty object body.');
  }

  if (typeof response.Body.pipe === 'function') {
    await pipeline(
      response.Body,
      fs.createWriteStream(outputPath)
    );

    return;
  }

  if (
    typeof response.Body.transformToByteArray === 'function'
  ) {
    const bytes = await response.Body.transformToByteArray();

    await fs.promises.writeFile(
      outputPath,
      Buffer.from(bytes)
    );

    return;
  }

  throw new Error(
    'Unsupported B2 response body type.'
  );
};

/* =========================================================
   DOWNLOAD REMOTE AUDIO
========================================================= */

const downloadRemoteAudioToFile = async (
  audioUrl,
  outputPath
) => {
  if (!isHttpUrl(audioUrl)) {
    throw new Error('Invalid audio URL.');
  }

  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    60000
  );

  try {
    const response = await fetch(audioUrl, {
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0'
      },
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(
        `Audio download failed with HTTP ${response.status}.`
      );
    }

    if (!response.body) {
      throw new Error(
        'Audio response contained no body.'
      );
    }

    await pipeline(
      Readable.fromWeb(response.body),
      fs.createWriteStream(outputPath)
    );
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(
        'Audio download timed out.'
      );
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
};

/* =========================================================
   FRONTEND FILTER NORMALIZATION
========================================================= */

const normalizeVideoFilter = (filter) => {
  const value =
    typeof filter === 'string'
      ? filter.trim().toLowerCase()
      : 'original';

  const filterMap = {
    original: 'none',
    none: 'none',

    neon_cyber: 'neon_cyber',
    electric: 'electric',
    cinema: 'cinema',
    golden_hour: 'golden_hour',
    vintage: 'vintage',
    midnight: 'midnight',
    vibrant_pop: 'vibrant_pop',

    normal: 'none',
    grayscale: 'grayscale',
    sepia: 'sepia',
    vivid: 'vivid',
    bright: 'bright',
    dark: 'dark',
    warm: 'warm',
    cool: 'cool'
  };

  return filterMap[value] || 'none';
};

/* =========================================================
   DETERMINE WHETHER FFMPEG IS ACTUALLY REQUIRED
========================================================= */

const needsVideoProcessing = ({
  audioUrl,
  videoVolume,
  musicVolume,
  audioEnhancement,
  filter
}) => {
  const hasMusic =
    Boolean(audioUrl) &&
    !['', 'null', 'undefined'].includes(
      String(audioUrl).trim().toLowerCase()
    );

  const normalizedFilter =
    normalizeVideoFilter(filter);

  const normalizedEnhancement =
    typeof audioEnhancement === 'string'
      ? audioEnhancement.trim().toLowerCase()
      : 'none';

  const safeVideoVolume = Number.isFinite(
    Number(videoVolume)
  )
    ? Number(videoVolume)
    : 1;

  const safeMusicVolume = Number.isFinite(
    Number(musicVolume)
  )
    ? Number(musicVolume)
    : 1;

  const filterNeedsProcessing =
    normalizedFilter !== 'none';

  const enhancementNeedsProcessing =
    normalizedEnhancement !== 'none';

  const videoVolumeNeedsProcessing =
    Math.abs(safeVideoVolume - 1) > 0.001;

  const musicVolumeNeedsProcessing =
    hasMusic &&
    Math.abs(safeMusicVolume - 1) > 0.001;

  return (
    hasMusic ||
    filterNeedsProcessing ||
    enhancementNeedsProcessing ||
    videoVolumeNeedsProcessing ||
    musicVolumeNeedsProcessing
  );
};

/* =========================================================
   VERIFY PRIVATE B2 OBJECT
========================================================= */

const verifyB2Object = async (objectKey) => {
  const parsed = parseStorageObjectKey(objectKey);

  if (!parsed) {
    throw new Error('Invalid B2 object key.');
  }

  const response = await b2.send(
    new HeadObjectCommand({
      Bucket: process.env.B2_BUCKET,
      Key: parsed.normalizedKey
    })
  );

  const size = Number(
    response.ContentLength || 0
  );

  if (!Number.isFinite(size) || size <= 0) {
    throw new Error(
      'Uploaded B2 object is empty or invalid.'
    );
  }

  const maxSize = parsed.folder === 'videos'
    ? MAX_VIDEO_UPLOAD_SIZE
    : MAX_UPLOAD_SIZE;

  if (size > maxSize) {
    throw new Error(
      parsed.folder === 'videos'
        ? 'Uploaded video exceeds the 50 MB maximum.'
        : 'Uploaded object exceeds the maximum allowed size.'
    );
  }

  return {
    objectKey: parsed.normalizedKey,
    size,
    contentType:
      response.ContentType || 'application/octet-stream'
  };
};

/* =========================================================
   PROCESS VIDEO WITH FFMPEG
========================================================= */

const processVideoWithFFmpeg = ({
  sourcePath,
  audioPath,
  outputPath,
  videoVolume = 1,
  musicVolume = 1,
  audioEnhancement = 'none',
  filter = 'none'
}) => {
  return new Promise((resolve, reject) => {
    const hasMusic = Boolean(audioPath);

    const safeVideoVolume = Math.max(
      0,
      Math.min(2, Number(videoVolume) || 1)
    );

    const safeMusicVolume = Math.max(
      0,
      Math.min(2, Number(musicVolume) || 1)
    );

    const normalizedFilter =
      normalizeVideoFilter(filter);

    const normalizedEnhancement =
      typeof audioEnhancement === 'string'
        ? audioEnhancement.trim().toLowerCase()
        : 'none';

    const allowedFilters = new Set([
      'none',
      'normal',
      'grayscale',
      'sepia',
      'vivid',
      'bright',
      'dark',
      'warm',
      'cool',
      'neon_cyber',
      'electric',
      'cinema',
      'golden_hour',
      'vintage',
      'midnight',
      'vibrant_pop'
    ]);

    const allowedEnhancements = new Set([
      'none',
      'crystal_voice',
      'studio_master',
      'bass_boost'
    ]);

    const safeFilter = allowedFilters.has(
      normalizedFilter
    )
      ? normalizedFilter
      : 'none';

    const safeEnhancement =
      allowedEnhancements.has(
        normalizedEnhancement
      )
        ? normalizedEnhancement
        : 'none';

    const videoFilterMap = {
      grayscale: 'hue=s=0',

      sepia:
        'colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131',

      vivid:
        'eq=contrast=1.12:saturation=1.25:brightness=0.02',

      bright:
        'eq=brightness=0.08:contrast=1.05',

      dark:
        'eq=brightness=-0.08:contrast=1.05',

      warm:
        'colorbalance=rs=.08:gs=.03:bs=-.03',

      cool:
        'colorbalance=rs=-.03:gs=.03:bs=.08',

      neon_cyber:
        'eq=contrast=1.08:saturation=1.35:brightness=0.02,hue=h=12',

      electric:
        'eq=contrast=1.18:saturation=1.50:brightness=0.02,hue=h=24',

      cinema:
        'eq=contrast=1.12:saturation=0.82:brightness=-0.01',

      golden_hour:
        'eq=contrast=1.06:saturation=1.25:brightness=0.025,colorbalance=rs=.08:gs=.03:bs=-.04',

      vintage:
        'eq=contrast=0.96:saturation=0.72:brightness=0.01',

      midnight:
        'eq=brightness=-0.08:contrast=1.18:saturation=0.90',

      vibrant_pop:
        'eq=contrast=1.08:saturation=1.65:brightness=0.02'
    };

    let command = ffmpeg(sourcePath);

    if (hasMusic) {
      command = command.input(audioPath);
    }

    if (!hasMusic) {
      if (videoFilterMap[safeFilter]) {
        command.videoFilters(
          videoFilterMap[safeFilter]
        );
      }

      command.outputOptions([
        '-map',
        '0:v:0',
        '-map',
        '0:a?',
        '-c:v',
        'libx264',
        '-preset',
        'veryfast',
        '-crf',
        '23',
        '-pix_fmt',
        'yuv420p',
        '-c:a',
        'aac',
        '-b:a',
        '128k',
        '-movflags',
        '+faststart',
        '-map_metadata',
        '-1'
      ]);
    } else {
      const filterGraph = [];

      if (videoFilterMap[safeFilter]) {
        filterGraph.push(
          `[0:v:0]${videoFilterMap[safeFilter]}[processed_video]`
        );
      } else {
        filterGraph.push(
          '[0:v:0]null[processed_video]'
        );
      }

      filterGraph.push(
        `[0:a:0]volume=${safeVideoVolume}[original_audio]`
      );

      filterGraph.push(
        `[1:a:0]volume=${safeMusicVolume}[music_audio]`
      );

      if (
        safeEnhancement ===
        'crystal_voice'
      ) {
        filterGraph.push(
          '[original_audio]highpass=f=80,lowpass=f=12000,acompressor=threshold=-18dB:ratio=3:attack=20:release=250[enhanced_audio]'
        );
      } else if (
        safeEnhancement ===
        'studio_master'
      ) {
        filterGraph.push(
          '[original_audio]highpass=f=50,lowpass=f=16000,acompressor=threshold=-16dB:ratio=2.5:attack=15:release=200,equalizer=f=3000:t=q:w=1:g=2[enhanced_audio]'
        );
      } else if (
        safeEnhancement ===
        'bass_boost'
      ) {
        filterGraph.push(
          '[original_audio]bass=g=5:f=100[enhanced_audio]'
        );
      } else {
        filterGraph.push(
          '[original_audio]anull[enhanced_audio]'
        );
      }

      filterGraph.push(
        '[enhanced_audio][music_audio]amix=inputs=2:duration=first:dropout_transition=2:normalize=0[mixed_audio]'
      );

      command.complexFilter(
        filterGraph
      );

      command.outputOptions([
        '-map',
        '[processed_video]',
        '-map',
        '[mixed_audio]',
        '-c:v',
        'libx264',
        '-preset',
        'veryfast',
        '-crf',
        '23',
        '-pix_fmt',
        'yuv420p',
        '-c:a',
        'aac',
        '-b:a',
        '128k',
        '-movflags',
        '+faststart',
        '-shortest',
        '-map_metadata',
        '-1'
      ]);
    }

    command
      .toFormat('mp4')
      .on('start', (commandLine) => {
        console.log('🎬 FFmpeg started.');
        console.log(commandLine);
      })
      .on('progress', (progress) => {
        if (
          progress.percent !== undefined
        ) {
          console.log(
            `🎬 FFmpeg progress: ${Number(
              progress.percent
            ).toFixed(1)}%`
          );
        }
      })
      .on(
        'error',
        (error, stdout, stderr) => {
          console.error(
            '❌ FFmpeg processing error:',
            error.message
          );

          if (stdout) {
            console.error(
              'FFmpeg stdout:',
              stdout.slice(-4000)
            );
          }

          if (stderr) {
            console.error(
              'FFmpeg stderr:',
              stderr.slice(-8000)
            );
          }

          reject(
            new Error(
              `ffmpeg exited with code 1: ${error.message}`
            )
          );
        }
      )
      .on('end', () => {
        console.log(
          '✅ FFmpeg processing completed.'
        );

        resolve();
      })
      .save(outputPath);
  });
};

/* =========================================================
   HEALTH
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

app.get('/api/storage/status', (req, res) => {
  res.json({
    configured: b2Configured,
    provider: 'Backblaze B2',
    bucket: b2Configured
      ? process.env.B2_BUCKET
      : null,
    region: b2Configured
      ? process.env.B2_REGION || 'us-east-005'
      : null
  });
});

/* =========================================================
   B2 SIGNED UPLOAD URL
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

      const normalizedFolder =
        String(folder).toLowerCase();

      if (
        !ALLOWED_STORAGE_FOLDERS.has(
          normalizedFolder
        )
      ) {
        return res.status(400).json({
          error: 'Invalid storage folder.',
          allowedFolders:
            Array.from(
              ALLOWED_STORAGE_FOLDERS
            )
        });
      }

      if (
        fileSize !== undefined &&
        fileSize !== null
      ) {
        const numericSize = Number(fileSize);
        const maxSize = getFolderMaxUploadSize(normalizedFolder);

        if (
          !Number.isFinite(numericSize) ||
          numericSize <= 0 ||
          numericSize > maxSize
        ) {
          return res.status(400).json({
            error:
              normalizedFolder === 'videos'
                ? 'Video exceeds the 50 MB maximum.'
                : 'File exceeds the maximum allowed size.',
            maxBytes: maxSize
          });
        }
      }

      const userId =
        req.authUser.id;

      const objectKey =
        createStorageObjectKey(
          userId,
          normalizedFolder,
          fileName
        );

      const command =
        new PutObjectCommand({
          Bucket:
            process.env.B2_BUCKET,
          Key:
            objectKey,
          ContentType:
            contentType
        });

      const uploadUrl =
        await getSignedUrl(
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
        bucket:
          process.env.B2_BUCKET,
        contentType,
        uploadHeaders: {
          'Content-Type':
            contentType
        },
        folder:
          normalizedFolder,
        objectUrl:
          getPublicObjectUrl(objectKey)
      });
    } catch (error) {
      console.error(
        '❌ B2 upload URL error:',
        error
      );

      return res.status(500).json({
        error:
          'Unable to create B2 upload URL.',
        details:
          error.message
      });
    }
  }
);

/* =========================================================
   B2 SIGNED DOWNLOAD URL
========================================================= */

app.post(
  '/api/storage/download-url',
  authenticateSupabaseUser,
  requireB2,
  async (req, res) => {
    try {
      const {
        objectKey
      } = req.body || {};

      if (
        !objectKey ||
        typeof objectKey !==
          'string'
      ) {
        return res.status(400).json({
          error:
            'Missing objectKey.'
        });
      }

      const parsed =
        parseStorageObjectKey(
          objectKey
        );

      if (!parsed) {
        return res.status(400).json({
          error:
            'Invalid object key.'
        });
      }

      if (
        parsed.ownerId !==
        req.authUser.id
      ) {
        return res.status(403).json({
          error:
            'You do not have permission to access this object.',
          code:
            'OBJECT_ACCESS_DENIED'
        });
      }

      const command =
        new GetObjectCommand({
          Bucket:
            process.env.B2_BUCKET,
          Key:
            parsed.normalizedKey
        });

      const downloadUrl =
        await getSignedUrl(
          b2,
          command,
          {
            expiresIn: 900
          }
        );

      return res.json({
        success: true,
        downloadUrl,
        objectKey:
          parsed.normalizedKey,
        expiresIn: 900
      });
    } catch (error) {
      console.error(
        '❌ B2 download URL error:',
        error
      );

      return res.status(500).json({
        error:
          'Unable to create download URL.',
        details:
          error.message
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
      const {
        objectKey
      } = req.body || {};

      if (
        !objectKey ||
        typeof objectKey !==
          'string'
      ) {
        return res.status(400).json({
          error:
            'Missing objectKey.'
        });
      }

      const parsed =
        parseStorageObjectKey(
          objectKey
        );

      if (!parsed) {
        return res.status(400).json({
          error:
            'Invalid object key.'
        });
      }

      if (
        parsed.ownerId !==
        req.authUser.id
      ) {
        return res.status(403).json({
          error:
            'You do not have permission to delete this object.',
          code:
            'OBJECT_DELETE_DENIED'
        });
      }

      await b2.send(
        new DeleteObjectCommand({
          Bucket:
            process.env.B2_BUCKET,
          Key:
            parsed.normalizedKey
        })
      );

      return res.json({
        success: true,
        deleted: true,
        objectKey:
          parsed.normalizedKey
      });
    } catch (error) {
      console.error(
        '❌ B2 delete error:',
        error
      );

      return res.status(500).json({
        error:
          'Unable to delete B2 object.',
        details:
          error.message
      });
    }
  }
);

/* =========================================================
   VIDEO / AUDIO MERGE
========================================================= */

const mergeVideoHandler = async (
  req,
  res
) => {
  let sourcePath = null;
  let audioPath = null;
  let outputPath = null;

  try {
    if (!req.authUser?.id) {
      return res.status(401).json({
        error:
          'Authentication required.',
        code:
          'AUTH_REQUIRED'
      });
    }

    if (!b2Configured || !b2) {
      return res.status(503).json({
        error:
          'Backblaze B2 storage is not configured.',
        code:
          'B2_NOT_CONFIGURED'
      });
    }

    const body =
      req.body || {};

    const sourceObjectKey =
      body.sourceObjectKey ||
      body.videoUrl;

    const audioUrl =
      body.audioUrl || null;

    const videoVolume =
      Number.isFinite(
        Number(body.videoVolume)
      )
        ? Number(body.videoVolume)
        : 1;

    const musicVolume =
      Number.isFinite(
        Number(body.musicVolume)
      )
        ? Number(body.musicVolume)
        : 1;

    const audioEnhancement =
      typeof body.audioEnhancement ===
      'string'
        ? body.audioEnhancement
            .trim()
            .toLowerCase()
        : 'none';

    const filter =
      typeof body.filter ===
      'string'
        ? body.filter
            .trim()
            .toLowerCase()
        : 'original';

    if (!sourceObjectKey) {
      return res.status(400).json({
        error:
          'Missing source video object key.'
      });
    }

    if (
      typeof sourceObjectKey !==
      'string'
    ) {
      return res.status(400).json({
        error:
          'Invalid source video object key.'
      });
    }

    const parsedSource =
      parseStorageObjectKey(
        sourceObjectKey
      );

    if (!parsedSource) {
      return res.status(400).json({
        error:
          'Invalid source video object key.'
      });
    }

    if (
      parsedSource.folder !==
      'videos'
    ) {
      return res.status(400).json({
        error:
          'Source video must be stored in the videos folder.'
      });
    }

    if (
      parsedSource.ownerId !==
      req.authUser.id
    ) {
      return res.status(403).json({
        error:
          'You do not have permission to process this video.',
        code:
          'SOURCE_ACCESS_DENIED'
      });
    }

    if (
      audioUrl !== null &&
      audioUrl !== '' &&
      !isHttpUrl(
        String(audioUrl)
      )
    ) {
      return res.status(400).json({
        error:
          'Invalid audioUrl. HTTP or HTTPS URL required.'
      });
    }

    const allowedEnhancements =
      new Set([
        'none',
        'crystal_voice',
        'studio_master',
        'bass_boost'
      ]);

    const safeEnhancement =
      allowedEnhancements.has(
        audioEnhancement
      )
        ? audioEnhancement
        : 'none';

    const safeVideoVolume =
      Math.max(
        0,
        Math.min(
          2,
          videoVolume
        )
      );

    const safeMusicVolume =
      Math.max(
        0,
        Math.min(
          2,
          musicVolume
        )
      );

    const normalizedFilter =
      normalizeVideoFilter(
        filter
      );

    console.log(
      '🎥 Video upload/processing request received.'
    );

    console.log(
      `👤 User: ${req.authUser.id}`
    );

    console.log(
      `📦 Source object: ${parsedSource.normalizedKey}`
    );

    console.log(
      `🎵 Audio supplied: ${Boolean(audioUrl)}`
    );

    console.log(
      `🔊 Video volume: ${safeVideoVolume}`
    );

    console.log(
      `🎵 Music volume: ${safeMusicVolume}`
    );

    console.log(
      `🎚️ Enhancement: ${safeEnhancement}`
    );

    console.log(
      `🎨 Filter: ${normalizedFilter}`
    );

    /* =======================================================
       FAST PATH

       If the uploaded B2 source does not require any
       transformation, DO NOT:

       - download video to Render
       - download music
       - run FFmpeg
       - upload another MP4

       The already-uploaded private B2 object becomes
       the published video.
    ======================================================= */

    const processingRequired =
      needsVideoProcessing({
        audioUrl,
        videoVolume:
          safeVideoVolume,
        musicVolume:
          safeMusicVolume,
        audioEnhancement:
          safeEnhancement,
        filter:
          normalizedFilter
      });

    console.log(
      `⚡ Processing required: ${processingRequired}`
    );

    if (!processingRequired) {
      console.log(
        '⚡ FAST PATH: verifying uploaded B2 object...'
      );

      const verifiedObject =
        await verifyB2Object(
          parsedSource.normalizedKey
        );

      console.log(
        `🚀 FAST PATH COMPLETE: ${verifiedObject.size} bytes published without FFmpeg.`
      );

      return res.status(200).json({
        success: true,
        processed: false,
        fastPath: true,
        objectKey:
          verifiedObject.objectKey,
        folder:
          'videos',
        contentType:
          verifiedObject.contentType,
        size:
          verifiedObject.size,
        objectUrl:
          getPublicObjectUrl(verifiedObject.objectKey)
      });
    }

    /* =======================================================
       PROCESSING PATH
    ======================================================= */

    const uniqueId =
      crypto.randomUUID();

    const sourceExtension =
      getExtension(
        parsedSource.normalizedKey
      ) || '.webm';

    sourcePath =
      path.join(
        os.tmpdir(),
        `made-source-${Date.now()}-${uniqueId}${sourceExtension}`
      );

    outputPath =
      path.join(
        os.tmpdir(),
        `made-final-${Date.now()}-${uniqueId}.mp4`
      );

    /* -------------------------------------------------------
       DOWNLOAD SOURCE VIDEO
    ------------------------------------------------------- */

    console.log(
      '⬇️ Downloading source video from B2...'
    );

    await downloadB2ObjectToFile(
      parsedSource.normalizedKey,
      sourcePath
    );

    if (
      !fs.existsSync(
        sourcePath
      )
    ) {
      throw new Error(
        'Source video could not be downloaded from B2.'
      );
    }

    const sourceStats =
      await fs.promises.stat(
        sourcePath
      );

    if (
      sourceStats.size <= 0
    ) {
      throw new Error(
        'Downloaded source video is empty.'
      );
    }

    console.log(
      `✅ Source video downloaded: ${sourceStats.size} bytes`
    );

    /* -------------------------------------------------------
       DOWNLOAD OPTIONAL MUSIC
    ------------------------------------------------------- */

    if (
      audioUrl &&
      String(audioUrl).trim() &&
      ![
        'null',
        'undefined'
      ].includes(
        String(audioUrl)
          .trim()
          .toLowerCase()
      )
    ) {
      const audioId =
        crypto.randomUUID();

      audioPath =
        path.join(
          os.tmpdir(),
          `made-audio-${Date.now()}-${audioId}.audio`
        );

      console.log(
        '⬇️ Downloading external audio...'
      );

      await downloadRemoteAudioToFile(
        String(audioUrl),
        audioPath
      );

      if (
        !fs.existsSync(
          audioPath
        )
      ) {
        throw new Error(
          'Audio file could not be downloaded.'
        );
      }

      const audioStats =
        await fs.promises.stat(
          audioPath
        );

      if (
        audioStats.size <= 0
      ) {
        throw new Error(
          'Downloaded audio file is empty.'
        );
      }

      console.log(
        `✅ Audio downloaded: ${audioStats.size} bytes`
      );
    }

    /* -------------------------------------------------------
       FFMPEG
    ------------------------------------------------------- */

    console.log(
      '🎬 Starting FFmpeg processing...'
    );

    await processVideoWithFFmpeg({
      sourcePath,
      audioPath,
      outputPath,
      videoVolume:
        safeVideoVolume,
      musicVolume:
        safeMusicVolume,
      audioEnhancement:
        safeEnhancement,
      filter:
        normalizedFilter
    });

    if (
      !fs.existsSync(
        outputPath
      )
    ) {
      throw new Error(
        'FFmpeg completed but generated video file was not found.'
      );
    }

    const outputStats =
      await fs.promises.stat(
        outputPath
      );

    if (
      outputStats.size <= 0
    ) {
      throw new Error(
        'Generated video file is empty.'
      );
    }

    console.log(
      `✅ Final MP4 generated: ${outputStats.size} bytes`
    );

    /* -------------------------------------------------------
       UPLOAD FINAL MP4 TO B2
    ------------------------------------------------------- */

    const finalFileName =
      `made-final-${Date.now()}-${uniqueId}.mp4`;

    const finalObjectKey =
      createStorageObjectKey(
        req.authUser.id,
        'videos',
        finalFileName
      );

    console.log(
      `⬆️ Uploading final MP4 to B2: ${finalObjectKey}`
    );

    const outputStream =
      fs.createReadStream(
        outputPath
      );

    await b2.send(
      new PutObjectCommand({
        Bucket:
          process.env.B2_BUCKET,
        Key:
          finalObjectKey,
        Body:
          outputStream,
        ContentType:
          'video/mp4',
        ContentLength:
          outputStats.size
      })
    );

    console.log(
      '☁️ Final MP4 uploaded successfully to B2.'
    );

    return res.status(200).json({
      success: true,
      processed: true,
      fastPath: false,
      objectKey:
        finalObjectKey,
      folder:
        'videos',
      contentType:
        'video/mp4',
      size:
        outputStats.size,
      objectUrl:
        getPublicObjectUrl(finalObjectKey)
    });
  } catch (error) {
    console.error(
      '❌ Video merge pipeline failed:',
      error
    );

    const message =
      String(
        error?.message || ''
      );

    if (
      message.includes(
        'NoSuchKey'
      ) ||
      message.includes(
        'The specified key does not exist'
      ) ||
      message.toLowerCase().includes(
        'source video could not be downloaded'
      )
    ) {
      return res.status(404).json({
        error:
          'Source video was not found in B2.',
        code:
          'SOURCE_VIDEO_NOT_FOUND'
      });
    }

    if (
      message.includes(
        'Uploaded B2 object is empty'
      ) ||
      message.includes(
        'Uploaded object exceeds'
      )
    ) {
      return res.status(422).json({
        error:
          message,
        code:
          'INVALID_UPLOADED_OBJECT'
      });
    }

    if (
      message.includes(
        'Audio download failed'
      ) ||
      message.includes(
        'Audio response contained no body'
      ) ||
      message.includes(
        'Invalid audio URL'
      ) ||
      message.includes(
        'Audio download timed out'
      )
    ) {
      return res.status(422).json({
        error:
          message,
        code:
          'AUDIO_DOWNLOAD_FAILED'
      });
    }

    if (
      message
        .toLowerCase()
        .includes('ffmpeg') ||
      message.includes(
        'Invalid data found when processing input'
      ) ||
      message.includes(
        'Output file'
      )
    ) {
      return res.status(422).json({
        error:
          `Video processing failed: ${message}`,
        code:
          'FFMPEG_PROCESSING_FAILED'
      });
    }

    return res.status(500).json({
      error:
        'Unable to process video.',
      code:
        'VIDEO_PROCESSING_FAILED',
      details:
        message
    });
  } finally {
    cleanupTempFiles(
      sourcePath,
      audioPath,
      outputPath
    );
  }
};

/* =========================================================
   VIDEO MERGE ROUTES
========================================================= */

app.post(
  '/api/storage/merge-video',
  authenticateSupabaseUser,
  requireB2,
  mergeVideoHandler
);

app.post(
  '/api/merge-video',
  authenticateSupabaseUser,
  requireB2,
  mergeVideoHandler
);

/* =========================================================
const { registerSocketServer } = require('./modules/socket-server');

registerSocketServer(io);

   SERVER START
========================================================= */

const PORT =
  process.env.PORT || 4000;

http.listen(
  PORT,
  () => {
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

    console.log(
      '🎬 Video merge routes: /api/merge-video + /api/storage/merge-video'
    );

    console.log(
      '⚡ Fast video publishing path: ENABLED'
    );
  }
);
