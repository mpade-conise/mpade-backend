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

  if (size > MAX_UPLOAD_SIZE) {
    throw new Error(
      'Uploaded object exceeds the maximum allowed size.'
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
        const numericSize =
          Number(fileSize);

        if (
          !Number.isFinite(
            numericSize
          ) ||
          numericSize <= 0 ||
          numericSize >
            MAX_UPLOAD_SIZE
        ) {
          return res.status(400).json({
            error: 'Invalid file size.',
            maxBytes:
              MAX_UPLOAD_SIZE
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
          normalizedFolder
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
          verifiedObject.size
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
        outputStats.size
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
   GLOBAL USER / STREAM STATE
========================================================= */

const activeUsers =
  new Map();

const streamRooms =
  new Map();

const callRooms =
  new Map();

/* =========================================================
   SOCKET HELPERS
========================================================= */

const resolveSocket = (
  value
) => {
  if (!value) {
    return null;
  }

  const stringValue =
    String(value);

  const directSocket =
    io.sockets.sockets.get(
      stringValue
    );

  if (directSocket) {
    return directSocket.id;
  }

  return (
    activeUsers.get(
      stringValue
    ) || null
  );
};

const rememberCallRoom = (
  roomId,
  socketId
) => {
  if (!roomId) {
    return;
  }

  if (
    !callRooms.has(
      roomId
    )
  ) {
    callRooms.set(
      roomId,
      new Set()
    );
  }

  callRooms
    .get(roomId)
    .add(socketId);
};

const forgetSocketFromCallRooms = (
  socketId
) => {
  for (
    const [
      roomId,
      members
    ] of callRooms
  ) {
    members.delete(
      socketId
    );

    if (!members.size) {
      callRooms.delete(
        roomId
      );
    }
  }
};

/* =========================================================
   SOCKET.IO
========================================================= */

io.on(
  'connection',
  (socket) => {
    const {
      room,
      role,
      streamId
    } =
      socket.handshake.query;

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
          String(
            streamId || room
          );

        activeUsers.set(
          hostIdentifier,
          socket.id
        );

        socket.hostIdentifier =
          hostIdentifier;

        if (
          !streamRooms.has(
            hostIdentifier
          )
        ) {
          streamRooms.set(
            hostIdentifier,
            {
              hostSocketId:
                socket.id,
              guestPanels:
                new Map()
            }
          );
        } else {
          streamRooms.get(
            hostIdentifier
          ).hostSocketId =
            socket.id;
        }

        console.log(
          `📡 Host registered: ${hostIdentifier} -> ${socket.id}`
        );
      }
    } else {
      console.log(
        `🔌 New client without room: ${socket.id}`
      );
    }

    const broadcastRoomPresence =
      async (
        roomName
      ) => {
        try {
          const sockets =
            await io
              .in(roomName)
              .fetchSockets();

          const viewersList =
            sockets
              .filter(
                (s) =>
                  s.handshake
                    .query
                    .role ===
                    'viewer' ||
                  s.handshake
                    .query
                    .role ===
                    'signal-viewer'
              )
              .map(
                (s) => ({
                  socketId:
                    s.id,
                  username:
                    s.handshake
                      .query
                      .username ||
                    'Anonymous'
                })
              );

          io.to(
            roomName
          ).emit(
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
          id:
            socket.id,
          username:
            socket.handshake
              .query
              .username
        }
      );

      broadcastRoomPresence(
        room
      );
    }

    /* =======================================================
       USER SESSION
    ======================================================= */

    socket.on(
      'register_user_session',
      ({ userId } = {}) => {
        if (!userId) {
          return;
        }

        socket.userId =
          String(userId);

        activeUsers.set(
          String(userId),
          socket.id
        );

        io.emit(
          'friend_presence_changed',
          {
            userId,
            status:
              'online'
          }
        );

        console.log(
          `🟢 User ${userId} registered on socket ${socket.id}`
        );
      }
    );

    /* =======================================================
       DIRECT CALL SIGNAL
    ======================================================= */

    socket.on(
      'initiate_call_signal',
      (callPayload = {}) => {
        const targetSocketId =
          resolveSocket(
            callPayload.receiverId
          );

        if (
          targetSocketId &&
          targetSocketId !==
            socket.id
        ) {
          console.log(
            `📞 Routing ${callPayload.callType || 'call'} to ${targetSocketId}`
          );

          io.to(
            targetSocketId
          ).emit(
            'incoming_call_signal',
            callPayload
          );
        }
      }
    );

    socket.on(
      'decline_call',
      ({ callerId } = {}) => {
        const targetSocketId =
          resolveSocket(
            callerId
          );

        if (targetSocketId) {
          io.to(
            targetSocketId
          ).emit(
            'call_cancelled_by_caller'
          );
        }
      }
    );

    /* =======================================================
       P2P CALL ROOMS
    ======================================================= */

    socket.on(
      'join_call_room',
      ({
        roomId,
        userId,
        targetPeerId
      } = {}) => {
        if (!roomId) {
          return;
        }

        socket.join(roomId);

        rememberCallRoom(
          roomId,
          socket.id
        );

        if (userId) {
          socket.userId =
            String(userId);

          activeUsers.set(
            String(userId),
            socket.id
          );
        }

        console.log(
          `📞 Socket ${socket.id} joined P2P call room: ${roomId}`
        );

        socket.to(roomId).emit(
          'peer_ready',
          {
            userId,
            socketId:
              socket.id
          }
        );

        void targetPeerId;
      }
    );

    socket.on(
      'peer_ready',
      ({
        roomId,
        userId
      } = {}) => {
        if (!roomId) {
          return;
        }

        socket.to(roomId).emit(
          'peer_ready',
          {
            userId,
            socketId:
              socket.id
          }
        );
      }
    );

    const endCall = ({
      roomId,
      to,
      userId
    } = {}) => {
      const targetSocketId =
        resolveSocket(
          to || userId
        );

      if (
        targetSocketId &&
        targetSocketId !==
          socket.id
      ) {
        io.to(
          targetSocketId
        ).emit(
          'peer_hung_up',
          {
            roomId
          }
        );
      }

      if (roomId) {
        socket.to(roomId).emit(
          'peer_hung_up',
          {
            roomId
          }
        );

        socket.leave(
          roomId
        );

        const members =
          callRooms.get(
            roomId
          );

        if (members) {
          members.delete(
            socket.id
          );

          if (!members.size) {
            callRooms.delete(
              roomId
            );
          }
        }
      }
    };

    socket.on(
      'reject_incoming_call',
      endCall
    );

    socket.on(
      'end_call',
      endCall
    );

    socket.on(
      'hang_up_call',
      endCall
    );

    /* =======================================================
       MULTI-PANEL LIVE STREAM INGEST
    ======================================================= */

    socket.on(
      'publish_guest_feed',
      ({
        streamId: sid,
        guestId,
        targetHostId,
        sdpOffer,
        mode
      } = {}) => {
        if (
          !sid ||
          !guestId
        ) {
          return;
        }

        const targetHostSocketId =
          resolveSocket(
            targetHostId
          ) ||
          streamRooms.get(
            sid
          )?.hostSocketId;

        if (
          !streamRooms.has(
            sid
          )
        ) {
          streamRooms.set(
            sid,
            {
              hostSocketId:
                targetHostSocketId,
              guestPanels:
                new Map()
            }
          );
        }

        const roomState =
          streamRooms.get(
            sid
          );

        if (
          targetHostSocketId
        ) {
          roomState.hostSocketId =
            targetHostSocketId;
        }

        roomState.guestPanels.set(
          String(guestId),
          socket.id
        );

        socket.data.isGuestPanel =
          true;

        socket.data.guestId =
          String(guestId);

        socket.data.streamId =
          sid;

        const payload = {
          guestId,
          guestSocketId:
            socket.id,
          sdpOffer,
          mode
        };

        console.log(
          `🎥 Guest ${guestId} publishing feed to stream ${sid}`
        );

        if (
          targetHostSocketId
        ) {
          io.to(
            targetHostSocketId
          ).emit(
            'incoming_guest_panel_feed',
            payload
          );
        } else {
          socket.to(sid).emit(
            'incoming_guest_panel_feed',
            payload
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
        if (
          !guestSocketId
        ) {
          return;
        }

        io.to(
          guestSocketId
        ).emit(
          'broadcast_ack_received',
          {
            sdpAnswer,
            guestId
          }
        );
      }
    );

    socket.on(
      'guest_ice_candidate',
      ({
        streamId: sid,
        candidate,
        to
      } = {}) => {
        const targetHostSocketId =
          resolveSocket(to) ||
          streamRooms.get(
            sid
          )?.hostSocketId;

        if (
          targetHostSocketId
        ) {
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
        } else if (sid) {
          socket.to(sid).emit(
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
        if (
          !targetGuestSocketId
        ) {
          return;
        }

        io.to(
          targetGuestSocketId
        ).emit(
          'incoming_host_ice',
          {
            candidate
          }
        );
      }
    );

    socket.on(
      'remove_guest_panel',
      ({
        streamId: sid,
        guestId
      } = {}) => {
        const roomState =
          streamRooms.get(
            sid
          );

        if (
          !roomState ||
          !roomState.guestPanels.has(
            String(guestId)
          )
        ) {
          return;
        }

        const guestSocketId =
          roomState.guestPanels.get(
            String(guestId)
          );

        io.to(
          guestSocketId
        ).emit(
          'removed_from_panel'
        );

        roomState.guestPanels.delete(
          String(guestId)
        );

        console.log(
          `🚫 Guest ${guestId} removed from panel`
        );
      }
    );

    /* =======================================================
       COHOST MANAGEMENT
    ======================================================= */

    socket.on(
      'approve_cohost',
      ({
        streamId: sid,
        guestId,
        mode
      } = {}) => {
        if (
          !sid ||
          !guestId
        ) {
          return;
        }

        const payload = {
          streamId:
            sid,
          guestId,
          mode
        };

        io.to(sid).emit(
          'cohost_approved',
          payload
        );

        const targetGuestSocketId =
          resolveSocket(
            guestId
          );

        if (
          targetGuestSocketId
        ) {
          io.to(
            targetGuestSocketId
          ).emit(
            'cohost_approved',
            payload
          );
        }
      }
    );

    socket.on(
      'kick_cohost',
      ({
        streamId: sid,
        guestId
      } = {}) => {
        if (
          !sid ||
          !guestId
        ) {
          return;
        }

        const payload = {
          streamId:
            sid,
          guestId
        };

        io.to(sid).emit(
          'cohost_kicked',
          payload
        );

        const targetGuestSocketId =
          resolveSocket(
            guestId
          );

        if (
          targetGuestSocketId
        ) {
          io.to(
            targetGuestSocketId
          ).emit(
            'cohost_kicked',
            payload
          );
        }
      }
    );

    socket.on(
      'send_cohost_invite',
      (data = {}) => {
        const targetSocketId =
          resolveSocket(
            data.targetUserId
          );

        if (
          targetSocketId
        ) {
          io.to(
            targetSocketId
          ).emit(
            'cohost_invite_received',
            {
              room:
                data.room,
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
          resolveSocket(
            data.targetUserId
          );

        if (
          originHostSocketId
        ) {
          io.to(
            originHostSocketId
          ).emit(
            'cohost_invite_accepted',
            {
              room:
                data.room,
              status:
                data.status
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
      ({
        streamId: sid
      } = {}) => {
        if (!sid) {
          return;
        }

        socket.to(sid).emit(
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

    const routeWebRTCOffer = (
      data = {}
    ) => {
      const {
        streamId: sid,
        roomId,
        offer,
        targetViewerId,
        to,
        guestId,
        mode
      } = data;

      const activeRoom =
        roomId || sid;

      const targetId =
        targetViewerId || to;

      const targetSocketId =
        resolveSocket(
          targetId
        );

      const payload = {
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

      console.log(
        `📤 WebRTC offer from ${socket.id} -> ${targetId || activeRoom || 'none'}`
      );

      if (
        targetSocketId &&
        targetSocketId !==
          socket.id
      ) {
        io.to(
          targetSocketId
        ).emit(
          'webrtc_offer_received',
          payload
        );
      } else if (
        activeRoom
      ) {
        socket.to(
          activeRoom
        ).emit(
          'webrtc_offer_received',
          payload
        );
      }
    };

    const routeWebRTCAnswer = (
      data = {}
    ) => {
      const {
        streamId: sid,
        roomId,
        answer,
        to,
        targetSocketId: targetId
      } = data;

      const activeRoom =
        roomId || sid;

      const destination =
        resolveSocket(
          to || targetId
        );

      const payload = {
        answer,
        viewerSocketId:
          socket.id,
        senderSocketId:
          socket.id
      };

      console.log(
        `📥 WebRTC answer from ${socket.id} -> ${to || targetId || activeRoom || 'none'}`
      );

      if (
        destination &&
        destination !==
          socket.id
      ) {
        io.to(
          destination
        ).emit(
          'webrtc_answer_received',
          payload
        );
      } else if (
        activeRoom
      ) {
        socket.to(
          activeRoom
        ).emit(
          'webrtc_answer_received',
          payload
        );
      }
    };

    const routeWebRTCIce = (
      data = {}
    ) => {
      const {
        streamId: sid,
        roomId,
        candidate,
        targetSocketId,
        to,
        senderType
      } = data;

      const activeRoom =
        roomId || sid;

      const destination =
        resolveSocket(
          to || targetSocketId
        );

      const payload = {
        candidate,
        senderType,
        senderSocketId:
          socket.id
      };

      if (
        destination &&
        destination !==
          socket.id
      ) {
        io.to(
          destination
        ).emit(
          'incoming_ice_candidate',
          payload
        );
      } else if (
        activeRoom
      ) {
        socket.to(
          activeRoom
        ).emit(
          'incoming_ice_candidate',
          payload
        );
      }
    };

    socket.on(
      'send_webrtc_offer',
      routeWebRTCOffer
    );

    socket.on(
      'send_webrtc_answer',
      routeWebRTCAnswer
    );

    socket.on(
      'webrtc_ice_candidate',
      routeWebRTCIce
    );

    /* Legacy aliases */

    socket.on(
      'webrtc_offer',
      routeWebRTCOffer
    );

    socket.on(
      'webrtc_answer',
      routeWebRTCAnswer
    );

    socket.on(
      'send_ice_candidate',
      routeWebRTCIce
    );

    socket.on(
      'ice_candidate',
      routeWebRTCIce
    );

    /* =======================================================
       CHAT / PRESENCE
    ======================================================= */

    socket.on(
      'user_going_online',
      (userId) => {
        if (!userId) {
          return;
        }

        socket.userId =
          String(userId);

        activeUsers.set(
          String(userId),
          socket.id
        );

        io.emit(
          'friend_presence_changed',
          {
            userId,
            status:
              'online'
          }
        );
      }
    );

    socket.on(
      'send_chat_message',
      (messagePayload = {}) => {
        const targetSocketId =
          resolveSocket(
            messagePayload.receiver_id
          );

        if (
          targetSocketId
        ) {
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
          resolveSocket(
            updatedPayload.receiver_id
          );

        if (
          targetSocketId
        ) {
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

        if (
          room &&
          (
            role === 'viewer' ||
            role === 'signal-viewer'
          )
        ) {
          broadcastRoomPresence(
            room
          );
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

            if (
              roomState.hostSocketId
            ) {
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

        if (
          socket.hostIdentifier
        ) {
          const hostKey =
            String(
              socket.hostIdentifier
            );

          if (
            activeUsers.get(
              hostKey
            ) === socket.id
          ) {
            activeUsers.delete(
              hostKey
            );
          }

          const roomState =
            streamRooms.get(
              hostKey
            );

          if (
            roomState &&
            roomState.hostSocketId ===
              socket.id
          ) {
            streamRooms.delete(
              hostKey
            );
          }
        }

        forgetSocketFromCallRooms(
          socket.id
        );

        if (
          socket.userId
        ) {
          const userKey =
            String(
              socket.userId
            );

          if (
            activeUsers.get(
              userKey
            ) === socket.id
          ) {
            activeUsers.delete(
              userKey
            );

            io.emit(
              'friend_presence_changed',
              {
                userId:
                  socket.userId,
                status:
                  'offline'
              }
            );
          }
        }
      }
    );
  }
);

/* =========================================================
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
