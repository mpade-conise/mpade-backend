const registerMediaRoutes = ({ app, http, io, crypto, path, fs, os, ffmpeg, ALLOWED_ORIGINS, ALLOWED_STORAGE_FOLDERS, b2Configured, b2, authenticateSupabaseUser, sanitizeFileName, getExtension, isValidContentType, createStorageObjectKey, parseStorageObjectKey, getPrivateObjectUrl, getFolderMaxUploadSize, isHttpUrl, cleanupTempFiles, downloadB2ObjectToFile, downloadRemoteAudioToFile, normalizeVideoFilter, needsVideoProcessing, verifyB2Object, processVideoWithFFmpeg }) => {
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
          await getPrivateObjectUrl(objectKey)
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
          await getPrivateObjectUrl(verifiedObject.objectKey)
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
        await getPrivateObjectUrl(finalObjectKey)
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

};

module.exports = { registerMediaRoutes };
