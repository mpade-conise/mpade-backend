const mediasoup = require('mediasoup');

const rooms = new Map();
let workerPromise;

const codecs = [
  { kind: 'audio', mimeType: 'audio/opus', clockRate: 48000, channels: 2 },
  { kind: 'video', mimeType: 'video/VP8', clockRate: 90000, parameters: { 'x-google-start-bitrate': 1000, 'x-google-max-bitrate': 2500 } },
  { kind: 'video', mimeType: 'video/H264', clockRate: 90000, parameters: { 'packetization-mode': 1, 'profile-level-id': '42e01f', 'level-asymmetry-allowed': 1 } }
];

const getWorker = () => {
  if (!workerPromise) {
    workerPromise = mediasoup.createWorker({
      logLevel: process.env.MEDIASOUP_LOG_LEVEL || 'warn',
      rtcMinPort: Number(process.env.MEDIASOUP_MIN_PORT || 40000),
      rtcMaxPort: Number(process.env.MEDIASOUP_MAX_PORT || 49999)
    });
    workerPromise.then(worker => worker.on('died', () => { workerPromise = null; }));
  }
  return workerPromise;
};

const getRoom = async streamId => {
  const id = String(streamId);
  if (rooms.has(id)) return rooms.get(id);
  const router = await (await getWorker()).createRouter({ mediaCodecs: codecs });
  const room = { router, peers: new Map() };
  rooms.set(id, room);
  return room;
};

const transportOptions = () => {
  const announcedAddress = process.env.MEDIASOUP_ANNOUNCED_IP || process.env.MEDIASOUP_ANNOUNCED_ADDRESS;
  if (!announcedAddress) throw new Error('MEDIASOUP_ANNOUNCED_IP is required.');
  return {
    listenInfos: [{ protocol: 'udp', ip: process.env.MEDIASOUP_LISTEN_IP || '0.0.0.0', announcedAddress, portRange: { min: Number(process.env.MEDIASOUP_MIN_PORT || 40000), max: Number(process.env.MEDIASOUP_MAX_PORT || 49999) } }],
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
    initialAvailableOutgoingBitrate: 1500000
  };
};

const leave = (streamId, socket) => {
  const id = String(streamId || '');
  const room = rooms.get(id);
  const peer = room?.peers.get(socket.id);
  if (!room || !peer) return;
  peer.producers.forEach(p => p.close());
  peer.consumers.forEach(c => c.close());
  peer.sendTransport?.close();
  peer.recvTransport?.close();
  room.peers.delete(socket.id);
  socket.to(id).emit('sfu_peer_left', { peerId: socket.id });
  if (!room.peers.size) { room.router.close(); rooms.delete(id); }
};

const attachLiveSFU = io => {
  io.on('connection', socket => {
    let streamId = null;

    socket.on('sfu_join', async ({ streamId: sid, role = 'viewer' } = {}, cb) => {
      try {
        if (!sid) throw new Error('streamId is required.');
        const room = await getRoom(sid);
        streamId = String(sid);
        let peer = room.peers.get(socket.id);
        if (!peer) {
          peer = { role, sendTransport: null, recvTransport: null, producers: new Map(), consumers: new Map() };
          room.peers.set(socket.id, peer);
        } else peer.role = role;
        const producers = [];
        room.peers.forEach((other, peerId) => {
          if (peerId !== socket.id) other.producers.forEach(p => producers.push({ producerId: p.id, peerId, kind: p.kind, role: other.role }));
        });
        cb?.({ ok: true, routerRtpCapabilities: room.router.rtpCapabilities, producers });
      } catch (error) { cb?.({ ok: false, error: error.message }); }
    });

    socket.on('sfu_create_transport', async ({ streamId: sid, direction } = {}, cb) => {
      try {
        const room = rooms.get(String(sid));
        const peer = room?.peers.get(socket.id);
        if (!room || !peer) throw new Error('Join the SFU room first.');
        const transport = await room.router.createWebRtcTransport(transportOptions());
        if (direction === 'send') { peer.sendTransport?.close(); peer.sendTransport = transport; }
        else if (direction === 'recv') { peer.recvTransport?.close(); peer.recvTransport = transport; }
        else throw new Error('Invalid transport direction.');
        transport.on('dtlsstatechange', state => { if (state === 'closed') transport.close(); });
        cb?.({ ok: true, id: transport.id, iceParameters: transport.iceParameters, iceCandidates: transport.iceCandidates, dtlsParameters: transport.dtlsParameters });
      } catch (error) { cb?.({ ok: false, error: error.message }); }
    });

    socket.on('sfu_connect_transport', async ({ streamId: sid, direction, dtlsParameters } = {}, cb) => {
      try {
        const peer = rooms.get(String(sid))?.peers.get(socket.id);
        const transport = direction === 'send' ? peer?.sendTransport : peer?.recvTransport;
        if (!transport) throw new Error('SFU transport not found.');
        await transport.connect({ dtlsParameters });
        cb?.({ ok: true });
      } catch (error) { cb?.({ ok: false, error: error.message }); }
    });

    socket.on('sfu_produce', async ({ streamId: sid, kind, rtpParameters, appData } = {}, cb) => {
      try {
        const room = rooms.get(String(sid));
        const peer = room?.peers.get(socket.id);
        if (!peer?.sendTransport) throw new Error('Send transport not found.');
        const producer = await peer.sendTransport.produce({ kind, rtpParameters, appData: { ...(appData || {}), socketId: socket.id } });
        peer.producers.set(producer.id, producer);
        producer.on('close', () => peer.producers.delete(producer.id));
        producer.on('transportclose', () => peer.producers.delete(producer.id));
        socket.to(String(sid)).emit('sfu_new_producer', { producerId: producer.id, peerId: socket.id, kind, role: peer.role });
        cb?.({ ok: true, id: producer.id });
      } catch (error) { cb?.({ ok: false, error: error.message }); }
    });

    socket.on('sfu_consume', async ({ streamId: sid, producerId, rtpCapabilities } = {}, cb) => {
      try {
        const room = rooms.get(String(sid));
        const peer = room?.peers.get(socket.id);
        if (!room || !peer?.recvTransport) throw new Error('Receive transport not found.');
        if (!room.router.canConsume({ producerId, rtpCapabilities })) throw new Error('Cannot consume producer.');
        const consumer = await peer.recvTransport.consume({ producerId, rtpCapabilities, paused: true });
        peer.consumers.set(consumer.id, consumer);
        consumer.on('producerclose', () => { peer.consumers.delete(consumer.id); socket.emit('sfu_producer_closed', { producerId }); });
        consumer.on('transportclose', () => peer.consumers.delete(consumer.id));
        cb?.({ ok: true, id: consumer.id, producerId, kind: consumer.kind, rtpParameters: consumer.rtpParameters });
      } catch (error) { cb?.({ ok: false, error: error.message }); }
    });

    socket.on('sfu_resume', async ({ streamId: sid, consumerId } = {}, cb) => {
      try {
        const consumer = rooms.get(String(sid))?.peers.get(socket.id)?.consumers.get(consumerId);
        if (!consumer) throw new Error('Consumer not found.');
        await consumer.resume();
        cb?.({ ok: true });
      } catch (error) { cb?.({ ok: false, error: error.message }); }
    });

    socket.on('sfu_close_producer', ({ streamId: sid, producerId } = {}) => {
      rooms.get(String(sid))?.peers.get(socket.id)?.producers.get(producerId)?.close();
    });

    socket.on('sfu_leave', () => { if (streamId) { leave(streamId, socket); streamId = null; } });
    socket.on('disconnect', () => { if (streamId) leave(streamId, socket); });
  });
  getWorker().then(() => console.log('🛰️ Live SFU worker ready.')).catch(error => console.warn('⚠️ Live SFU unavailable:', error.message));
};

module.exports = { attachLiveSFU };
