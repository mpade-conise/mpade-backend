const registerSocketServer = (io) => {
/* =========================================================
   GLOBAL USER / STREAM STATE
========================================================= */

const activeUsers =
  new Map();

const userSockets =
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

  const userSocketIds = userSockets.get(stringValue);
  if (userSocketIds?.size) {
    for (const socketId of userSocketIds) {
      if (io.sockets.sockets.has(socketId)) return socketId;
    }
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

        const userKey = String(userId);
        if (!userSockets.has(userKey)) userSockets.set(userKey, new Set());
        userSockets.get(userKey).add(socket.id);

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
      (data = {}) => {
        const callerId =
          data.callerId ||
          data.to ||
          data.userId;

        const targetSocketId =
          resolveSocket(
            callerId
          );

        const payload = {
          roomId:
            data.roomId || null,
          callId:
            data.callId || null,
          reason:
            'declined'
        };

        if (targetSocketId) {
          io.to(
            targetSocketId
          ).emit(
            'call_cancelled_by_caller',
            payload
          );
        }

        if (data.roomId) {
          socket.to(
            data.roomId
          ).emit(
            'peer_hung_up',
            payload
          );
        } else if (
          targetSocketId &&
          targetSocketId !== socket.id
        ) {
          io.to(
            targetSocketId
          ).emit(
            'peer_hung_up',
            payload
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

          const userKey = String(userId);
          if (!userSockets.has(userKey)) userSockets.set(userKey, new Set());
          userSockets.get(userKey).add(socket.id);
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
      userId,
      callId
    } = {}) => {
      const targetSocketId = resolveSocket(to || userId);
      const payload = { roomId, callId: callId || null };

      if (roomId) {
        socket.to(roomId).emit('peer_hung_up', payload);
      } else if (targetSocketId && targetSocketId !== socket.id) {
        io.to(targetSocketId).emit('peer_hung_up', payload);
      }

      if (roomId) {
        socket.leave(roomId);

        const members = callRooms.get(roomId);
        if (members) {
          members.delete(socket.id);
          if (!members.size) callRooms.delete(roomId);
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

        if (!originHostSocketId) {
          console.warn(
            '⚠️ Co-host response target socket not found:',
            data.targetUserId
          );
          return;
        }

        const eventName =
          data.status === 'accepted'
            ? 'cohost_invite_accepted'
            : 'cohost_invite_declined';

        io.to(
          originHostSocketId
        ).emit(
          eventName,
          {
            room:
              data.room,
            status:
              data.status,
            responderSocketId:
              socket.id,
            responderUserId:
              socket.userId || null
          }
        );
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

        const roomState =
          streamRooms.get(String(sid));

        const hostSocketId =
          roomState?.hostSocketId;

        if (hostSocketId) {
          io.to(hostSocketId).emit(
            'viewer_requesting_stream',
            {
              viewerSocketId:
                socket.id
            }
          );
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

      const payload = {
        offer,
        callId: data.callId || null,
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

      if (activeRoom) {
        socket.to(
          activeRoom
        ).emit(
          'webrtc_offer_received',
          payload
        );
      } else {
        const targetSocketId =
          resolveSocket(
            targetId
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
        }
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
        callId: data.callId || null,
        viewerSocketId:
          socket.id,
        senderSocketId:
          socket.id,
        targetHostSocketId:
          destination || null
      };

      console.log(
        `📥 WebRTC answer from ${socket.id} -> ${to || targetId || activeRoom || 'none'}`
      );

      if (activeRoom) {
        socket.to(
          activeRoom
        ).emit(
          'webrtc_answer_received',
          payload
        );
      } else if (
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

      if (activeRoom) {
        socket.to(
          activeRoom
        ).emit(
          'incoming_ice_candidate',
          payload
        );
      } else if (
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

        const userKey = String(userId);
        if (!userSockets.has(userKey)) userSockets.set(userKey, new Set());
        userSockets.get(userKey).add(socket.id);

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
        receiverId,
        isTyping,
        mode
      } = {}) => {
        const payload = {
          userId,
          isTyping,
          mode
        };

        const targetSocketId = resolveSocket(receiverId);

        if (targetSocketId && targetSocketId !== socket.id) {
          io.to(targetSocketId).emit(
            'peer_typing_state_changed',
            payload
          );
          return;
        }

        socket.broadcast.emit(
          'peer_typing_state_changed',
          payload
        );
      }
    );

    /* =======================================================
       IN-CALL DATA
    ======================================================= */

    socket.on('in_call_text_message', (data = {}) => {
      if (!data.roomId) return;
      socket.to(data.roomId).emit('in_call_text_message', data);
    });

    socket.on('in_call_reaction_burst', (data = {}) => {
      if (!data.roomId || !data.emoji) return;
      socket.to(data.roomId).emit('in_call_reaction_burst', data);
    });

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

        if (socket.userId) {
          const userKey = String(socket.userId);
          const socketsForUser = userSockets.get(userKey);
          if (socketsForUser) {
            socketsForUser.delete(socket.id);
            if (!socketsForUser.size) {
              userSockets.delete(userKey);
              io.emit('friend_presence_changed', { userId: socket.userId, status: 'offline' });
            }
          }
        }
      }
    );
  }
);

};

module.exports = { registerSocketServer };
