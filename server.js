    const endCall = ({
      roomId,
      to,
      userId,
      callId
    } = {}) => {
      const targetSocketId = resolveSocket(to || userId);
      const payload = { roomId, callId: callId || null };

      if (targetSocketId && targetSocketId !== socket.id) {
        io.to(targetSocketId).emit('peer_hung_up', payload);
      } else if (roomId) {
        socket.to(roomId).emit('peer_hung_up', payload);
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