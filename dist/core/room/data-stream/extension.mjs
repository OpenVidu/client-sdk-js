import {CLIENT_PROTOCOL_DEFAULT}from'../../version.mjs';import {ConnectionState}from'../CoreRoom.mjs';import {RoomEvent}from'../events.mjs';import {defineExtension}from'../extensions.mjs';import {ParticipantAgentAttributes}from'../participant/attributes.mjs';import {Track}from'../track/Track.mjs';import TranscriptionStreamConverter from'../transcription/TranscriptionStreamConverter.mjs';import IncomingDataStreamManager from'./incoming/IncomingDataStreamManager.mjs';import OutgoingDataStreamManager from'./outgoing/OutgoingDataStreamManager.mjs';/**
 * Text and byte streams (`sendText`, `sendFile`, `registerTextStreamHandler`, ...).
 * @experimental
 */
const dataStreams = /* @__PURE__ */defineExtension('dataStreams', [], (room, ctx, streamOptions) => {
  const incoming = new IncomingDataStreamManager(streamOptions !== null && streamOptions !== void 0 ? streamOptions : room.options.dataStream);
  const outgoing = new OutgoingDataStreamManager(room.engine, ctx.log, identity => {
    var _a, _b;
    return (_b = (_a = room.remoteParticipants.get(identity)) === null || _a === void 0 ? void 0 : _a.clientProtocol) !== null && _b !== void 0 ? _b : CLIENT_PROTOCOL_DEFAULT;
  }, identity => {
    var _a, _b;
    return (_b = (_a = room.remoteParticipants.get(identity)) === null || _a === void 0 ? void 0 : _a.capabilities) !== null && _b !== void 0 ? _b : [];
  }, () => Array.from(room.remoteParticipants.keys()));
  const getMicrophoneTrackSid = identity => {
    var _a;
    const participant = room.getParticipantByIdentity(identity);
    for (const publication of (_a = participant === null || participant === void 0 ? void 0 : participant.trackPublications.values()) !== null && _a !== void 0 ? _a : []) {
      if (publication.source === Track.Source.Microphone) {
        return publication.trackSid;
      }
    }
    return undefined;
  };
  const getDelegatingPublisherIdentity = identity => {
    // An avatar worker carries `lk.publish_on_behalf` naming the agent it speaks for.
    for (const participant of room.remoteParticipants.values()) {
      if (participant.attributes[ParticipantAgentAttributes.PublishOnBehalf] === identity) {
        return participant.identity;
      }
    }
    return undefined;
  };
  const transcriptionStreamConverter = new TranscriptionStreamConverter({
    onTranscription: transcription => ctx.handleTranscription(transcription),
    getMicrophoneTrackSid,
    getDelegatingPublisherIdentity
  });
  incoming.on('transcriptionStreamArrived', _ref => {
    let reader = _ref.reader,
      participantIdentity = _ref.participantIdentity;
    transcriptionStreamConverter.handleTextStream(reader, participantIdentity);
  });
  const onStreamPacket = (_value, packet, encryptionType) => incoming.handleDataStreamPacket(packet, encryptionType);
  room.on(RoomEvent.ConnectionStateChanged, state => incoming.setConnected(state === ConnectionState.Connected)).on(RoomEvent.ParticipantDisconnected, participant => incoming.validateParticipantHasNoActiveDataStreams(participant.identity));
  return {
    incoming,
    outgoing,
    // The preconnect audio buffer is sent as a byte stream from inside the core publish path.
    byteStreamOpener: options => outgoing.streamBytes(options),
    dataPackets: {
      streamHeader: onStreamPacket,
      streamChunk: onStreamPacket,
      streamTrailer: onStreamPacket
    },
    onEngineCreated: engine => outgoing.setupEngine(engine),
    onDisconnect: () => {
      transcriptionStreamConverter.reset();
      incoming.clearControllers();
    },
    room: {
      registerTextStreamHandler: (topic, callback) => incoming.registerTextStreamHandler(topic, callback),
      unregisterTextStreamHandler: topic => incoming.unregisterTextStreamHandler(topic),
      registerByteStreamHandler: (topic, callback) => incoming.registerByteStreamHandler(topic, callback),
      unregisterByteStreamHandler: topic => incoming.unregisterByteStreamHandler(topic)
    },
    localParticipant: {
      sendText: (text, options) => outgoing.sendText(text, options),
      streamText: options => outgoing.streamText(options),
      sendFile: (file, options) => outgoing.sendFile(file, options),
      sendBytes: (bytes, options) => outgoing.sendBytes(bytes, options),
      streamBytes: options => outgoing.streamBytes(options)
    }
  };
});export{dataStreams};//# sourceMappingURL=extension.mjs.map
