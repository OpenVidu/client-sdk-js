import {slicedToArray as _slicedToArray}from'../../../_virtual/_rollupPluginBabelHelpers.mjs';import {__awaiter}from'tslib';import {DataStream_CompressionType,DataStream_Chunk}from'@livekit/protocol';import {EventEmitter}from'events';import livekitLogger from'../../../logger.mjs';import {DataStreamError,DataStreamErrorReason}from'../../errors.mjs';import {isDeflateRawCompressionSupported,bigIntToNumber,numberToBigInt}from'../../utils.mjs';import {deflateRawDecompress,inflateRawTransform}from'../compression.mjs';import {DEFAULT_MAX_PAYLOAD_BYTE_LENGTH,DEFAULT_MAX_OPEN_STREAMS_PER_PARTICIPANT,TRANSCRIPTION_TOPIC}from'../constants.mjs';import {TextStreamReader,ByteStreamReader}from'./StreamReader.mjs';class IncomingDataStreamManager extends EventEmitter {
  constructor() {
    let _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      _ref$maxPayloadByteLe = _ref.maxPayloadByteLength,
      maxPayloadByteLength = _ref$maxPayloadByteLe === void 0 ? DEFAULT_MAX_PAYLOAD_BYTE_LENGTH : _ref$maxPayloadByteLe,
      _ref$maxOpenStreamsPe = _ref.maxOpenStreamsPerParticipant,
      maxOpenStreamsPerParticipant = _ref$maxOpenStreamsPe === void 0 ? DEFAULT_MAX_OPEN_STREAMS_PER_PARTICIPANT : _ref$maxOpenStreamsPe;
    super();
    this.log = livekitLogger;
    this.byteStreamControllers = new Map();
    this.textStreamControllers = new Map();
    this.byteStreamHandlers = new Map();
    this.textStreamHandlers = new Map();
    this.isConnected = false;
    this.bufferedPackets = [];
    this.maxPayloadByteLength = maxPayloadByteLength;
    this.maxOpenStreamsPerParticipant = maxOpenStreamsPerParticipant;
  }
  setConnected(connected) {
    this.isConnected = connected;
    if (connected) {
      this.flushBufferedPackets();
    }
  }
  flushBufferedPackets() {
    const packets = this.bufferedPackets;
    this.bufferedPackets = [];
    for (const _ref2 of packets) {
      const packet = _ref2.packet;
      const encryptionType = _ref2.encryptionType;
      this.handleDataStreamPacket(packet, encryptionType);
    }
  }
  registerTextStreamHandler(topic, callback) {
    if (this.textStreamHandlers.has(topic)) {
      throw new DataStreamError("A text stream handler for topic \"".concat(topic, "\" has already been set."), DataStreamErrorReason.HandlerAlreadyRegistered);
    }
    this.textStreamHandlers.set(topic, callback);
  }
  unregisterTextStreamHandler(topic) {
    this.textStreamHandlers.delete(topic);
  }
  registerByteStreamHandler(topic, callback) {
    if (this.byteStreamHandlers.has(topic)) {
      throw new DataStreamError("A byte stream handler for topic \"".concat(topic, "\" has already been set."), DataStreamErrorReason.HandlerAlreadyRegistered);
    }
    this.byteStreamHandlers.set(topic, callback);
  }
  unregisterByteStreamHandler(topic) {
    this.byteStreamHandlers.delete(topic);
  }
  clearControllers() {
    // Error any in flight streams so readers do not wait forever for chunks that can no longer arrive.
    const abnormalEndError = new DataStreamError('Room disconnected in the middle of receiving data', DataStreamErrorReason.AbnormalEnd);
    for (const _ref3 of this.byteStreamControllers.values()) {
      const controller = _ref3.controller;
      controller.error(abnormalEndError);
    }
    for (const group of this.textStreamControllers.values()) {
      for (const controller of group.controllers) {
        controller.error(abnormalEndError);
      }
    }
    this.byteStreamControllers.clear();
    this.textStreamControllers.clear();
    this.bufferedPackets = [];
  }
  validateParticipantHasNoActiveDataStreams(participantIdentity) {
    // Terminate any in flight data stream receives from the given participant
    const textStreamsBeingSentByDisconnectingParticipant = Array.from(this.textStreamControllers.entries()).filter(_ref4 => {
      let _ref5 = _slicedToArray(_ref4, 2),
        group = _ref5[1];
      return group.sendingParticipantIdentity === participantIdentity;
    });
    const byteStreamsBeingSentByDisconnectingParticipant = Array.from(this.byteStreamControllers.entries()).filter(entry => entry[1].sendingParticipantIdentity === participantIdentity);
    if (textStreamsBeingSentByDisconnectingParticipant.length > 0 || byteStreamsBeingSentByDisconnectingParticipant.length > 0) {
      const abnormalEndError = new DataStreamError("Participant ".concat(participantIdentity, " unexpectedly disconnected in the middle of sending data"), DataStreamErrorReason.AbnormalEnd);
      for (const _ref6 of byteStreamsBeingSentByDisconnectingParticipant) {
        var _ref7 = _slicedToArray(_ref6, 2);
        const id = _ref7[0];
        const controller = _ref7[1];
        controller.controller.error(abnormalEndError);
        this.byteStreamControllers.delete(id);
      }
      for (const _ref8 of textStreamsBeingSentByDisconnectingParticipant) {
        var _ref9 = _slicedToArray(_ref8, 2);
        const id = _ref9[0];
        const group = _ref9[1];
        for (const controller of group.controllers) {
          controller.error(abnormalEndError);
        }
        this.textStreamControllers.delete(id);
      }
    }
  }
  handleDataStreamPacket(packet, encryptionType) {
    if (!this.isConnected) {
      this.bufferedPackets.push({
        packet,
        encryptionType
      });
      return;
    }
    switch (packet.value.case) {
      case 'streamHeader':
        return this.handleStreamHeader(packet.value.value, packet.participantIdentity, encryptionType);
      case 'streamChunk':
        return this.handleStreamChunk(packet.value.value, encryptionType);
      case 'streamTrailer':
        return this.handleStreamTrailer(packet.value.value, encryptionType);
      default:
        throw new Error("DataPacket of value \"".concat(packet.value.case, "\" is not data stream related!"));
    }
  }
  /** Returns true if the given participant already has the maximum number of streams open. */
  hasTooManyOpenStreams(participantIdentity) {
    let openStreamCount = 0;
    for (const controllers of [this.byteStreamControllers, this.textStreamControllers]) {
      for (const _ref0 of controllers.values()) {
        const sendingParticipantIdentity = _ref0.sendingParticipantIdentity;
        if (sendingParticipantIdentity === participantIdentity) {
          openStreamCount += 1;
        }
      }
    }
    return openStreamCount >= this.maxOpenStreamsPerParticipant;
  }
  handleStreamHeader(streamHeader, participantIdentity, encryptionType) {
    var _a;
    if (this.hasTooManyOpenStreams(participantIdentity)) {
      this.log.error("Participant ".concat(participantIdentity, " already has ").concat(this.maxOpenStreamsPerParticipant, " data streams open, dropping stream ").concat(streamHeader.streamId));
      return;
    }
    switch (streamHeader.contentHeader.case) {
      case 'byteHeader':
        {
          const streamHandlerCallback = this.byteStreamHandlers.get(streamHeader.topic);
          if (!streamHandlerCallback) {
            this.log.debug('ignoring incoming byte stream due to no handler for topic', streamHeader.topic);
            return;
          }
          let streamController;
          const info = {
            id: streamHeader.streamId,
            name: (_a = streamHeader.contentHeader.value.name) !== null && _a !== void 0 ? _a : 'unknown',
            mimeType: streamHeader.mimeType,
            size: streamHeader.totalLength ? Number(streamHeader.totalLength) : undefined,
            topic: streamHeader.topic,
            timestamp: bigIntToNumber(streamHeader.timestamp),
            attributes: streamHeader.attributes,
            encryptionType
          };
          // Determine if the byte payload needs to be decompressed.
          let compressed;
          switch (streamHeader.compression) {
            case DataStream_CompressionType.DEFLATE_RAW:
              if (!isDeflateRawCompressionSupported()) {
                // NOTE: this shouldn't really ever happen, if this warning is logged then the sender
                // isn't properly abiding by the data streams v2 protocol.
                livekitLogger.warn("Data stream ".concat(streamHeader.streamId, " received with deflate-raw compression, but this browser does not support deflate-raw decompression. Dropping..."));
                return;
              }
              compressed = true;
              break;
            case DataStream_CompressionType.NONE:
              compressed = false;
              break;
            default:
              // NOTE: this shouldn't really ever happen, if this warning is logged then the sender
              // isn't properly abiding by the data streams v2 protocol.
              livekitLogger.warn("Data stream ".concat(streamHeader.streamId, " received with unknown compression type ").concat(streamHeader.compression, ", dropping..."));
              return;
          }
          // Single-packet stream: the entire payload was packaged into the header's `inlineContent`.
          // Synthesize an already-complete stream and skip waiting for chunk/trailer packets.
          const inlineContent = streamHeader.inlineContent;
          if (typeof inlineContent !== 'undefined') {
            // Inline bytes are the raw payload, optionally deflate-raw compressed.
            streamHandlerCallback(new ByteStreamReader(info, createInlineStream(streamHeader.streamId, compressed ? deflateRawDecompress(inlineContent, this.maxPayloadByteLength) : inlineContent), bigIntToNumber(streamHeader.totalLength)), {
              identity: participantIdentity
            });
            return;
          }
          const stream = new ReadableStream({
            start: controller => {
              streamController = controller;
              if (this.byteStreamControllers.has(streamHeader.streamId)) {
                throw new DataStreamError("A data stream read is already in progress for a stream with id ".concat(streamHeader.streamId, "."), DataStreamErrorReason.AlreadyOpened);
              }
              this.byteStreamControllers.set(streamHeader.streamId, {
                info,
                controller: streamController,
                startTime: Date.now(),
                sendingParticipantIdentity: participantIdentity
              });
            },
            cancel: () => {
              var _a;
              // The consumer side of the pipe chain errored (e.g. `ensureOrderedChunks` saw a gap) or
              // was cancelled, which cancels this source stream. Unregister it so later chunks and the
              // trailer for this stream are dropped rather than hitting a closed controller.
              if (((_a = this.byteStreamControllers.get(streamHeader.streamId)) === null || _a === void 0 ? void 0 : _a.controller) === streamController) {
                this.byteStreamControllers.delete(streamHeader.streamId);
              }
            }
          });
          streamHandlerCallback(new ByteStreamReader(info, compressed ? inflateRawByteChunkStream(stream, streamHeader.streamId, this.maxPayloadByteLength) : stream.pipeThrough(ensureOrderedChunks(streamHeader.streamId)),
          // `totalLength` is the pre-compression size, and the reader counts decompressed bytes,
          // so it applies to both paths (mirrors text).
          bigIntToNumber(streamHeader.totalLength)), {
            identity: participantIdentity
          });
          return;
        }
      case 'textHeader':
        {
          // The transcription tap runs alongside the application handler, each with its own reader,
          // so the SDK can rebuild transcription events without taking the reserved topic away from
          // an application that reads it too. `listenerCount` keeps the "nobody wants this stream,
          // drop it" behavior intact when no one has subscribed.
          const streamHandlerCallbacks = [];
          if (streamHeader.topic === TRANSCRIPTION_TOPIC && this.listenerCount('transcriptionStreamArrived') > 0) {
            streamHandlerCallbacks.push((reader, _ref1) => {
              let identity = _ref1.identity;
              this.emit('transcriptionStreamArrived', {
                reader,
                participantIdentity: identity
              });
            });
          }
          const applicationCallback = this.textStreamHandlers.get(streamHeader.topic);
          if (applicationCallback) {
            streamHandlerCallbacks.push(applicationCallback);
          }
          if (streamHandlerCallbacks.length === 0) {
            this.log.debug('ignoring incoming text stream due to no handler for topic', streamHeader.topic);
            return;
          }
          const info = {
            id: streamHeader.streamId,
            mimeType: streamHeader.mimeType,
            size: streamHeader.totalLength ? Number(streamHeader.totalLength) : undefined,
            topic: streamHeader.topic,
            timestamp: Number(streamHeader.timestamp),
            attributes: streamHeader.attributes,
            encryptionType,
            attachedStreamIds: streamHeader.contentHeader.value.attachedStreamIds
          };
          // Determine if the byte payload needs to be decompressed.
          let compressed;
          switch (streamHeader.compression) {
            case DataStream_CompressionType.DEFLATE_RAW:
              if (!isDeflateRawCompressionSupported()) {
                // NOTE: this shouldn't really ever happen, if this warning is logged then the sender
                // isn't properly abiding by the data streams v2 protocol.
                livekitLogger.warn("Data stream ".concat(streamHeader.streamId, " received with deflate-raw compression, but this browser does not support deflate-raw decompression. Dropping..."));
                return;
              }
              compressed = true;
              break;
            case DataStream_CompressionType.NONE:
              compressed = false;
              break;
            default:
              // NOTE: this shouldn't really ever happen, if this warning is logged then the sender
              // isn't properly abiding by the data streams v2 protocol.
              livekitLogger.warn("Data stream ".concat(streamHeader.streamId, " received with unknown compression type ").concat(streamHeader.compression, ", dropping..."));
              return;
          }
          // Single-packet stream: the entire payload was smuggled into the header's `inlineContent`.
          // Synthesize an already-complete stream and skip waiting for chunk/trailer packets.
          const inlineContent = streamHeader.inlineContent;
          if (typeof inlineContent !== 'undefined') {
            // Inline text is the raw UTF-8 payload, optionally deflate-raw compressed. `content` is
            // computed once and shared: when compressed it is a promise, so each inline stream awaits
            // the same decompression rather than repeating it.
            const content = compressed ? deflateRawDecompress(inlineContent, this.maxPayloadByteLength) : inlineContent;
            for (const streamHandlerCallback of streamHandlerCallbacks) {
              streamHandlerCallback(new TextStreamReader(info, createInlineStream(streamHeader.streamId, content), bigIntToNumber(streamHeader.totalLength)), {
                identity: participantIdentity
              });
            }
            return;
          }
          if (this.textStreamControllers.has(streamHeader.streamId)) {
            throw new DataStreamError("A data stream read is already in progress for a stream with id ".concat(streamHeader.streamId, "."), DataStreamErrorReason.AlreadyOpened);
          }
          const group = {
            info,
            sendingParticipantIdentity: participantIdentity,
            controllers: []
          };
          this.textStreamControllers.set(streamHeader.streamId, group);
          for (const streamHandlerCallback of streamHandlerCallbacks) {
            let streamController;
            const stream = new ReadableStream({
              start: controller => {
                streamController = controller;
                group.controllers.push(controller);
              },
              cancel: () => {
                // This consumer's side of the pipe chain errored (e.g. `ensureOrderedChunks` saw a
                // gap) or was cancelled, which cancels this source stream. Drop its controller so later
                // chunks and the trailer skip it rather than hitting a closed controller, and
                // unregister the stream once no consumers remain.
                const index = group.controllers.indexOf(streamController);
                if (index !== -1) {
                  group.controllers.splice(index, 1);
                }
                if (group.controllers.length === 0 && this.textStreamControllers.get(streamHeader.streamId) === group) {
                  this.textStreamControllers.delete(streamHeader.streamId);
                }
              }
            });
            streamHandlerCallback(new TextStreamReader(info, compressed ? inflateRawChunkStream(stream, streamHeader.streamId, this.maxPayloadByteLength) : stream.pipeThrough(ensureOrderedChunks(streamHeader.streamId)),
            // `totalLength` is the pre-compression size, and the reader sees decompressed bytes,
            // so it applies to both paths.
            bigIntToNumber(streamHeader.totalLength)), {
              identity: participantIdentity
            });
          }
          return;
        }
    }
  }
  handleStreamChunk(chunk, encryptionType) {
    const fileBuffer = this.byteStreamControllers.get(chunk.streamId);
    if (fileBuffer) {
      if (fileBuffer.info.encryptionType !== encryptionType) {
        fileBuffer.controller.error(new DataStreamError("Encryption type mismatch for stream ".concat(chunk.streamId, ". Expected ").concat(encryptionType, ", got ").concat(fileBuffer.info.encryptionType), DataStreamErrorReason.EncryptionTypeMismatch));
        this.byteStreamControllers.delete(chunk.streamId);
      } else {
        fileBuffer.controller.enqueue(chunk);
      }
    }
    const textGroup = this.textStreamControllers.get(chunk.streamId);
    if (textGroup) {
      if (textGroup.info.encryptionType !== encryptionType) {
        const error = new DataStreamError("Encryption type mismatch for stream ".concat(chunk.streamId, ". Expected ").concat(encryptionType, ", got ").concat(textGroup.info.encryptionType), DataStreamErrorReason.EncryptionTypeMismatch);
        for (const controller of textGroup.controllers) {
          controller.error(error);
        }
        this.textStreamControllers.delete(chunk.streamId);
      } else {
        for (const controller of textGroup.controllers) {
          controller.enqueue(chunk);
        }
      }
    }
  }
  handleStreamTrailer(trailer, encryptionType) {
    const textGroup = this.textStreamControllers.get(trailer.streamId);
    if (textGroup) {
      if (textGroup.info.encryptionType !== encryptionType) {
        const error = new DataStreamError("Encryption type mismatch for stream ".concat(trailer.streamId, ". Expected ").concat(encryptionType, ", got ").concat(textGroup.info.encryptionType), DataStreamErrorReason.EncryptionTypeMismatch);
        for (const controller of textGroup.controllers) {
          controller.error(error);
        }
      } else {
        // One `info` per stream, so this merge is visible to every consumer's reader.
        textGroup.info.attributes = Object.assign(Object.assign({}, textGroup.info.attributes), trailer.attributes);
        if (trailer.reason) {
          // A non-empty reason marks an abnormal close by the sender (e.g. an aborted send);
          // surface it as an error rather than pretending the stream completed.
          const error = new DataStreamError("Data stream ".concat(trailer.streamId, " closed abnormally: ").concat(trailer.reason), DataStreamErrorReason.AbnormalEnd);
          for (const controller of textGroup.controllers) {
            controller.error(error);
          }
        } else {
          for (const controller of textGroup.controllers) {
            controller.close();
          }
        }
      }
      this.textStreamControllers.delete(trailer.streamId);
    }
    const fileBuffer = this.byteStreamControllers.get(trailer.streamId);
    if (fileBuffer) {
      if (fileBuffer.info.encryptionType !== encryptionType) {
        fileBuffer.controller.error(new DataStreamError("Encryption type mismatch for stream ".concat(trailer.streamId, ". Expected ").concat(encryptionType, ", got ").concat(fileBuffer.info.encryptionType), DataStreamErrorReason.EncryptionTypeMismatch));
      } else {
        fileBuffer.info.attributes = Object.assign(Object.assign({}, fileBuffer.info.attributes), trailer.attributes);
        if (trailer.reason) {
          // A non-empty reason marks an abnormal close by the sender (e.g. an aborted send);
          // surface it as an error rather than pretending the stream completed.
          fileBuffer.controller.error(new DataStreamError("Data stream ".concat(trailer.streamId, " closed abnormally: ").concat(trailer.reason), DataStreamErrorReason.AbnormalEnd));
        } else {
          fileBuffer.controller.close();
        }
      }
      this.byteStreamControllers.delete(trailer.streamId);
    }
  }
}
/**
 * Builds a `ReadableStream` that yields the given content as a single chunk and then immediately
 * closes - used to surface an inline (single-packet) data stream as a fully-formed stream. `content`
 * may be a promise (e.g. async gzip decompression); a rejection errors the stream.
 */
function createInlineStream(streamId, content) {
  return new ReadableStream({
    start: controller => __awaiter(this, void 0, void 0, function* () {
      const bytes = yield content;
      controller.enqueue(new DataStream_Chunk({
        streamId,
        chunkIndex: BigInt(0),
        content: bytes
      }));
      controller.close();
    })
  });
}
/**
 * Validates that chunks are received in order, dropping duplicates and throwing if gaps are found.
 *
 * Reassembly (and, for compressed streams, a stateful decompressor) silently corrupts on duplicated
 * or out-of-order input, so duplicates are dropped (with a warning - in-order delivery is expected
 * on the reliable channel, but reconnect handling may replay) and a gap is a hard error. Empty
 * chunks consume their index and are then dropped, so they never reach the reader. Applied to every
 * chunked stream, compressed or not.
 */
function ensureOrderedChunks(streamId) {
  let lastChunkIndex = -1;
  return new TransformStream({
    transform: (value, controller) => {
      const index = bigIntToNumber(value.chunkIndex);
      if (index <= lastChunkIndex) {
        livekitLogger.warn("ignoring duplicate chunk ".concat(index, " ").concat(value.version > 0 ? "(version ".concat(value.version, ")") : '', " for data stream ").concat(streamId, " (last processed: ").concat(lastChunkIndex, ")"));
        return;
      }
      if (index > lastChunkIndex + 1) {
        throw new DataStreamError("Missing chunk(s) ".concat(lastChunkIndex + 1, "..").concat(index - 1, " for data stream ").concat(streamId, " - cannot reassemble payload"), DataStreamErrorReason.Incomplete);
      }
      lastChunkIndex = index;
      if (value.content.length === 0) {
        return;
      }
      controller.enqueue(value);
    }
  });
}
/** Unwraps compressed `DataStream_Chunk`s to their compressed bytes (in `chunkIndex` order), */
function chunksToBytes() {
  return new TransformStream({
    transform: (value, controller) => {
      controller.enqueue(value.content);
    }
  });
}
/** Re-wraps decompressed bytes into contiguous `DataStream_Chunk`s, skipping inflate's empty reads. */
function bytesToChunks(streamId) {
  let outIndex = 0;
  return new TransformStream({
    transform: (value, controller) => {
      // Inflate can emit empty reads; only synthesize a chunk when there is content.
      if (value.byteLength > 0) {
        controller.enqueue(new DataStream_Chunk({
          streamId,
          chunkIndex: numberToBigInt(outIndex),
          content: value
        }));
        outIndex += 1;
      }
    }
  });
}
/**
 * Reframes decompressed byte chunks onto UTF-8 character boundaries via a streaming `TextDecoder`
 * (a write larger than the MTU spans several packets, which may split a codepoint), so each
 * synthesized text chunk decodes independently. The `flush` emits the decoder's trailing bytes.
 */
function bytesToDecodedUtf8(streamId) {
  const decoder = new TextDecoder('utf-8');
  const encoder = new TextEncoder();
  let outIndex = 0;
  const decodeOrThrow = bytes => {
    try {
      return bytes ? decoder.decode(bytes, {
        stream: true
      }) : decoder.decode();
    } catch (err) {
      throw new DataStreamError("Cannot decode compressed data stream ".concat(streamId, " as text: ").concat(err), DataStreamErrorReason.DecodeFailed);
    }
  };
  return new TransformStream({
    transform: (value, controller) => {
      const text = decodeOrThrow(value);
      // Everything so far may have been a partial codepoint; only emit once we have characters.
      if (text.length > 0) {
        controller.enqueue(new DataStream_Chunk({
          streamId,
          chunkIndex: numberToBigInt(outIndex),
          content: encoder.encode(text)
        }));
        outIndex += 1;
      }
    },
    flush: controller => {
      const tail = decodeOrThrow();
      if (tail.length > 0) {
        controller.enqueue(new DataStream_Chunk({
          streamId,
          chunkIndex: numberToBigInt(outIndex),
          content: encoder.encode(tail)
        }));
        outIndex += 1;
      }
    }
  });
}
/**
 * Transforms a stream of deflate-raw-compressed byte `DataStream_Chunk`s into a stream of
 * decompressed chunks, so `ByteStreamReader` consumes it unchanged. All chunk contents are fed (in
 * `chunkIndex` order) through ONE raw-deflate decompressor for the stream's lifetime; decompressed
 * output is re-wrapped as chunks as soon as it is produced. The sender (sendFile) compresses the
 * whole payload in one shot, but the format also supports a single context-takeover stream
 * sync-flushed at write boundaries, so a future incremental streamBytes could compress with no
 * protocol change. Errors and cancellation propagate through the pipe chain.
 */
function inflateRawByteChunkStream(raw, streamId, maxPayloadByteLength) {
  return raw.pipeThrough(ensureOrderedChunks(streamId)).pipeThrough(chunksToBytes()).pipeThrough(inflateRawTransform()).pipeThrough(maxDecompressedLengthGuard(streamId, maxPayloadByteLength)).pipeThrough(bytesToChunks(streamId));
}
/**
 * Transforms a stream of deflate-raw-compressed text `DataStream_Chunk`s into a stream of
 * decompressed chunks, so `TextStreamReader` consumes it unchanged. Builds on
 * {@link inflateRawByteChunkStream} (single decompressor + ordering guard) and adds a streaming
 * `TextDecoder` that reframes the decompressed bytes on UTF-8 character boundaries so each
 * synthesized chunk decodes independently. Errors and cancellation propagate through the pipe chain.
 */
function inflateRawChunkStream(raw, streamId, maxPayloadByteLength) {
  return raw.pipeThrough(ensureOrderedChunks(streamId)).pipeThrough(chunksToBytes()).pipeThrough(inflateRawTransform()).pipeThrough(maxDecompressedLengthGuard(streamId, maxPayloadByteLength)).pipeThrough(bytesToDecodedUtf8(streamId));
}
/**
 * Caps the total decompressed byte count of a compressed stream (decompression-bomb guard): a
 * tiny compressed payload can inflate to an arbitrarily large output, so the decompressor's
 * output is bounded rather than trusting the wire size. Exceeding the cap errors the stream with
 * `PayloadTooLarge`.
 */
function maxDecompressedLengthGuard(streamId, maxByteLength) {
  let total = 0;
  return new TransformStream({
    transform: (value, controller) => {
      total += value.byteLength;
      if (total > maxByteLength) {
        throw new DataStreamError("Data stream ".concat(streamId, " exceeds the maximum payload size of ").concat(maxByteLength, " bytes"), DataStreamErrorReason.PayloadTooLarge);
      }
      controller.enqueue(value);
    }
  });
}export{IncomingDataStreamManager as default};//# sourceMappingURL=IncomingDataStreamManager.mjs.map
