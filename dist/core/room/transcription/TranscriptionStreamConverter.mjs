import {__awaiter,__asyncValues}from'tslib';import {Transcription,TranscriptionSegment}from'@livekit/protocol';import livekitLogger from'../../logger.mjs';import {ParticipantAgentAttributes}from'../participant/attributes.mjs';/**
 * Rebuilds legacy-shaped `Transcription` messages from `lk.transcription` text streams, so
 * transcription events keep flowing once agents stop publishing the legacy `Transcription` data
 * packet (client protocol 3).
 *
 * Two stream shapes arrive on this topic and they accumulate differently:
 *
 * - **Delta** (agent speech): one stream per segment, each chunk an increment, finality on the
 *   closing trailer.
 * - **Non-delta** (user speech-to-text): a fresh stream per update, all sharing one
 *   `lk.segment_id`, each carrying the full text, finality on the header.
 *
 * So a new stream id for a segment already in flight *replaces* the accumulated text, and
 * `lk.transcription_final` is trusted whenever present - a stream close only implies finality when
 * the attribute is missing entirely. Treating every close as final would wrongly finalize each
 * interim user transcript.
 */
class TranscriptionStreamConverter {
  constructor(options) {
    this.log = livekitLogger;
    /** In-flight segments, keyed by `partialKey(senderIdentity, segmentId)`. */
    this.partials = new Map();
    this.handleTextStream = (reader, senderIdentity) => __awaiter(this, void 0, void 0, function* () {
      var _a, reader_1, reader_1_1;
      var _b, e_1, _c, _d;
      var _e, _f, _g;
      const segmentId = ((_e = reader.info.attributes) === null || _e === void 0 ? void 0 : _e[ParticipantAgentAttributes.TranscriptionSegmentId]) || reader.info.id;
      const key = partialKey(senderIdentity, segmentId);
      const existing = this.partials.get(key);
      if (!existing || existing.streamId !== reader.info.id) {
        this.partials.set(key, {
          streamId: reader.info.id,
          text: ''
        });
      }
      // Chunk boundaries follow the transport's packet splitting, not the sender's writes, so
      // payload decoding has to carry state across chunks for the lifetime of this stream.
      const decoder = new TranscriptStreamDecoder();
      let final;
      try {
        try {
          for (_a = true, reader_1 = __asyncValues(reader); reader_1_1 = yield reader_1.next(), _b = reader_1_1.done, !_b; _a = true) {
            _d = reader_1_1.value;
            _a = false;
            const chunk = _d;
            const partial = this.partials.get(key);
            if (!partial || partial.streamId !== reader.info.id) {
              // A newer stream for this segment took over while this one was still draining.
              return;
            }
            partial.text += decoder.push(chunk);
            this.emitSegment(partial, segmentId, senderIdentity, reader, (_f = this.isFinal(reader)) !== null && _f !== void 0 ? _f : false);
          }
        } catch (e_1_1) {
          e_1 = {
            error: e_1_1
          };
        } finally {
          try {
            if (!_a && !_b && (_c = reader_1.return)) yield _c.call(reader_1);
          } finally {
            if (e_1) throw e_1.error;
          }
        }
        final = (_g = this.isFinal(reader)) !== null && _g !== void 0 ? _g : true;
      } catch (err) {
        // The stream ended abnormally (sender disconnected mid-segment, decode failure). Whatever
        // was accumulated is all there will ever be, so close the segment out.
        this.log.debug('lk.transcription stream ended abnormally', err);
        final = true;
      }
      this.closeSegment(key, segmentId, senderIdentity, reader, final, decoder.flush());
    });
    this.options = options;
  }
  /** Drops all in-flight segment state. */
  reset() {
    this.partials.clear();
  }
  closeSegment(key, segmentId, senderIdentity, reader, final, trailingText) {
    const partial = this.partials.get(key);
    if (!partial || partial.streamId !== reader.info.id) {
      return;
    }
    partial.text += trailingText;
    this.emitSegment(partial, segmentId, senderIdentity, reader, final);
    if (final) {
      this.partials.delete(key);
    }
  }
  /**
   * Works out which participant and track a transcription should be attributed to, matching what
   * the legacy channel reported.
   *
   * In an avatar session the legacy channel named the *delegating publisher* (the avatar worker)
   * rather than the agent it speaks for, and the stream's `lk.transcribed_track_id` is absent
   * because the agent publishes no microphone track of its own. Preferring the worker keeps the
   * identity stable across the cutover and lets `CoreRoom.handleTranscription` resolve a publication.
   */
  resolveSpeaker(senderIdentity, reader) {
    var _a;
    // `||` rather than `??` throughout: a resolver that reports "nothing found" as an empty string
    // rather than undefined should fall through to the next candidate, not pin the result to ''.
    // An empty identity resolves no participant, and an empty track sid resolves no publication,
    // so treating either as a real answer silently stops the transcription events from firing.
    const identity = this.options.getDelegatingPublisherIdentity(senderIdentity) || senderIdentity;
    const trackId = ((_a = reader.info.attributes) === null || _a === void 0 ? void 0 : _a[ParticipantAgentAttributes.TranscribedTrackId]) || this.options.getMicrophoneTrackSid(identity) || this.options.getMicrophoneTrackSid(senderIdentity) || '';
    return {
      identity,
      trackId
    };
  }
  emitSegment(partial, segmentId, senderIdentity, reader, final) {
    if (typeof partial.emittedText === 'undefined' && partial.text === '') {
      // A stream that carried no content at all - nothing worth surfacing.
      return;
    }
    if (partial.emittedText === partial.text && partial.emittedFinal === final) {
      // Nothing changed since the last emission (e.g. a non-delta stream closing with the same
      // finality its header already declared).
      return;
    }
    partial.emittedText = partial.text;
    partial.emittedFinal = final;
    const _this$resolveSpeaker = this.resolveSpeaker(senderIdentity, reader),
      identity = _this$resolveSpeaker.identity,
      trackId = _this$resolveSpeaker.trackId;
    this.options.onTranscription(new Transcription({
      transcribedParticipantIdentity: identity,
      trackId,
      segments: [new TranscriptionSegment({
        id: segmentId,
        text: partial.text,
        // Agents write zeroes and an empty language into legacy packets, so nothing is lost.
        startTime: BigInt(0),
        endTime: BigInt(0),
        final,
        language: ''
      })]
    }));
  }
  /** `undefined` when the sender declared no finality at all. */
  isFinal(reader) {
    var _a;
    const raw = (_a = reader.info.attributes) === null || _a === void 0 ? void 0 : _a[ParticipantAgentAttributes.TranscriptionFinal];
    if (typeof raw === 'undefined') {
      return undefined;
    }
    // Agents send the string form even though the generated attribute typing calls it a boolean.
    return raw === 'true' || raw === '1' || raw === true;
  }
}
function partialKey(senderIdentity, segmentId) {
  return "".concat(senderIdentity, "/").concat(segmentId);
}
/**
 * Decodes the chunks of a single `lk.transcription` stream into transcript text, unwrapping the
 * payloads of an agent running with `json_format`. That mode wraps every write as a JSON
 * `TimedString` (`{"text": "...", "start_time": 1.5}`) terminated by a newline.
 *
 * There is no wire marker for this mode - no attribute, no distinct mime type - so the stream's
 * first chunk is sniffed: one opening with `{"` puts the whole stream in JSON mode. A plain-text
 * transcript that happens to start that way still comes through intact, since lines that fail to
 * parse as a `TimedString` are passed through verbatim; it just isn't surfaced until each newline
 * (or the stream's close) arrives. The durable fix is a marker attribute on the agent side.
 *
 * A chunk is a transport packet, not a sender write: a `TimedString` larger than one packet arrives
 * split across chunks, and one chunk may hold several. So JSON mode buffers and only decodes
 * complete newline-terminated lines. Whatever is left unterminated when the stream closes (agents
 * write their final markup-stripped tail unencoded) is decoded by `flush`.
 *
 * Only the text is taken. `TimedString` also carries `start_time`/`end_time` as floating point
 * values in an undocumented unit, while the proto segment fields are `uint64` - and the legacy
 * channel always reported zero - so the timings are deliberately dropped rather than guessed at.
 */
class TranscriptStreamDecoder {
  constructor() {
    /** JSON mode only: the trailing, not yet newline-terminated part of the stream. */
    this.buffered = '';
  }
  /** Returns the transcript text the chunk completes, possibly empty. */
  push(chunk) {
    var _a, _b;
    (_a = this.json) !== null && _a !== void 0 ? _a : this.json = chunk.trim() === '' ? undefined : chunk.trim().startsWith('{"');
    if (!this.json) {
      return chunk;
    }
    this.buffered += chunk;
    const lines = this.buffered.split('\n');
    this.buffered = (_b = lines.pop()) !== null && _b !== void 0 ? _b : '';
    return lines.map(line => {
      var _a;
      return (_a = unwrapTimedString(line)) !== null && _a !== void 0 ? _a : "".concat(line, "\n");
    }).join('');
  }
  /** Returns any text still buffered at the end of the stream. */
  flush() {
    var _a;
    const rest = this.buffered;
    this.buffered = '';
    return (_a = unwrapTimedString(rest)) !== null && _a !== void 0 ? _a : rest;
  }
}
/** The `text` of a JSON `TimedString` line, or `undefined` if the line is not one. */
function unwrapTimedString(line) {
  const trimmed = line.trim();
  if (!trimmed.startsWith('{') || !trimmed.endsWith('}')) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(trimmed);
    if (parsed !== null && typeof parsed === 'object' && typeof parsed.text === 'string') {
      return parsed.text;
    }
  } catch (_a) {
    // Not JSON after all - it is ordinary transcript text that happens to be brace-wrapped.
  }
  return undefined;
}export{TranscriptionStreamConverter as default};//# sourceMappingURL=TranscriptionStreamConverter.mjs.map
