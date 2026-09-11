import { Transcription } from '@livekit/protocol';
import type { TextStreamReader } from '../data-stream/incoming/StreamReader';
export interface TranscriptionStreamConverterOptions {
    /** Invoked with a synthesized `Transcription` for every transcription update. */
    onTranscription: (transcription: Transcription) => void;
    /** Resolves the sid of the microphone track published by `identity`, if it has one. */
    getMicrophoneTrackSid: (identity: string) => string | undefined;
    /**
     * Resolves the identity of a participant whose `lk.publish_on_behalf` attribute names
     * `identity` - an avatar worker speaking for an agent, if one is present.
     */
    getDelegatingPublisherIdentity: (identity: string) => string | undefined;
}
/**
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
export default class TranscriptionStreamConverter {
    private log;
    private options;
    /** In-flight segments, keyed by `partialKey(senderIdentity, segmentId)`. */
    private partials;
    constructor(options: TranscriptionStreamConverterOptions);
    /** Drops all in-flight segment state. */
    reset(): void;
    handleTextStream: (reader: TextStreamReader, senderIdentity: string) => Promise<void>;
    private closeSegment;
    /**
     * Works out which participant and track a transcription should be attributed to, matching what
     * the legacy channel reported.
     *
     * In an avatar session the legacy channel named the *delegating publisher* (the avatar worker)
     * rather than the agent it speaks for, and the stream's `lk.transcribed_track_id` is absent
     * because the agent publishes no microphone track of its own. Preferring the worker keeps the
     * identity stable across the cutover and lets `CoreRoom.handleTranscription` resolve a publication.
     */
    private resolveSpeaker;
    private emitSegment;
    /** `undefined` when the sender declared no finality at all. */
    private isFinal;
}
//# sourceMappingURL=TranscriptionStreamConverter.d.ts.map
