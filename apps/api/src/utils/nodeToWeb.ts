import { Readable } from "stream";
import { ReadableStream } from "stream/web";

export function nodeToWeb(stream: Readable): ReadableStream {
  // Node 20+ provides a native bridge that preserves backpressure and cancelation.
  return Readable.toWeb(stream) as ReadableStream;
}
