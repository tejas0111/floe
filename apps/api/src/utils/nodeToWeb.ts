import { Readable } from "stream";
import { ReadableStream } from "stream/web";

export function nodeToWeb(stream: Readable): ReadableStream {
  return new ReadableStream({
    start(controller) {
      stream.on("data", chunk => controller.enqueue(chunk));
      stream.on("end", () => controller.close());
      stream.on("error", err => controller.error(err));
    },
  });
}

