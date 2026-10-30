import { start } from "./dist/adam.js";

export default function adam(pi) {
  return start(pi, new URL("./processor.js", import.meta.url).href,
    new URL("./worker.js", import.meta.url).href);
}
