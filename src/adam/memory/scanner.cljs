(ns adam.memory.scanner
  (:require [adam.memory.protocol :as protocol]
            [clojure.string :as string]
            ["node:buffer" :refer [isUtf8]]
            ["node:crypto" :refer [createHash]]
            ["node:fs" :refer [closeSync lstatSync openSync readSync statSync]]))

(def ^:private chunk-bytes (* 64 1024))
(def ^:private max-record-bytes (* 1024 1024))

(defn- sidecar-error [path line-number message reason]
  (let [location (if line-number (str path ":" line-number) path)]
    (doto (ex-info (str location ": " message)
                   {:type :memory-sidecar-error
                    :reason reason
                    :path path
                    :line-number line-number})
      (aset "name" "MemorySidecarError"))))

(defn- digest-copy [^js hash]
  (-> (.copy hash) (.digest "hex")))

(defn- validate-locator! [{:keys [path source-kind source-session-id producer-id]}]
  (when-not (and (string? path) (not (string/blank? path)))
    (throw (js/Error. "memory sidecar path is required")))
  (when-not (protocol/valid-locator?
             {:source-kind source-kind
              :source-session-id source-session-id
              :producer-id producer-id})
    (throw (js/Error. "memory sidecar source and producer identity are invalid")))
  (let [metadata (lstatSync path)]
    (when (.isSymbolicLink metadata)
      (throw (sidecar-error path nil "symbolic links are not allowed" :unsafe-path)))
    (when-not (.isFile metadata)
      (throw (sidecar-error path nil "sidecar must be a regular file" :unsafe-path)))
    (when-not (zero? (bit-and (.-mode metadata) 63))
      (throw (sidecar-error path nil "sidecar must be owner-only" :unsafe-permissions)))))

(defn- parse-record [bytes path line-number locator]
  (when (> (.-length bytes) max-record-bytes)
    (throw (sidecar-error path line-number "record exceeds 1 MiB" :record-too-large)))
  (when-not (isUtf8 bytes)
    (throw (sidecar-error path line-number "record is not valid UTF-8" :invalid-utf8)))
  (let [raw-json (.toString bytes "utf8")
        parsed (try
                 {:value (js/JSON.parse raw-json)}
                 (catch :default _ nil))]
    (if-not parsed
      {:raw-json raw-json
       :semantic-status :skipped
       :diagnostics [:malformed-json]}
      (try
        (merge {:raw-json raw-json :value (:value parsed)}
               (protocol/inspect-event (:value parsed) locator))
        (catch :default _
          {:raw-json raw-json
           :value (:value parsed)
           :semantic-status :skipped
           :diagnostics [:invalid-envelope]})))))

(defn scan-sidecar [{:keys [path] :as locator}]
  (validate-locator! locator)
  (let [before (statSync path)
        prefix-hash (createHash "sha256")
        byte-offset (atom 0)
        line-number (atom 0)
        records (atom [])
        largest-record-bytes (atom 0)
        incomplete-tail-bytes (atom 0)
        event-hashes (atom {})
        accepted-source-checkpoint (atom nil)
        semantic-conflict (atom nil)
        apply-semantics!
        (fn [record]
          (if-not (= :accepted (:semantic-status record))
            record
            (let [event-id (:event-id record)
                  event-hash (:event-hash record)
                  existing-hash (get @event-hashes event-id)]
              (cond
                @semantic-conflict
                (assoc record :semantic-status :blocked
                       :diagnostics [:stream-conflicted])

                (= existing-hash event-hash)
                (assoc record :semantic-status :replay
                       :diagnostics [:idempotent-replay])

                existing-hash
                (do
                  (reset! semantic-conflict :immutable-event-conflict)
                  (assoc record :semantic-status :conflict
                         :diagnostics [:immutable-event-conflict]))

                (and @accepted-source-checkpoint
                     (protocol/checkpoint-regression?
                      @accepted-source-checkpoint (:source-checkpoint record)))
                (do
                  ;; The event is semantically skipped, but its otherwise valid
                  ;; identity is still immutable within this sidecar.
                  (swap! event-hashes assoc event-id event-hash)
                  (assoc record :semantic-status :skipped
                         :diagnostics [:checkpoint-regression]))

                :else
                (do
                  (swap! event-hashes assoc event-id event-hash)
                  (reset! accepted-source-checkpoint (:source-checkpoint record))
                  record)))))
        process-line!
        (fn [bytes]
          (swap! line-number inc)
          (.update prefix-hash bytes)
          (.update prefix-hash "\n")
          (let [next-offset (+ @byte-offset (.-length bytes) 1)
                parsed (parse-record bytes path @line-number locator)
                record (apply-semantics!
                        (merge parsed
                              {:ordinal (count @records)
                               :byte-offset @byte-offset
                               :next-byte-offset next-offset
                               :payload-hash (protocol/sha256 bytes)
                               :payload-bytes (.-length bytes)
                               :prefix-hash (digest-copy prefix-hash)}))]
            (swap! largest-record-bytes max (.-length bytes))
            (swap! records conj record)
            (reset! byte-offset next-offset)))
        file-descriptor (openSync path "r")]
    (try
      (loop [pending (js/Buffer.alloc 0)]
        (let [buffer (js/Buffer.allocUnsafe chunk-bytes)
              bytes-read (readSync file-descriptor buffer 0 chunk-bytes nil)]
          (if (zero? bytes-read)
            (reset! incomplete-tail-bytes (.-length pending))
            (let [chunk (.subarray buffer 0 bytes-read)
                  combined (if (zero? (.-length pending))
                             chunk
                             (js/Buffer.concat #js [pending chunk]))
                  remaining
                  (loop [start 0]
                    (let [newline-index (.indexOf combined 10 start)]
                      (if (= -1 newline-index)
                        (.subarray combined start)
                        (do
                          (process-line! (.subarray combined start newline-index))
                          (recur (inc newline-index))))))]
              (when (> (.-length remaining) max-record-bytes)
                (throw (sidecar-error path (inc @line-number)
                                      "record exceeds 1 MiB" :record-too-large)))
              (recur remaining)))))
      (finally
        (closeSync file-descriptor)))
    (let [after (statSync path)]
      (when (or (not= (.-size before) (.-size after))
                (not= (.-mtimeMs before) (.-mtimeMs after)))
        (throw (sidecar-error path nil "file changed while it was being scanned"
                              :concurrent-change)))
      {:path path
       :source-kind (:source-kind locator)
       :source-session-id (:source-session-id locator)
       :producer-id (:producer-id locator)
       :records @records
       :record-count (count @records)
       :header-next-byte-offset 0
       :header-prefix-hash (protocol/sha256 "")
       :log-hash (digest-copy prefix-hash)
       :log-bytes @byte-offset
       :source-bytes (.-size after)
       :incomplete-tail-bytes @incomplete-tail-bytes
       :largest-record-bytes @largest-record-bytes
       :has-final-newline? (and (pos? (.-size after))
                                (= @byte-offset (.-size after)))
       :semantic-conflict @semantic-conflict})))
