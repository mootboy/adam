(ns adam.replica.jsonl
  (:require ["node:crypto" :refer [createHash]]
            ["node:fs/promises" :as async-fs]
            ["node:fs" :refer [closeSync openSync readSync statSync]]))

(def ^:private chunk-bytes (* 64 1024))
(def ^:private max-session-header-bytes (* 1024 1024))

(defn- session-error
  ([path message]
   (session-error path nil message))
  ([path line-number message]
   (let [location (if line-number (str path ":" line-number) path)]
     (doto (js/Error. (str location ": " message))
       (aset "name" "SessionJsonlError")
       (aset "path" path)
       (aset "lineNumber" line-number)))))

(defn- json-object? [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (js/Array.isArray value))))

(defn- parse-object [raw-json path line-number]
  (let [value (try
                (js/JSON.parse raw-json)
                (catch :default _
                  (throw (session-error path line-number "invalid JSON"))))]
    (when-not (json-object? value)
      (throw (session-error path line-number "line must contain a JSON object")))
    value))

(defn- required-string [value field path line-number]
  (when-not (and (string? value) (not= "" value))
    (throw (session-error path line-number (str "missing or invalid " field))))
  value)

(defn- optional-string [value]
  (when (string? value) value))

(defn- digest-copy [^js hash]
  (-> (.copy hash) (.digest "hex")))

(defn read-session-header [path]
  (let [file-descriptor (openSync path "r")]
    (try
      (let [header-bytes
            (loop [chunks []
                   total-bytes 0]
              (when (> total-bytes max-session-header-bytes)
                (throw (session-error path 1
                                      (str "session header exceeds "
                                           max-session-header-bytes " bytes"))))
              (let [buffer-size (min chunk-bytes
                                     (inc (- max-session-header-bytes total-bytes)))
                    buffer (js/Buffer.allocUnsafe buffer-size)
                    bytes-read (readSync file-descriptor buffer 0 buffer-size nil)]
                (if (zero? bytes-read)
                  (js/Buffer.concat (clj->js chunks) total-bytes)
                  (let [bytes (.subarray buffer 0 bytes-read)
                        newline-index (.indexOf bytes 10)]
                    (if (neg? newline-index)
                      (recur (conj chunks bytes) (+ total-bytes bytes-read))
                      (js/Buffer.concat
                       (clj->js (conj chunks (.subarray bytes 0 newline-index)))
                       (+ total-bytes newline-index)))))))
            header-json (.toString header-bytes "utf8")]
        (when (= "" header-json)
          (throw (session-error path "missing session header")))
        (let [header (parse-object header-json path 1)]
          (when-not (= "session" (aget header "type"))
            (throw (session-error path 1 "first line must be a session header")))
          (cond-> {:pi-session-id (required-string (aget header "id") "session id" path 1)
                   :header-json header-json}
            (string? (aget header "cwd")) (assoc :cwd (aget header "cwd"))
            (string? (aget header "parentSession"))
            (assoc :parent-session (aget header "parentSession")))))
      (finally
        (closeSync file-descriptor)))))

(defn read-header-bytes!
  "Best-effort bounded async parent locator read; interpretation stays pure."
  [path]
  (-> (.open async-fs path "r")
      (.then
        (fn [handle]
          (letfn [(read-next [position parts]
                    (when (>= position max-session-header-bytes)
                      (throw (session-error path "session header exceeds byte limit")))
                    (let [buffer (js/Buffer.allocUnsafe
                                   (min chunk-bytes (- max-session-header-bytes position)))]
                      (-> (.read handle buffer 0 (.-length buffer) position)
                          (.then
                            (fn [^js result]
                              (let [n (.-bytesRead result) chunk (.subarray buffer 0 n)
                                    end (.indexOf chunk 10)]
                                (cond
                                  (not (neg? end))
                                  (js/Buffer.concat (to-array (conj parts (.subarray chunk 0 end))))
                                  (zero? n) (js/Buffer.concat (to-array parts))
                                  :else (read-next (+ position n) (conj parts chunk)))))))))]
            (-> (.then (js/Promise.resolve nil) (fn [_] (read-next 0 [])))
                (.finally (fn [] (.close handle)))))))
      (.catch (fn [_] nil))))

(defn read-records!
  "Asynchronous, backpressured physical-record reader. consume! is awaited for
   every batch; JSON decoding/hashing belong to the processor, not this reader.
   Retains at most a read chunk plus one spanning record before delivery."
  [path consume!]
  (-> (.open async-fs path "r")
      (.then
        (fn [handle]
          (-> (.stat handle)
              (.then
                (fn [^js before]
                  (letfn [(read-next [position parts]
                            (if (< position (.-size before))
                              (let [buffer (js/Buffer.allocUnsafe
                                             (min chunk-bytes (- (.-size before) position)))]
                                (-> (.read handle buffer 0 (.-length buffer) position)
                                    (.then
                                      (fn [^js result]
                                        (let [n (.-bytesRead result)]
                                          (when (zero? n)
                                            (throw (session-error path "file changed while it was being scanned")))
                                          (let [chunk (.subarray buffer 0 n)
                                                framed
                                                (loop [start 0 parts parts records []]
                                                  (let [end (.indexOf chunk 10 start)]
                                                    (if (neg? end)
                                                      {:parts (cond-> parts
                                                                (< start n) (conj (.subarray chunk start)))
                                                       :records records}
                                                      (let [line (.subarray chunk start end)
                                                            bytes (if (seq parts)
                                                                    (js/Buffer.concat (to-array (conj parts line)))
                                                                    line)]
                                                        (recur (inc end) []
                                                               (conj records {:bytes bytes :newline? true}))))))]
                                            (-> (.then (js/Promise.resolve nil)
                                                       (fn [_]
                                                         (when (seq (:records framed))
                                                           (consume! (:records framed)))))
                                                (.then (fn [_]
                                                         (read-next (+ position n) (:parts framed)))))))))))
                              (-> (.then (js/Promise.resolve nil)
                                         (fn [_]
                                           (when (seq parts)
                                             (consume! [{:bytes (js/Buffer.concat (to-array parts))
                                                         :newline? false}]))))
                                  (.then (fn [_] (.stat async-fs path)))
                                  (.then
                                    (fn [^js after]
                                      (when (or (not= (.-size before) (.-size after))
                                                (not= (.-mtimeMs before) (.-mtimeMs after))
                                                (not= (.-ino before) (.-ino after))
                                                (not= (.-dev before) (.-dev after)))
                                        (throw (session-error path "file changed while it was being scanned")))
                                      {:bytes (.-size after)})))))]
                    (read-next 0 []))))
              (.finally (fn [] (.close handle))))))))

(defn scan-record
  "Pure fold of one physical record. Input hash/state is never mutated.
   The caller owns framing and filesystem stability checks."
  [path state {:keys [bytes newline?]}]
  (let [bytes (js/Buffer.from bytes)
        hash (if-let [previous (:hash state)] (.copy ^js previous) (createHash "sha256"))
        _ (.update hash bytes)
        _ (when newline? (.update hash "\n"))
        offset (or (:offset state) 0)
        line (inc (or (:line state) 0))
        end (+ offset (.-length bytes) (if newline? 1 0))
        next-state (assoc state :hash hash :offset end :line line :newline? newline?)]
    (if (zero? (.-length bytes))
      next-state
      (let [raw (.toString bytes "utf8")
            value (parse-object raw path line)]
        (if-not (:header state)
          (do
            (when-not (= "session" (aget value "type"))
              (throw (session-error path line "first line must be a session header")))
            (required-string (aget value "id") "session id" path line)
            (assoc next-state :header value :header-json raw
                   :header-offset end :header-hash (digest-copy hash)))
          (let [id (required-string (aget value "id") "entry id" path line)
                parent (aget value "parentId")
                seen (or (:seen state) #{})
                type (required-string (aget value "type") "entry type" path line)]
            (when (= "session" type)
              (throw (session-error path line "duplicate session header")))
            (when (contains? seen id)
              (throw (session-error path line (str "duplicate entry id " id))))
            (when-not (and (js/Object.hasOwn value "parentId")
                           (or (nil? parent) (string? parent)))
              (throw (session-error path line "missing or invalid parentId")))
            (when (and (string? parent) (not (contains? seen parent)))
              (throw (session-error path line (str "unresolved parentId " parent))))
            (let [message (aget value "message")
                  entry (cond-> {:entry-id id :type type :parent-id parent
                                 :ordinal (or (:entry-count state) 0)
                                 :byte-offset offset :next-byte-offset end
                                 :raw-json raw :payload-bytes (.-length bytes)
                                 :payload-hash (-> (createHash "sha256") (.update bytes) (.digest "hex"))
                                 :prefix-hash (digest-copy hash)}
                          (and (json-object? message) (string? (aget message "role")))
                          (assoc :role (aget message "role"))
                          (string? (aget value "timestamp")) (assoc :timestamp (aget value "timestamp")))]
              (cond-> (assoc next-state :seen (conj seen id)
                             :entries (conj (or (:entries state) []) entry)
                             :entry-count (inc (or (:entry-count state) 0)) :last-entry-id id
                             :largest (max (or (:largest state) 0) (.-length bytes)))
                (= "session_info" type) (assoc :name (optional-string (aget value "name")))))))))))

(defn scan-summary [path state]
  (when-not (:header state)
    (throw (session-error path "missing session header")))
  (let [header (:header state)]
    (cond-> {:path path :pi-session-id (aget header "id")
             :header-json (:header-json state)
             :header-next-byte-offset (:header-offset state)
             :header-prefix-hash (:header-hash state)
             :entry-count (or (:entry-count state) 0)
             :log-hash (digest-copy (:hash state))
             :log-bytes (:offset state)
             :largest-entry-bytes (or (:largest state) 0)
             :has-final-newline? (boolean (:newline? state))}
      (string? (aget header "cwd")) (assoc :cwd (aget header "cwd"))
      (number? (aget header "version")) (assoc :version (aget header "version"))
      (string? (aget header "timestamp")) (assoc :created-at (aget header "timestamp"))
      (string? (aget header "parentSession")) (assoc :parent-session (aget header "parentSession"))
      (:name state) (assoc :name (:name state))
      (:last-entry-id state) (assoc :last-entry-id (:last-entry-id state)))))

(defn scan-file
  "Synchronous scanner API; validation is shared
   with the input-only processor. Raw records are not retained by this reader."
  ([path] (scan-file path {}))
  ([path {:keys [on-entry]}]
   (let [before (statSync path) state (atom nil)
         process-line!
         (fn [bytes newline?]
           (let [previous @state
                 next-state (scan-record path previous {:bytes bytes :newline? newline?})]
             (when (and on-entry (> (or (:entry-count next-state) 0)
                                   (or (:entry-count previous) 0)))
               (on-entry (peek (:entries next-state))))
             (reset! state (dissoc next-state :entries))))
         fd (openSync path "r")]
     (try
       (loop [pending (js/Buffer.alloc 0)]
         (let [buffer (js/Buffer.allocUnsafe chunk-bytes)
               n (readSync fd buffer 0 chunk-bytes nil)]
           (if (zero? n)
             (when (pos? (.-length pending)) (process-line! pending false))
             (let [chunk (.subarray buffer 0 n)
                   combined (if (zero? (.-length pending)) chunk (js/Buffer.concat #js [pending chunk]))
                   remaining
                   (loop [start 0]
                     (let [end (.indexOf combined 10 start)]
                       (if (neg? end)
                         (.subarray combined start)
                         (do (process-line! (.subarray combined start end) true)
                             (recur (inc end))))))]
               (recur remaining)))))
       (finally (closeSync fd)))
     (let [after (statSync path)]
       (when (or (not= (.-size before) (.-size after))
                 (not= (.-mtimeMs before) (.-mtimeMs after)))
         (throw (session-error path "file changed while it was being scanned")))
       (scan-summary path @state)))))
