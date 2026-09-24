(ns adam.replica.jsonl
  (:require ["node:crypto" :refer [createHash]]
            ["node:fs" :refer [closeSync openSync readSync statSync]]))

(def ^:private chunk-bytes (* 64 1024))

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

(defn scan-file
  ([path]
   (scan-file path {}))
  ([path {:keys [on-entry]}]
   (let [before (statSync path)
         log-hash (createHash "sha256")
         prefix-hash (createHash "sha256")
         header (atom nil)
         header-json (atom nil)
         header-next-byte-offset (atom nil)
         header-prefix-hash (atom nil)
         byte-offset (atom 0)
         line-number (atom 0)
         entry-count (atom 0)
         last-entry-id (atom nil)
         largest-entry-bytes (atom 0)
         session-name (atom nil)
         seen-entry-ids (js/Set.)
         has-final-newline? (atom false)
         process-line!
         (fn [bytes has-newline?]
           (swap! line-number inc)
           (.update prefix-hash bytes)
           (when has-newline? (.update prefix-hash "\n"))
           (let [next-byte-offset (+ @byte-offset (.-length bytes) (if has-newline? 1 0))]
             (if (zero? (.-length bytes))
               (reset! byte-offset next-byte-offset)
               (let [raw-json (.toString bytes "utf8")
                     value (parse-object raw-json path @line-number)]
                 (if-not @header
                   (do
                     (when-not (= "session" (aget value "type"))
                       (throw (session-error path @line-number "first line must be a session header")))
                     (required-string (aget value "id") "session id" path @line-number)
                     (reset! header value)
                     (reset! header-json raw-json)
                     (reset! header-next-byte-offset next-byte-offset)
                     (reset! header-prefix-hash (digest-copy prefix-hash)))
                   (do
                     (when (= "session" (aget value "type"))
                       (throw (session-error path @line-number "duplicate session header")))
                     (let [entry-id (required-string (aget value "id") "entry id" path @line-number)
                           parent-present? (js/Object.hasOwn value "parentId")
                           parent-id (aget value "parentId")]
                       (when (.has seen-entry-ids entry-id)
                         (throw (session-error path @line-number (str "duplicate entry id " entry-id))))
                       (when-not (and parent-present?
                                      (or (nil? parent-id) (string? parent-id)))
                         (throw (session-error path @line-number "missing or invalid parentId")))
                       (when (and (string? parent-id) (not (.has seen-entry-ids parent-id)))
                         (throw (session-error path @line-number (str "unresolved parentId " parent-id))))
                       (let [type (required-string (aget value "type") "entry type" path @line-number)
                             message (aget value "message")
                             role (when (json-object? message) (optional-string (aget message "role")))
                             payload-bytes (.-length bytes)
                             entry (cond-> {:entry-id entry-id
                                            :type type
                                            :parent-id parent-id
                                            :ordinal @entry-count
                                            :byte-offset @byte-offset
                                            :next-byte-offset next-byte-offset
                                            :raw-json raw-json
                                            :payload-hash (-> (createHash "sha256")
                                                              (.update bytes)
                                                              (.digest "hex"))
                                            :payload-bytes payload-bytes
                                            :prefix-hash (digest-copy prefix-hash)}
                                     role (assoc :role role)
                                     (string? (aget value "timestamp"))
                                     (assoc :timestamp (aget value "timestamp")))]
                         (.add seen-entry-ids entry-id)
                         (reset! last-entry-id entry-id)
                         (swap! largest-entry-bytes max payload-bytes)
                         (when (= "session_info" type)
                           (reset! session-name (optional-string (aget value "name"))))
                         (when on-entry (on-entry entry))
                         (swap! entry-count inc)))))
                 (reset! byte-offset next-byte-offset)))))
         file-descriptor (openSync path "r")]
     (try
       (loop [pending (js/Buffer.alloc 0)]
         (let [buffer (js/Buffer.allocUnsafe chunk-bytes)
               bytes-read (readSync file-descriptor buffer 0 chunk-bytes nil)]
           (if (zero? bytes-read)
             (when (pos? (.-length pending))
               (process-line! pending false))
             (let [chunk (.subarray buffer 0 bytes-read)
                   combined (if (zero? (.-length pending))
                              chunk
                              (js/Buffer.concat #js [pending chunk]))]
               (.update log-hash chunk)
               (reset! has-final-newline? (= 10 (aget chunk (dec bytes-read))))
               (let [remaining
                     (loop [start 0]
                       (let [newline-index (.indexOf combined 10 start)]
                         (if (= -1 newline-index)
                           (.subarray combined start)
                           (do
                             (process-line! (.subarray combined start newline-index) true)
                             (recur (inc newline-index))))))]
                 (recur remaining))))))
       (finally
         (closeSync file-descriptor)))
     (when-not @header
       (throw (session-error path "missing session header")))
     (let [after (statSync path)]
       (when (or (not= (.-size before) (.-size after))
                 (not= (.-mtimeMs before) (.-mtimeMs after)))
         (throw (session-error path "file changed while it was being scanned")))
       (let [header-value @header]
         (cond-> {:path path
                  :pi-session-id (required-string (aget header-value "id") "session id" path 1)
                  :header-json @header-json
                  :header-next-byte-offset @header-next-byte-offset
                  :header-prefix-hash @header-prefix-hash
                  :entry-count @entry-count
                  :log-hash (.digest log-hash "hex")
                  :log-bytes (.-size after)
                  :largest-entry-bytes @largest-entry-bytes
                  :has-final-newline? @has-final-newline?}
           (string? (aget header-value "cwd")) (assoc :cwd (aget header-value "cwd"))
           (number? (aget header-value "version")) (assoc :version (aget header-value "version"))
           (string? (aget header-value "timestamp")) (assoc :created-at (aget header-value "timestamp"))
           (string? (aget header-value "parentSession"))
           (assoc :parent-session (aget header-value "parentSession"))
           @session-name (assoc :name @session-name)
           @last-entry-id (assoc :last-entry-id @last-entry-id)))))))
