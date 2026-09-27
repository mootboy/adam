(ns adam.sources.claude-code.scanner
  (:require [adam.replica.identity :as identity]
            [clojure.set :as set]
            [clojure.string :as string]
            ["node:crypto" :refer [createHash]]
            ["node:fs" :refer [closeSync openSync readSync statSync]]))

(def ^:private chunk-bytes (* 64 1024))

(defn- transcript-error [path line-number message]
  (let [location (if line-number (str path ":" line-number) path)]
    (doto (js/Error. (str location ": " message))
      (aset "name" "ClaudeTranscriptError")
      (aset "path" path)
      (aset "lineNumber" line-number))))

(defn- json-object? [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (js/Array.isArray value))))

(defn- sha256 [& values]
  (let [hash (createHash "sha256")]
    (doseq [value values] (.update hash value))
    (.digest hash "hex")))

(defn- digest-copy [^js hash]
  (-> (.copy hash) (.digest "hex")))

(defn- deterministic-entry-id [stream-id ordinal raw-json]
  (str "urn:adam:claude-record:"
       (sha256 stream-id "\u0000" (str ordinal) "\u0000" raw-json)))

(defn- parse-record [raw-json path line-number]
  (let [value (try
                (js/JSON.parse raw-json)
                (catch :default _
                  (throw (transcript-error path line-number "invalid JSON"))))]
    (when-not (json-object? value)
      (throw (transcript-error path line-number "line must contain a JSON object")))
    value))

(defn- optional-string [value]
  (when (and (string? value) (not (string/blank? value))) value))

(defn scan-stream
  [{:keys [session-id transcript-path stream-id agent-id on-entry]
    :or {stream-id "main"}}]
  (when-not (and (string? session-id) (not (string/blank? session-id)))
    (throw (js/Error. "Claude session id is required")))
  (when-not (and (string? transcript-path) (not (string/blank? transcript-path)))
    (throw (js/Error. "Claude transcript path is required")))
  (let [before (statSync transcript-path)
        prefix-hash (createHash "sha256")
        byte-offset (atom 0)
        line-number (atom 0)
        entries (atom [])
        seen-entry-ids (js/Set.)
        largest-entry-bytes (atom 0)
        current-leaf-id (atom nil)
        has-final-newline? (atom false)
        incomplete-tail-bytes (atom 0)
        process-line!
        (fn [bytes has-newline?]
          (swap! line-number inc)
          (.update prefix-hash bytes)
          (when has-newline? (.update prefix-hash "\n"))
          (let [next-offset (+ @byte-offset (.-length bytes) (if has-newline? 1 0))]
            (if (zero? (.-length bytes))
              (reset! byte-offset next-offset)
              (let [raw-json (.toString bytes "utf8")
                    value (parse-record raw-json transcript-path @line-number)
                    record-session-id (optional-string (aget value "sessionId"))
                    record-agent-id (optional-string (aget value "agentId"))
                    uuid (optional-string (aget value "uuid"))
                    ordinal (count @entries)
                    entry-id (or uuid (deterministic-entry-id stream-id ordinal raw-json))
                    parent-present? (js/Object.hasOwn value "parentUuid")
                    parent-id (aget value "parentUuid")
                    logical-parent-id (optional-string (aget value "logicalParentUuid"))
                    type (or (optional-string (aget value "type")) "unknown")]
                (when (and record-session-id (not= session-id record-session-id))
                  (throw (transcript-error transcript-path @line-number
                                           (str "sessionId does not match " session-id))))
                (when (and agent-id record-agent-id (not= agent-id record-agent-id))
                  (throw (transcript-error transcript-path @line-number
                                           (str "agentId does not match " agent-id))))
                (when (and (= stream-id "main") record-agent-id)
                  (throw (transcript-error transcript-path @line-number
                                           "parent stream record unexpectedly has agentId")))
                (when (and (not= stream-id "main") (not= agent-id record-agent-id))
                  (throw (transcript-error transcript-path @line-number
                                           "subagent stream record is missing its agentId")))
                (when (and parent-present?
                           (not (or (nil? parent-id) (string? parent-id))))
                  (throw (transcript-error transcript-path @line-number
                                           "invalid parentUuid")))
                (when (.has seen-entry-ids entry-id)
                  (throw (transcript-error transcript-path @line-number
                                           (str "duplicate record identity " entry-id))))
                (.add seen-entry-ids entry-id)
                (when (and (= type "last-prompt")
                           (optional-string (aget value "leafUuid")))
                  (reset! current-leaf-id (aget value "leafUuid")))
                (let [entry (cond-> {:entry-id entry-id
                                     :record-uuid uuid
                                     :stream-id stream-id
                                     :type type
                                     :parent-id (when (string? parent-id) parent-id)
                                     :logical-parent-id logical-parent-id
                                     :ordinal ordinal
                                     :byte-offset @byte-offset
                                     :next-byte-offset next-offset
                                     :raw-json raw-json
                                     :payload-hash (sha256 bytes)
                                     :payload-bytes (.-length bytes)
                                     :prefix-hash (digest-copy prefix-hash)}
                              agent-id (assoc :agent-id agent-id)
                              (optional-string (aget value "cwd"))
                              (assoc :cwd (aget value "cwd"))
                              (optional-string (aget value "requestId"))
                              (assoc :request-id (aget value "requestId"))
                              (optional-string (aget value "timestamp"))
                              (assoc :timestamp (aget value "timestamp")))]
                  (swap! largest-entry-bytes max (.-length bytes))
                  (swap! entries conj entry)
                  (when on-entry (on-entry entry))
                  (reset! byte-offset next-offset))))))
        file-descriptor (openSync transcript-path "r")]
    (try
      (loop [pending (js/Buffer.alloc 0)]
        (let [buffer (js/Buffer.allocUnsafe chunk-bytes)
              bytes-read (readSync file-descriptor buffer 0 chunk-bytes nil)]
          (if (zero? bytes-read)
            (when (pos? (.-length pending))
              (let [raw-json (.toString pending "utf8")
                    valid-json? (try
                                  (js/JSON.parse raw-json)
                                  true
                                  (catch :default _ false))]
                (if valid-json?
                  (process-line! pending false)
                  (reset! incomplete-tail-bytes (.-length pending)))))
            (let [chunk (.subarray buffer 0 bytes-read)
                  combined (if (zero? (.-length pending))
                             chunk
                             (js/Buffer.concat #js [pending chunk]))]
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
    (let [after (statSync transcript-path)]
      (when (or (not= (.-size before) (.-size after))
                (not= (.-mtimeMs before) (.-mtimeMs after)))
        (throw (transcript-error transcript-path nil
                                 "file changed while it was being scanned")))
      (let [known-uuids (into #{} (keep :record-uuid) @entries)]
        (doseq [{:keys [parent-id logical-parent-id]} @entries
                referenced-id [parent-id logical-parent-id]
                :when referenced-id]
          (when-not (contains? known-uuids referenced-id)
            (throw (transcript-error transcript-path nil
                                     (str "unresolved record reference " referenced-id)))))
        (when (and @current-leaf-id
                   (not (contains? known-uuids @current-leaf-id)))
          (throw (transcript-error transcript-path nil
                                   (str "unresolved current leaf " @current-leaf-id))))
        {:path transcript-path
         :stream-id stream-id
         :agent-id agent-id
         :source-session-id session-id
         :entries @entries
         :entry-count (count @entries)
         :header-next-byte-offset 0
         :header-prefix-hash (sha256 "")
         :current-leaf-id @current-leaf-id
         :log-hash (digest-copy prefix-hash)
         :log-bytes @byte-offset
         :source-bytes (.-size after)
         :incomplete-tail-bytes @incomplete-tail-bytes
         :largest-entry-bytes @largest-entry-bytes
         :has-final-newline? @has-final-newline?}))))

(defn- parsed-entry [entry]
  (assoc entry :value (js/JSON.parse (:raw-json entry))))

(defn- message-blocks [value]
  (let [message (aget value "message")
        content (when (json-object? message) (aget message "content"))]
    (if (js/Array.isArray content) (array-seq content) [])))

(defn- tool-call-ids [value]
  (keep (fn [block]
          (when (and (json-object? block)
                     (= "tool_use" (aget block "type")))
            (optional-string (aget block "id"))))
        (message-blocks value)))

(defn- tool-result-ids [value]
  (keep (fn [block]
          (when (and (json-object? block)
                     (= "tool_result" (aget block "type")))
            (optional-string (aget block "tool_use_id"))))
        (message-blocks value)))

(defn- selected-parent-entries [parent-stream current-leaf-id]
  (let [parsed (mapv parsed-entry (:entries parent-stream))
        by-uuid (into {} (keep (fn [entry]
                                 (when-let [uuid (:record-uuid entry)] [uuid entry])))
                      parsed)
        ancestry
        (loop [cursor current-leaf-id selected #{}]
          (if (or (nil? cursor) (contains? selected cursor))
            selected
            (if-let [entry (get by-uuid cursor)]
              (recur (or (:parent-id entry) (:logical-parent-id entry))
                     (conj selected cursor))
              selected)))
        selected
        (loop [selected ancestry]
          (let [active (keep by-uuid selected)
                source-assistant-ids
                (into #{} (keep (fn [{:keys [value]}]
                                  (when (seq (tool-result-ids value))
                                    (optional-string
                                     (aget value "sourceToolAssistantUUID")))))
                      active)
                with-sources (into selected (filter #(contains? by-uuid %)
                                                    source-assistant-ids))
                active-with-sources (keep by-uuid with-sources)
                request-ids (into #{} (keep :request-id) active-with-sources)
                with-fragments
                (into with-sources
                      (keep (fn [entry]
                              (when (and (:record-uuid entry)
                                         (contains? request-ids (:request-id entry)))
                                (:record-uuid entry))))
                      parsed)
                active-assistants (keep by-uuid with-fragments)
                calls-by-assistant
                (into {} (keep (fn [{:keys [record-uuid value]}]
                                 (let [call-ids (set (tool-call-ids value))]
                                   (when (and record-uuid (seq call-ids))
                                     [record-uuid call-ids]))))
                      active-assistants)
                with-results
                (into with-fragments
                      (keep (fn [{:keys [record-uuid value]}]
                              (let [source-id (optional-string
                                               (aget value "sourceToolAssistantUUID"))
                                    result-ids (set (tool-result-ids value))]
                                (when (and record-uuid source-id
                                           (seq (set/intersection
                                                 result-ids
                                                 (get calls-by-assistant source-id #{}))))
                                  record-uuid))))
                      parsed)]
            (if (= selected with-results) selected (recur with-results))))]
    (filterv #(contains? selected (:record-uuid %)) parsed)))

(defn scan-session
  [{:keys [session-id transcript-path subagents]}]
  (let [parent (scan-stream {:session-id session-id
                             :transcript-path transcript-path
                             :stream-id "main"})
        children (->> subagents
                      (sort-by :agent-id)
                      (mapv (fn [{:keys [agent-id transcript-path]}]
                              (scan-stream {:session-id session-id
                                            :transcript-path transcript-path
                                            :stream-id (str "agent:" agent-id)
                                            :agent-id agent-id}))))]
    {:source-kind identity/claude-source-kind
     :source-session-id session-id
     :current-leaf-id (:current-leaf-id parent)
     :streams (into [parent] children)
     :entries (into [] (mapcat :entries) (into [parent] children))}))

(defn selected-entries [session-scan]
  (let [streams (:streams session-scan)
        parent (first streams)
        selected-parent (selected-parent-entries parent (:current-leaf-id session-scan))
        selected-children (mapcat :entries (rest streams))]
    (into selected-parent selected-children)))
