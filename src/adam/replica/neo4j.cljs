(ns adam.replica.neo4j
  (:require [adam.knowledge.store :as knowledge-store]
            [adam.replica.store :as store]
            [adam.sources.claude-code.store :as claude-store]
            [clojure.string :as string]
            ["neo4j-driver" :as neo4j-driver]))

(def ^:private session-labels
  ["AdamUser" "AdamIdentity" "AdamSession" "AdamEntry"])

(def ^:private file-evidence-labels
  ["AdamRepository" "AdamCodeFile" "AdamObservation" "AdamReflection"])

(defn- with-session! [^js driver database f]
  (let [^js session (.session driver #js {:database database})]
    (-> (js/Promise.resolve (f session))
        (.finally (fn [] (.close session))))))

(defn- run-sequentially! [^js session statements]
  (reduce
   (fn [promise [query params]]
     (.then promise (fn [_] (.run session query params))))
   (js/Promise.resolve nil)
   statements))

(defn- ensure-label-constraints! [driver database labels]
  (with-session!
    driver
    database
    (fn [session]
      (run-sequentially!
       session
       (mapv
        (fn [label]
          [(str "CREATE CONSTRAINT " (string/lower-case label)
                "_id_unique IF NOT EXISTS FOR (n:" label ") REQUIRE n.id IS UNIQUE")
           nil])
        labels)))))

(defn ensure-session-constraints! [driver database]
  (ensure-label-constraints! driver database session-labels))

(defn ensure-file-evidence-constraints! [driver database]
  (with-session!
    driver
    database
    (fn [session]
      (run-sequentially!
       session
       (conj
        (mapv
         (fn [label]
           [(str "CREATE CONSTRAINT " (string/lower-case label)
                 "_id_unique IF NOT EXISTS FOR (n:" label ") REQUIRE n.id IS UNIQUE")
            nil])
         file-evidence-labels)
        ["CREATE INDEX adamentry_session_entry IF NOT EXISTS
          FOR (n:AdamEntry) ON (n.sessionId, n.entryId)"
         nil])))))

(defn- initialize-user! [driver database {:keys [id identity]}]
  (with-session!
    driver
    database
    (fn [session]
      (run-sequentially!
       session
       (cond->
        [["MERGE (u:AdamUser {id: $userId}) ON CREATE SET u.createdAt = datetime()"
          #js {:userId id}]]
         identity
         (conj
          ["MATCH (u:AdamUser {id: $userId})
            MERGE (i:AdamIdentity {id: $identityId})
            ON CREATE SET i.firstSeenAt = datetime()
            SET i.kind = $kind,
                i.value = $value,
                i.normalizedValue = $normalizedValue,
                i.displayValue = $displayValue,
                i.lastSeenAt = datetime()
            MERGE (u)-[:HAS_IDENTITY]->(i)"
           #js {:userId id
                :identityId (:id identity)
                :kind (:kind identity)
                :value (:value identity)
                :normalizedValue (:normalized-value identity)
                :displayValue (:display-value identity)}]))))))

(defn initialize-schema! [driver database user]
  (-> (ensure-session-constraints! driver database)
      (.then (fn [_] (initialize-user! driver database user)))))

(defn- set-property! [object key value include-nil?]
  (when (or include-nil? (some? value))
    (aset object key value))
  object)

(defn- session-properties [session]
  (let [properties #js {}]
    (doseq [[key value include-nil?]
            [["id" (:id session) false]
             ["sourceKind" (:source-kind session) false]
             ["sourceSessionId" (:source-session-id session) false]
             ["piSessionId" (:pi-session-id session) false]
             ["headerJson" (:header-json session) false]
             ["cwd" (:cwd session) false]
             ["version" (:version session) false]
             ["createdAt" (:created-at session) false]
             ["parentSession" (:parent-session session) false]
             ["parentPiSessionId" (:parent-pi-session-id session) false]
             ["parentSessionId" (:parent-session-id session) false]
             ["name" (:name session) false]
             ["currentLeafId" (:current-leaf-id session) true]
             ["sourceFile" (:source-file session) false]
             ["writerVersion" (:writer-version session) false]]]
      (set-property! properties key value include-nil?))
    properties))

(defn- entry-properties [session entry]
  (let [properties #js {}]
    (doseq [[key value include-nil?]
            [["id" (:id entry) false]
             ["sessionId" (:id session) false]
             ["sourceKind" (:source-kind session) false]
             ["sourceSessionId" (:source-session-id session) false]
             ["entryId" (:entry-id entry) false]
             ["type" (:type entry) false]
             ["role" (:role entry) false]
             ["parentId" (:parent-id entry) true]
             ["timestamp" (:timestamp entry) false]
             ["ordinal" (:ordinal entry) false]
             ["rawJson" (:raw-json entry) false]
             ["payloadHash" (:payload-hash entry) false]
             ["payloadBytes" (:payload-bytes entry) false]
             ["parentUrn" (:parent-urn entry) false]
             ["logicalParentId" (:logical-parent-id entry) true]
             ["logicalParentUrn" (:logical-parent-urn entry) false]
             ["recordUuid" (:record-uuid entry) false]
             ["streamId" (:stream-id entry) false]
             ["agentId" (:agent-id entry) false]
             ["cwd" (:cwd entry) false]
             ["requestId" (:request-id entry) false]]]
      (set-property! properties key value include-nil?))
    properties))

(defn- records [^js result]
  (array-seq (.-records result)))

(defn- record-get [^js record key]
  (.get record key))

(defn- neo-integer [value]
  (let [^js candidate value]
    (cond
      (number? value) value
      (and value (fn? (.-toNumber candidate))) (.toNumber candidate)
      :else (throw (js/Error. "invalid Neo4j integer value")))))

(def ^:private user-urn-prefix "urn:adam:user:")

(defn- source-identity-prefixes [user-id]
  (when-not (and (string? user-id) (string/starts-with? user-id user-urn-prefix))
    (throw (js/Error. "invalid Adam user identity")))
  (let [user-uuid (subs user-id (count user-urn-prefix))]
    {:session-prefix (str "urn:adam:session:" user-uuid ":pi:")
     :entry-prefix (str "urn:adam:entry:" user-uuid ":pi:")
     :observation-prefix (str "urn:adam:observation:" user-uuid ":pi:")
     :reflection-prefix (str "urn:adam:reflection:" user-uuid ":pi:")}))

(defn- checkpoint-from-record [record]
  (when (and record (some? (record-get record "byteOffset")))
    (cond-> {:complete-through-ordinal (neo-integer (record-get record "ordinal"))
             :complete-through-byte-offset (neo-integer (record-get record "byteOffset"))
             :committed-prefix-hash (str (record-get record "prefixHash"))}
      (some? (record-get record "entryCount"))
      (assoc :entry-count (neo-integer (record-get record "entryCount")))
      (some? (record-get record "logHash"))
      (assoc :log-hash (str (record-get record "logHash"))))))

(defn- merge-session! [^js tx session]
  (-> (.run
       tx
       "MERGE (u:AdamUser {id: $userId})
        ON CREATE SET u.createdAt = datetime()
        MERGE (s:AdamSession {id: $sessionId})
        SET s += $session
        MERGE (u)-[:OWNS]->(s)"
       #js {:userId (:user-id session)
            :sessionId (:id session)
            :session (session-properties session)})
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (s:AdamSession {id: $sessionId})
           OPTIONAL MATCH (parent:AdamSession {id: s.parentSessionId})
           FOREACH (_ IN CASE WHEN parent IS NULL OR parent.id = s.id THEN [] ELSE [1] END |
             MERGE (s)-[lineage:FORKED_FROM]->(parent)
             SET lineage.resolution = 'parent_header_id')
           WITH s
           OPTIONAL MATCH (child:AdamSession {parentSessionId: s.id})
           FOREACH (_ IN CASE WHEN child IS NULL OR child.id = s.id THEN [] ELSE [1] END |
             MERGE (child)-[lineage:FORKED_FROM]->(s)
             SET lineage.resolution = 'parent_header_id')"
          #js {:sessionId (:id session)})))
      (.then
       (fn [_]
         (if-let [identity (:identity session)]
           (.run
            tx
            "MATCH (u:AdamUser {id: $userId})
             MERGE (i:AdamIdentity {id: $identityId})
             ON CREATE SET i.firstSeenAt = datetime()
             SET i.kind = $kind,
                 i.value = $value,
                 i.normalizedValue = $normalizedValue,
                 i.displayValue = $displayValue,
                 i.lastSeenAt = datetime()
             MERGE (u)-[:HAS_IDENTITY]->(i)"
            #js {:userId (:user-id session)
                 :identityId (:id identity)
                 :kind (:kind identity)
                 :value (:value identity)
                 :normalizedValue (:normalized-value identity)
                 :displayValue (:display-value identity)})
           nil)))))

(defn- update-current-leaf! [^js tx session]
  (.run
   tx
   "MATCH (s:AdamSession {id: $sessionId})
    OPTIONAL MATCH (s)-[old:CURRENT_LEAF]->()
    DELETE old
    WITH s
    OPTIONAL MATCH (leaf:AdamEntry {sessionId: $sessionId, entryId: $leafId})
    FOREACH (_ IN CASE WHEN leaf IS NULL THEN [] ELSE [1] END |
      MERGE (s)-[:CURRENT_LEAF]->(leaf))"
   #js {:sessionId (:id session)
        :leafId (:current-leaf-id session)}))

(defn- checkpoint-in-transaction! [^js tx session-id]
  (-> (.run
       tx
       "MATCH (s:AdamSession {id: $sessionId})
        RETURN s.completeThroughOrdinal AS ordinal,
               s.completeThroughByteOffset AS byteOffset,
               s.committedPrefixHash AS prefixHash,
               s.entryCount AS entryCount,
               s.logHash AS logHash"
       #js {:sessionId session-id})
      (.then (fn [result] (checkpoint-from-record (first (records result)))))))

(defn- reject-checkpoint! [tx session-id actual-offset]
  (-> (checkpoint-in-transaction! tx session-id)
      (.then
       (fn [current]
         (throw (store/checkpoint-conflict
                 (or (:complete-through-byte-offset current) -1)
                 actual-offset))))))

(defn- write-batch-transaction! [^js tx {:keys [session entries checkpoint]}]
  (-> (merge-session! tx session)
      (.then
       (fn [_]
         (.run
          tx
          "UNWIND $entries AS input
           MERGE (e:AdamEntry {id: input.id})
           ON CREATE SET e += input
           WITH e, input
           RETURN e.entryId AS entryId,
                  e.payloadHash AS expectedHash,
                  input.payloadHash AS actualHash"
          #js {:entries (clj->js (mapv #(entry-properties session %) entries))})))
      (.then
       (fn [result]
         (doseq [record (records result)]
           (store/assert-entry-compatible!
            {:entry-id (str (record-get record "entryId"))
             :payload-hash (str (record-get record "expectedHash"))}
            {:entry-id (str (record-get record "entryId"))
             :payload-hash (str (record-get record "actualHash"))}))))
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (s:AdamSession {id: $sessionId})
           UNWIND $entries AS input
           MATCH (e:AdamEntry {id: input.id})
           MERGE (s)-[:HAS_ENTRY]->(e)
           WITH input, e
           OPTIONAL MATCH (parent:AdamEntry {id: input.parentUrn})
           FOREACH (_ IN CASE WHEN parent IS NULL THEN [] ELSE [1] END |
             MERGE (e)-[:PARENT]->(parent))"
          #js {:sessionId (:id session)
               :entries (clj->js (mapv #(entry-properties session %) entries))})))
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (s:AdamSession {id: $sessionId})
           WHERE s.completeThroughByteOffset IS NULL
              OR s.completeThroughByteOffset < $byteOffset
              OR (s.completeThroughByteOffset = $byteOffset
                  AND s.committedPrefixHash = $prefixHash)
           SET s.completeThroughOrdinal = $ordinal,
               s.completeThroughByteOffset = $byteOffset,
               s.committedPrefixHash = $prefixHash,
               s.entryCount = null,
               s.logHash = null,
               s.logBytes = null,
               s.largestEntryBytes = null,
               s.lastMirroredAt = datetime()
           RETURN s.completeThroughByteOffset AS byteOffset"
          #js {:sessionId (:id session)
               :ordinal (:complete-through-ordinal checkpoint)
               :byteOffset (:complete-through-byte-offset checkpoint)
               :prefixHash (:committed-prefix-hash checkpoint)})))
      (.then
       (fn [result]
         (if (empty? (records result))
           (reject-checkpoint! tx (:id session) (:complete-through-byte-offset checkpoint))
           (update-current-leaf! tx session))))))

(defn- complete-session-transaction! [^js tx {:keys [session checkpoint log-bytes
                                                      largest-entry-bytes has-final-newline?]}]
  (-> (merge-session! tx session)
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (s:AdamSession {id: $sessionId})
           WHERE s.completeThroughByteOffset IS NULL
              OR (s.completeThroughByteOffset = $byteOffset
                  AND s.committedPrefixHash = $prefixHash)
           SET s.completeThroughOrdinal = $ordinal,
               s.completeThroughByteOffset = $byteOffset,
               s.committedPrefixHash = $prefixHash,
               s.entryCount = $entryCount,
               s.logHash = $logHash,
               s.logBytes = $logBytes,
               s.largestEntryBytes = $largestEntryBytes,
               s.hasFinalNewline = $hasFinalNewline,
               s.lastMirroredAt = datetime()
           RETURN s.completeThroughByteOffset AS byteOffset"
          #js {:sessionId (:id session)
               :ordinal (:complete-through-ordinal checkpoint)
               :byteOffset (:complete-through-byte-offset checkpoint)
               :prefixHash (:committed-prefix-hash checkpoint)
               :entryCount (:entry-count checkpoint)
               :logHash (:log-hash checkpoint)
               :logBytes log-bytes
               :largestEntryBytes largest-entry-bytes
               :hasFinalNewline has-final-newline?})))
      (.then
       (fn [result]
         (if (empty? (records result))
           (reject-checkpoint! tx (:id session) (:complete-through-byte-offset checkpoint))
           (update-current-leaf! tx session))))))

(defn- stream-properties [stream]
  (let [properties #js {}]
    (doseq [[key value include-nil?]
            [["id" (:id stream) false]
             ["sessionId" (:session-id stream) false]
             ["streamId" (:stream-id stream) false]
             ["agentId" (:agent-id stream) false]
             ["transcriptPath" (:path stream) false]]]
      (set-property! properties key value include-nil?))
    properties))

(defn- merge-claude-stream! [^js tx session stream]
  (-> (merge-session! tx session)
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (s:AdamSession {id: $sessionId})
           MERGE (stream:AdamTranscriptStream {id: $streamId})
           SET stream += $stream
           MERGE (s)-[:HAS_STREAM]->(stream)"
          #js {:sessionId (:id session)
               :streamId (:id stream)
               :stream (stream-properties stream)})))))

(defn- stream-checkpoint-in-transaction! [^js tx stream-id]
  (-> (.run
       tx
       "MATCH (stream:AdamTranscriptStream {id: $streamId})
        RETURN stream.completeThroughOrdinal AS ordinal,
               stream.completeThroughByteOffset AS byteOffset,
               stream.committedPrefixHash AS prefixHash,
               stream.entryCount AS entryCount,
               stream.logHash AS logHash"
       #js {:streamId stream-id})
      (.then (fn [result] (checkpoint-from-record (first (records result)))))))

(defn- reject-stream-checkpoint! [tx stream-id actual-offset]
  (-> (stream-checkpoint-in-transaction! tx stream-id)
      (.then
       (fn [current]
         (throw (store/checkpoint-conflict
                 (or (:complete-through-byte-offset current) -1)
                 actual-offset))))))

(defn- write-claude-stream-batch-transaction!
  [^js tx {:keys [session stream entries checkpoint]}]
  (-> (merge-claude-stream! tx session stream)
      (.then
       (fn [_]
         (.run
          tx
          "UNWIND $entries AS input
           MERGE (entry:AdamEntry {id: input.id})
           ON CREATE SET entry += input
           WITH entry, input
           RETURN entry.entryId AS entryId,
                  entry.payloadHash AS expectedHash,
                  input.payloadHash AS actualHash"
          #js {:entries (clj->js (mapv #(entry-properties session %) entries))})))
      (.then
       (fn [result]
         (doseq [record (records result)]
           (store/assert-entry-compatible!
            {:entry-id (str (record-get record "entryId"))
             :payload-hash (str (record-get record "expectedHash"))}
            {:entry-id (str (record-get record "entryId"))
             :payload-hash (str (record-get record "actualHash"))}))))
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (s:AdamSession {id: $sessionId}),
                 (stream:AdamTranscriptStream {id: $streamId})
           UNWIND $entries AS input
           MATCH (entry:AdamEntry {id: input.id})
           MERGE (s)-[:HAS_ENTRY]->(entry)
           MERGE (stream)-[:HAS_ENTRY]->(entry)
           WITH input, entry
           OPTIONAL MATCH (parent:AdamEntry {id: input.parentUrn})
           OPTIONAL MATCH (logicalParent:AdamEntry {id: input.logicalParentUrn})
           FOREACH (_ IN CASE WHEN parent IS NULL THEN [] ELSE [1] END |
             MERGE (entry)-[:PARENT]->(parent))
           FOREACH (_ IN CASE WHEN logicalParent IS NULL THEN [] ELSE [1] END |
             MERGE (entry)-[:LOGICAL_PARENT]->(logicalParent))"
          #js {:sessionId (:id session)
               :streamId (:id stream)
               :entries (clj->js (mapv #(entry-properties session %) entries))})))
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (stream:AdamTranscriptStream {id: $streamId})
           WHERE stream.completeThroughByteOffset IS NULL
              OR stream.completeThroughByteOffset < $byteOffset
              OR (stream.completeThroughByteOffset = $byteOffset
                  AND stream.committedPrefixHash = $prefixHash)
           SET stream.completeThroughOrdinal = $ordinal,
               stream.completeThroughByteOffset = $byteOffset,
               stream.committedPrefixHash = $prefixHash,
               stream.entryCount = null,
               stream.logHash = null,
               stream.logBytes = null,
               stream.lastMirroredAt = datetime()
           RETURN stream.completeThroughByteOffset AS byteOffset"
          #js {:streamId (:id stream)
               :ordinal (:complete-through-ordinal checkpoint)
               :byteOffset (:complete-through-byte-offset checkpoint)
               :prefixHash (:committed-prefix-hash checkpoint)})))
      (.then
       (fn [result]
         (if (empty? (records result))
           (reject-stream-checkpoint! tx (:id stream)
                                      (:complete-through-byte-offset checkpoint))
           nil)))))

(defn- complete-claude-stream-transaction!
  [^js tx {:keys [session stream checkpoint log-bytes source-bytes
                  incomplete-tail-bytes largest-entry-bytes has-final-newline?]}]
  (-> (merge-claude-stream! tx session stream)
      (.then
       (fn [_]
         (.run
          tx
          "MATCH (stream:AdamTranscriptStream {id: $streamId})
           WHERE stream.completeThroughByteOffset IS NULL
              OR (stream.completeThroughByteOffset = $byteOffset
                  AND stream.committedPrefixHash = $prefixHash)
           SET stream.completeThroughOrdinal = $ordinal,
               stream.completeThroughByteOffset = $byteOffset,
               stream.committedPrefixHash = $prefixHash,
               stream.entryCount = $entryCount,
               stream.logHash = $logHash,
               stream.logBytes = $logBytes,
               stream.sourceBytes = $sourceBytes,
               stream.incompleteTailBytes = $incompleteTailBytes,
               stream.largestEntryBytes = $largestEntryBytes,
               stream.hasFinalNewline = $hasFinalNewline,
               stream.lastMirroredAt = datetime(),
               stream.conflicted = false
           RETURN stream.completeThroughByteOffset AS byteOffset"
          #js {:streamId (:id stream)
               :ordinal (:complete-through-ordinal checkpoint)
               :byteOffset (:complete-through-byte-offset checkpoint)
               :prefixHash (:committed-prefix-hash checkpoint)
               :entryCount (:entry-count checkpoint)
               :logHash (:log-hash checkpoint)
               :logBytes log-bytes
               :sourceBytes source-bytes
               :incompleteTailBytes incomplete-tail-bytes
               :largestEntryBytes largest-entry-bytes
               :hasFinalNewline has-final-newline?})))
      (.then
       (fn [result]
         (if (empty? (records result))
           (reject-stream-checkpoint! tx (:id stream)
                                      (:complete-through-byte-offset checkpoint))
           nil)))))

(defn- repository-properties [repository]
  (let [properties #js {:id (:id repository)}]
    (when-let [origin (:normalized-remote repository)]
      (aset properties "normalizedOrigin" origin))
    properties))

(defn- file-properties [file]
  #js {:id (:id file)
       :repositoryId (:repository-id file)
       :relativePath (:relative-path file)})

(defn- evidence-properties [evidence]
  (let [properties #js {:entryId (:entry-id evidence)
                        :fileId (:file-id evidence)
                        :commit (:commit evidence)
                        :dirty (= true (:dirty? evidence))}]
    (when-let [branch (:branch evidence)]
      (aset properties "branch" branch))
    properties))

(defn- observation-properties [observation]
  #js {:id (:id observation)
       :sourceKind (:source-kind observation)
       :sourceSessionId (:source-session-id observation)
       :producer (:producer observation)
       :adapterVersion (:adapter-version observation)
       :memoryId (:memory-id observation)
       :content (:content observation)
       :timestamp (:timestamp observation)
       :relevance (:relevance observation)
       :tokenCount (:token-count observation)
       :recordingEntryId (:recording-entry-id observation)
       :sourceEntryIds (clj->js (:source-entry-ids observation))
       :fileIds (clj->js (:file-ids observation))
       :dropped (= true (:dropped? observation))})

(defn- reflection-properties [reflection]
  #js {:id (:id reflection)
       :sourceKind (:source-kind reflection)
       :sourceSessionId (:source-session-id reflection)
       :producer (:producer reflection)
       :adapterVersion (:adapter-version reflection)
       :memoryId (:memory-id reflection)
       :content (:content reflection)
       :tokenCount (:token-count reflection)
       :recordingEntryId (:recording-entry-id reflection)
       :supportingObservationIds
       (clj->js (:supporting-observation-ids reflection))})

(defn- index-file-evidence-transaction! [^js tx projection]
  (let [repository (:repository projection)]
    (-> (.run
         tx
         "MATCH (s:AdamSession {id: $sessionId})
          OPTIONAL MATCH (s)-[:HAS_MEMORY]->(memory)
          DETACH DELETE memory"
         #js {:sessionId (:session-id projection)})
        (.then
         (fn [_]
           (.run
            tx
            "MATCH (s:AdamSession {id: $sessionId})-[:HAS_ENTRY]->(entry)
             OPTIONAL MATCH (entry)-[touch:TOUCHES]->()
             DELETE touch"
            #js {:sessionId (:session-id projection)})))
        (.then
         (fn [_]
           (.run
            tx
            "MATCH (s:AdamSession {id: $sessionId})
             OPTIONAL MATCH (s)-[old:WORKED_ON]->()
             DELETE old
             SET s.codeMemoryVersion = $extractorVersion
             WITH s
             MERGE (repository:AdamRepository {id: $repository.id})
             SET repository.normalizedOrigin = $repository.normalizedOrigin
             REMOVE repository.root, repository.normalizedRemote
             WITH s, repository
             OPTIONAL MATCH (:AdamUser)-[legacyOwnership:OWNS]->(repository)
             DELETE legacyOwnership
             WITH s, repository
             MERGE (s)-[worked:WORKED_ON]->(repository)
             SET worked.root = $root,
                 worked.commit = $commit,
                 worked.branch = $branch,
                 worked.dirty = $dirty,
                 worked.extractorVersion = $extractorVersion,
                 worked.indexedAt = datetime()"
            #js {:sessionId (:session-id projection)
                 :repository (repository-properties repository)
                 :root (:root repository)
                 :commit (:commit repository)
                 :branch (:branch repository)
                 :dirty (= true (:dirty? repository))
                 :extractorVersion (:extractor-version projection)})))
        (.then
         (fn [_]
           (.run
            tx
            "MATCH (repository:AdamRepository {id: $repositoryId})
             UNWIND $files AS input
             MERGE (file:AdamCodeFile {id: input.id})
             SET file.repositoryId = input.repositoryId,
                 file.relativePath = input.relativePath
             MERGE (repository)-[:CONTAINS]->(file)"
            #js {:repositoryId (:id repository)
                 :files (clj->js (mapv file-properties (:files projection)))})))
        (.then
         (fn [_]
           (.run
            tx
            "UNWIND $evidence AS input
             MATCH (entry:AdamEntry {sessionId: $sessionId, entryId: input.entryId})
             MATCH (file:AdamCodeFile {id: input.fileId})
             MERGE (entry)-[touch:TOUCHES]->(file)
             SET touch.basis = 'tool_path',
                 touch.extractorVersion = $extractorVersion,
                 touch.commit = input.commit,
                 touch.branch = input.branch,
                 touch.dirty = input.dirty"
            #js {:sessionId (:session-id projection)
                 :evidence (clj->js (mapv evidence-properties
                                           (:entry-file-evidence projection)))
                 :extractorVersion (:extractor-version projection)})))
        (.then
         (fn [_]
           (.run
            tx
            "UNWIND $observations AS input
             MATCH (s:AdamSession {id: $sessionId})
             MERGE (observation:AdamObservation {id: input.id})
             SET observation.sourceKind = input.sourceKind,
                 observation.sourceSessionId = input.sourceSessionId,
                 observation.producer = input.producer,
                 observation.adapterVersion = input.adapterVersion,
                 observation.memoryId = input.memoryId,
                 observation.content = input.content,
                 observation.timestamp = input.timestamp,
                 observation.relevance = input.relevance,
                 observation.tokenCount = input.tokenCount,
                 observation.recordingEntryId = input.recordingEntryId,
                 observation.sourceEntryIds = input.sourceEntryIds,
                 observation.dropped = input.dropped,
                 observation.extractorVersion = $extractorVersion
             MERGE (s)-[:HAS_MEMORY]->(observation)
             WITH observation, input
             UNWIND input.sourceEntryIds AS sourceEntryId
             MATCH (source:AdamEntry {sessionId: $sessionId, entryId: sourceEntryId})
             MERGE (observation)-[:SOURCED_FROM]->(source)"
            #js {:sessionId (:session-id projection)
                 :observations (clj->js (mapv observation-properties
                                              (:observations projection)))
                 :extractorVersion (:extractor-version projection)})))
        (.then
         (fn [_]
           (.run
            tx
            "UNWIND $observations AS input
             MATCH (observation:AdamObservation {id: input.id})
             UNWIND input.fileIds AS fileId
             MATCH (file:AdamCodeFile {id: fileId})
             MERGE (observation)-[about:ABOUT]->(file)
             SET about.basis = 'source_tool_path',
                 about.extractorVersion = $extractorVersion"
            #js {:observations (clj->js (mapv observation-properties
                                              (:observations projection)))
                 :extractorVersion (:extractor-version projection)})))
        (.then
         (fn [_]
           (.run
            tx
            "UNWIND $reflections AS input
             MATCH (s:AdamSession {id: $sessionId})
             MERGE (reflection:AdamReflection {id: input.id})
             SET reflection.sourceKind = input.sourceKind,
                 reflection.sourceSessionId = input.sourceSessionId,
                 reflection.producer = input.producer,
                 reflection.adapterVersion = input.adapterVersion,
                 reflection.memoryId = input.memoryId,
                 reflection.content = input.content,
                 reflection.tokenCount = input.tokenCount,
                 reflection.recordingEntryId = input.recordingEntryId,
                 reflection.supportingObservationIds = input.supportingObservationIds,
                 reflection.extractorVersion = $extractorVersion
             MERGE (s)-[:HAS_MEMORY]->(reflection)
             WITH reflection, input
             UNWIND input.supportingObservationIds AS observationId
             MATCH (observation:AdamObservation {memoryId: observationId})
                   <-[:HAS_MEMORY]-(:AdamSession {id: $sessionId})
             MERGE (reflection)-[:SUPPORTED_BY]->(observation)"
            #js {:sessionId (:session-id projection)
                 :reflections (clj->js (mapv reflection-properties
                                             (:reflections projection)))
                 :extractorVersion (:extractor-version projection)}))))))

(defn- session-summary [properties]
  {:id (str (aget properties "id"))
   :source-kind (when (some? (aget properties "sourceKind"))
                  (str (aget properties "sourceKind")))
   :source-session-id (when (some? (aget properties "sourceSessionId"))
                        (str (aget properties "sourceSessionId")))
   :pi-session-id (str (aget properties "piSessionId"))
   :cwd (str (aget properties "cwd"))
   :name (when (some? (aget properties "name")) (str (aget properties "name")))
   :created-at (when (some? (aget properties "createdAt")) (str (aget properties "createdAt")))
   :source-file (when (some? (aget properties "sourceFile")) (str (aget properties "sourceFile")))
   :current-leaf-id (when (some? (aget properties "currentLeafId"))
                      (str (aget properties "currentLeafId")))
   :conflicted? (= true (aget properties "conflicted"))
   :complete? (and (some? (aget properties "entryCount"))
                   (some? (aget properties "logHash")))})

(defn- file-memory-record [record]
  (let [memory (.-properties (record-get record "memory"))
        stored-session (.-properties (record-get record "s"))
        contexts (or (record-get record "sourceContexts") #js [])]
    {:kind (keyword (str (record-get record "kind")))
     :memory-id (str (aget memory "memoryId"))
     :content (str (aget memory "content"))
     :session-id (str (aget stored-session "id"))
     :pi-session-id (str (aget stored-session "piSessionId"))
     :source-entry-ids (->> (or (record-get record "sourceEntryIds") #js [])
                            array-seq (mapv str) sort vec)
     :source-contexts
     (->> (array-seq contexts)
          (keep (fn [context]
                  (let [entry-id (aget context "entryId")
                        commit (aget context "commit")]
                    (when (and (string? entry-id) (string? commit))
                      (cond-> {:entry-id entry-id
                               :commit commit
                               :dirty? (= true (aget context "dirty"))}
                        (string? (aget context "branch"))
                        (assoc :branch (aget context "branch")))))))
          (sort-by :entry-id)
          vec)
     :dropped? (= true (aget memory "dropped"))}))

(defrecord Neo4jSessionReplica [driver database]
  store/SessionIdentityMigrationStore
  (session-identity-version! [_ user-id]
    (let [{:keys [session-prefix entry-prefix observation-prefix reflection-prefix]}
          (source-identity-prefixes user-id)]
      (with-session!
        driver
        database
        (fn [session]
          (-> (.run
               session
               "MATCH (u:AdamUser {id: $userId})
                RETURN CASE
                  WHEN EXISTS {
                    MATCH (u)-[:OWNS]->(s:AdamSession)
                    WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      AND (s.piSessionId IS NULL
                       OR coalesce(s.sourceKind, '') <> 'pi'
                       OR coalesce(s.sourceSessionId, '') <> s.piSessionId
                       OR s.id <> $sessionPrefix + s.piSessionId
                       OR (s.parentPiSessionId IS NOT NULL
                           AND coalesce(s.parentSessionId, '') <>
                               $sessionPrefix + s.parentPiSessionId))
                  }
                  OR EXISTS {
                    MATCH (u)-[:OWNS]->(s:AdamSession)-[:HAS_ENTRY]->(entry:AdamEntry)
                    WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      AND (entry.id <> $entryPrefix + s.piSessionId + ':' + entry.entryId
                       OR coalesce(entry.sessionId, '') <> s.id
                       OR coalesce(entry.sourceKind, '') <> 'pi'
                       OR coalesce(entry.sourceSessionId, '') <> s.piSessionId
                       OR (entry.parentId IS NOT NULL
                           AND coalesce(entry.parentUrn, '') <>
                               $entryPrefix + s.piSessionId + ':' + entry.parentId)
                       OR (entry.parentId IS NULL AND entry.parentUrn IS NOT NULL))
                  }
                  OR EXISTS {
                    MATCH (u)-[:OWNS]->(s:AdamSession)-[:HAS_MEMORY]->(memory:AdamObservation)
                    WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      AND (memory.id <>
                            $observationPrefix + s.piSessionId + ':' + memory.memoryId
                       OR coalesce(memory.sourceKind, '') <> 'pi'
                       OR coalesce(memory.sourceSessionId, '') <> s.piSessionId)
                  }
                  OR EXISTS {
                    MATCH (u)-[:OWNS]->(s:AdamSession)-[:HAS_MEMORY]->(memory:AdamReflection)
                    WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      AND (memory.id <>
                            $reflectionPrefix + s.piSessionId + ':' + memory.memoryId
                       OR coalesce(memory.sourceKind, '') <> 'pi'
                       OR coalesce(memory.sourceSessionId, '') <> s.piSessionId)
                  }
                  THEN 0
                  ELSE u.sessionIdentityVersion
                END AS version"
               #js {:userId user-id
                    :sessionPrefix session-prefix
                    :entryPrefix entry-prefix
                    :observationPrefix observation-prefix
                    :reflectionPrefix reflection-prefix})
              (.then
               (fn [result]
                 (when-let [record (first (records result))]
                   (when-let [version (record-get record "version")]
                     (neo-integer version))))))))))

  (migrate-pi-session-identities! [_ user-id target-version]
    (let [{:keys [session-prefix entry-prefix observation-prefix reflection-prefix]}
          (source-identity-prefixes user-id)]
      (with-session!
        driver
        database
        (fn [^js session]
          (.executeWrite
           session
           (fn [tx]
             (-> (.run
                  tx
                  "MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)
                   WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                     AND s.piSessionId IS NULL
                   RETURN count(s) AS invalidCount"
                  #js {:userId user-id})
                 (.then
                  (fn [result]
                    (let [record (first (records result))
                          invalid-count (if record
                                          (neo-integer (record-get record "invalidCount"))
                                          0)]
                      (when (pos? invalid-count)
                        (throw (ex-info
                                "cannot source-scope sessions without piSessionId"
                                {:type :invalid-session-identity
                                 :count invalid-count}))))))
                 (.then
                  (fn [_]
                    (.run
                     tx
                     "MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)
                      WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      SET s.id = $sessionPrefix + s.piSessionId,
                          s.sourceKind = 'pi',
                          s.sourceSessionId = s.piSessionId,
                          s.parentSessionId = CASE
                            WHEN s.parentPiSessionId IS NULL THEN null
                            ELSE $sessionPrefix + s.parentPiSessionId
                          END"
                     #js {:userId user-id :sessionPrefix session-prefix})))
                 (.then
                  (fn [_]
                    (.run
                     tx
                     "MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)-[:HAS_ENTRY]->(entry:AdamEntry)
                      WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      SET entry.id = $entryPrefix + s.piSessionId + ':' + entry.entryId,
                          entry.sessionId = s.id,
                          entry.sourceKind = 'pi',
                          entry.sourceSessionId = s.piSessionId,
                          entry.parentUrn = CASE
                            WHEN entry.parentId IS NULL THEN null
                            ELSE $entryPrefix + s.piSessionId + ':' + entry.parentId
                          END"
                     #js {:userId user-id :entryPrefix entry-prefix})))
                 (.then
                  (fn [_]
                    (.run
                     tx
                     "MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)-[:HAS_MEMORY]->(memory:AdamObservation)
                      WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      SET memory.id = $observationPrefix + s.piSessionId + ':' + memory.memoryId,
                          memory.sourceKind = 'pi',
                          memory.sourceSessionId = s.piSessionId"
                     #js {:userId user-id :observationPrefix observation-prefix})))
                 (.then
                  (fn [_]
                    (.run
                     tx
                     "MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)-[:HAS_MEMORY]->(memory:AdamReflection)
                      WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                      SET memory.id = $reflectionPrefix + s.piSessionId + ':' + memory.memoryId,
                          memory.sourceKind = 'pi',
                          memory.sourceSessionId = s.piSessionId"
                     #js {:userId user-id :reflectionPrefix reflection-prefix})))
                 (.then
                  (fn [_]
                    (.run
                     tx
                     "MATCH (u:AdamUser {id: $userId})
                      SET u.sessionIdentityVersion = $targetVersion,
                          u.sessionIdentityMigratedAt = datetime()"
                     #js {:userId user-id :targetVersion target-version}))))))))))

  claude-store/ClaudeTranscriptStore
  (ensure-claude-schema! [_]
    (ensure-label-constraints! driver database ["AdamTranscriptStream"]))

  (get-stream-checkpoint! [_ stream-id]
    (with-session!
      driver database
      (fn [session]
        (-> (.run
             session
             "MATCH (stream:AdamTranscriptStream {id: $streamId})
              RETURN stream.completeThroughOrdinal AS ordinal,
                     stream.completeThroughByteOffset AS byteOffset,
                     stream.committedPrefixHash AS prefixHash,
                     stream.entryCount AS entryCount,
                     stream.logHash AS logHash"
             #js {:streamId stream-id})
            (.then (fn [result]
                     (checkpoint-from-record (first (records result)))))))))

  (write-stream-batch! [_ request]
    (with-session!
      driver database
      (fn [^js session]
        (.executeWrite session
                       (fn [tx]
                         (write-claude-stream-batch-transaction! tx request))))))

  (complete-stream! [_ request]
    (with-session!
      driver database
      (fn [^js session]
        (.executeWrite session
                       (fn [tx]
                         (complete-claude-stream-transaction! tx request))))))

  (mark-stream-conflict! [_ conflict]
    (with-session!
      driver database
      (fn [session]
        (.run
         session
         "MERGE (stream:AdamTranscriptStream {id: $streamId})
          SET stream.conflicted = true,
              stream.conflictReason = $reason,
              stream.conflictSourceFile = $sourceFile,
              stream.conflictDetectedAt = datetime()"
         #js {:streamId (:stream-id conflict)
              :reason (name (:reason conflict))
              :sourceFile (:source-file conflict)}))))

  (complete-claude-session! [_ {:keys [session]}]
    (with-session!
      driver database
      (fn [^js neo-session]
        (.executeWrite
         neo-session
         (fn [tx]
           (-> (merge-session! tx session)
               (.then (fn [_] (update-current-leaf! tx session)))))))))

  knowledge-store/FileEvidenceStore
  (ensure-file-evidence-schema! [_]
    (ensure-file-evidence-constraints! driver database))

  (index-file-evidence! [_ projection]
    (with-session!
      driver
      database
      (fn [^js session]
        (.executeWrite session
                       (fn [tx]
                         (index-file-evidence-transaction! tx projection))))))

  (clear-file-evidence! [_ session-id extractor-version]
    (with-session!
      driver
      database
      (fn [^js session]
        (.executeWrite
         session
         (fn [tx]
           (-> (.run
                tx
                "MATCH (s:AdamSession {id: $sessionId})
                 OPTIONAL MATCH (s)-[:HAS_MEMORY]->(memory)
                 DETACH DELETE memory"
                #js {:sessionId session-id})
               (.then
                (fn [_]
                  (.run
                   tx
                   "MATCH (s:AdamSession {id: $sessionId})-[:HAS_ENTRY]->(entry)
                    OPTIONAL MATCH (entry)-[touch:TOUCHES]->()
                    DELETE touch"
                   #js {:sessionId session-id})))
               (.then
                (fn [_]
                  (.run
                   tx
                   "MATCH (s:AdamSession {id: $sessionId})
                    OPTIONAL MATCH (s)-[worked:WORKED_ON]->()
                    DELETE worked
                    SET s.codeMemoryVersion = $extractorVersion"
                   #js {:sessionId session-id
                        :extractorVersion extractor-version})))))))))

  knowledge-store/FileMemoryQueryStore
  (query-file-memory! [_ user-id repository-id relative-path limit]
    (with-session!
      driver
      database
      (fn [session]
        (-> (.run
             session
             "MATCH (repository:AdamRepository {id: $repositoryId})-[:CONTAINS]->(file:AdamCodeFile {relativePath: $relativePath})
              MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)-[:HAS_MEMORY]->(observation:AdamObservation)-[:ABOUT]->(file)
              OPTIONAL MATCH (observation)-[:SOURCED_FROM]->(source:AdamEntry)
              OPTIONAL MATCH (source)-[touch:TOUCHES]->(file)
              WITH observation, s, collect(DISTINCT source.entryId) AS sourceEntryIds,
                   [context IN collect(DISTINCT CASE WHEN touch IS NULL THEN null ELSE {entryId: source.entryId, commit: touch.commit, branch: touch.branch, dirty: touch.dirty} END) WHERE context IS NOT NULL] AS sourceContexts
              RETURN 'observation' AS kind, observation AS memory, s, sourceEntryIds, sourceContexts
              UNION ALL
              MATCH (repository:AdamRepository {id: $repositoryId})-[:CONTAINS]->(file:AdamCodeFile {relativePath: $relativePath})
              MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)-[:HAS_MEMORY]->(reflection:AdamReflection)-[:SUPPORTED_BY]->(observation:AdamObservation)-[:ABOUT]->(file)
              OPTIONAL MATCH (observation)-[:SOURCED_FROM]->(source:AdamEntry)
              OPTIONAL MATCH (source)-[touch:TOUCHES]->(file)
              WITH reflection, s, collect(DISTINCT source.entryId) AS sourceEntryIds,
                   [context IN collect(DISTINCT CASE WHEN touch IS NULL THEN null ELSE {entryId: source.entryId, commit: touch.commit, branch: touch.branch, dirty: touch.dirty} END) WHERE context IS NOT NULL] AS sourceContexts
              RETURN 'reflection' AS kind, reflection AS memory, s, sourceEntryIds, sourceContexts
              ORDER BY kind, memory.memoryId, s.piSessionId
              LIMIT $limit"
             #js {:userId user-id
                  :repositoryId repository-id
                  :relativePath relative-path
                  :limit ((.-int neo4j-driver) limit)})
            (.then (fn [result] (mapv file-memory-record (records result))))))))

  knowledge-store/CodeMemoryMigrationStore
  (code-memory-version! [_ user-id]
    (with-session!
      driver
      database
      (fn [session]
        (-> (.run
             session
             "MATCH (u:AdamUser {id: $userId})
              OPTIONAL MATCH (u)-[:OWNS]->(s:AdamSession)
              WHERE coalesce(s.sourceKind, 'pi') = 'pi'
              OPTIONAL MATCH (s)-[worked:WORKED_ON]->(:AdamRepository)
              WITH u, s,
                   CASE
                     WHEN worked.extractorVersion IS NULL
                       THEN coalesce(s.codeMemoryVersion, 0)
                     WHEN s.codeMemoryVersion IS NULL
                       THEN worked.extractorVersion
                     WHEN s.codeMemoryVersion < worked.extractorVersion
                       THEN s.codeMemoryVersion
                     ELSE worked.extractorVersion
                   END AS sessionVersion
              WITH u, count(DISTINCT s) AS sessionCount,
                   min(sessionVersion) AS projectedVersion
              RETURN CASE WHEN sessionCount = 0
                          THEN u.codeMemoryVersion
                          ELSE projectedVersion
                     END AS version"
             #js {:userId user-id})
            (.then
             (fn [result]
               (when-let [record (first (records result))]
                 (neo-integer (record-get record "version")))))))))

  (complete-code-memory-rebuild! [_ user-id version]
    (with-session!
      driver
      database
      (fn [^js session]
        (.executeWrite
         session
         (fn [tx]
           (-> (.run
                tx
                "MATCH (:AdamUser {id: $userId})-[:OWNS]->(repository:AdamRepository)
                 WHERE NOT EXISTS {
                   MATCH (:AdamSession)-[:WORKED_ON]->(repository)
                 }
                 DETACH DELETE repository"
                #js {:userId user-id})
               (.then
                (fn [_]
                  (.run
                   tx
                   "MATCH (u:AdamUser {id: $userId})
                    OPTIONAL MATCH (u)-[legacyOwnership:OWNS]->(:AdamRepository)
                    DELETE legacyOwnership"
                   #js {:userId user-id})))
               (.then
                (fn [_]
                  (.run
                   tx
                   "MATCH (u:AdamUser {id: $userId})
                    SET u.codeMemoryVersion = $version,
                        u.codeMemoryRebuiltAt = datetime()"
                   #js {:userId user-id :version version})))))))))

  store/SessionReplicaStore
  (initialize! [_ user]
    (initialize-schema! driver database user))

  (get-checkpoint! [_ session-id]
    (with-session!
      driver
      database
      (fn [session]
        (-> (.run
             session
             "MATCH (s:AdamSession {id: $sessionId})
              WHERE s.completeThroughOrdinal IS NOT NULL
              RETURN s.completeThroughOrdinal AS ordinal,
                     s.completeThroughByteOffset AS byteOffset,
                     s.committedPrefixHash AS prefixHash,
                     s.entryCount AS entryCount,
                     s.logHash AS logHash"
             #js {:sessionId session-id})
            (.then (fn [result]
                     (checkpoint-from-record (first (records result)))))))))

  (write-batch! [_ request]
    (with-session!
      driver
      database
      (fn [^js session]
        (.executeWrite session (fn [tx] (write-batch-transaction! tx request))))))

  (complete-session! [_ request]
    (with-session!
      driver
      database
      (fn [^js session]
        (.executeWrite session (fn [tx] (complete-session-transaction! tx request))))))

  (mark-conflict! [_ conflict]
    (with-session!
      driver
      database
      (fn [session]
        (.run
         session
         "MATCH (s:AdamSession {id: $sessionId})
          SET s.conflicted = true,
              s.conflictReason = $reason,
              s.conflictEntryId = $entryId,
              s.conflictExpectedHash = $expectedHash,
              s.conflictActualHash = $actualHash,
              s.conflictSourceFile = $sourceFile,
              s.conflictedAt = datetime()"
         #js {:sessionId (:session-id conflict)
              :reason (name (:reason conflict))
              :entryId (:entry-id conflict)
              :expectedHash (or (:expected-hash conflict) (:expected-offset conflict))
              :actualHash (or (:actual-hash conflict) (:actual-offset conflict))
              :sourceFile (:source-file conflict)}))))

  (list-sessions! [_ {:keys [user-id cwd]}]
    (with-session!
      driver
      database
      (fn [session]
        (-> (.run
             session
             "MATCH (:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession)
              WHERE s.cwd = $cwd
                AND coalesce(s.sourceKind, 'pi') = 'pi'
              RETURN s
              ORDER BY s.createdAt DESC, s.id"
             #js {:userId user-id :cwd cwd})
            (.then
             (fn [result]
               (mapv #(session-summary (.-properties (record-get % "s")))
                     (records result))))))))

  (read-session! [_ session-id]
    (with-session!
      driver
      database
      (fn [session]
        (-> (.run session
                  "MATCH (s:AdamSession {id: $sessionId})
                   WHERE coalesce(s.sourceKind, 'pi') = 'pi'
                   RETURN s"
                  #js {:sessionId session-id})
            (.then
             (fn [result]
               (when-let [record (first (records result))]
                 (let [properties (.-properties (record-get record "s"))]
                   (-> (.run
                        session
                        "MATCH (:AdamSession {id: $sessionId})-[:HAS_ENTRY]->(e:AdamEntry)
                         RETURN e
                         ORDER BY e.ordinal"
                        #js {:sessionId session-id})
                       (.then
                        (fn [entry-result]
                          {:session (session-summary properties)
                           :header-json (str (aget properties "headerJson"))
                           :entry-count (neo-integer (aget properties "entryCount"))
                           :log-hash (str (aget properties "logHash"))
                           :log-bytes (neo-integer (aget properties "logBytes"))
                           :has-final-newline? (= true (aget properties "hasFinalNewline"))
                           :entries
                           (mapv
                            (fn [entry-record]
                              (let [entry-properties (.-properties (record-get entry-record "e"))]
                                {:entry-id (str (aget entry-properties "entryId"))
                                 :parent-id (when (some? (aget entry-properties "parentId"))
                                              (str (aget entry-properties "parentId")))
                                 :ordinal (neo-integer (aget entry-properties "ordinal"))
                                 :raw-json (str (aget entry-properties "rawJson"))
                                 :payload-hash (str (aget entry-properties "payloadHash"))
                                 :payload-bytes (neo-integer (aget entry-properties "payloadBytes"))}))
                            (records entry-result))})))))))))))

  (close! [_]
    (.close driver)))

(defn replica-with-driver [driver database]
  (->Neo4jSessionReplica driver database))

(defn create-replica [{:keys [uri username password database]}]
  (let [^js auth-api (.-auth neo4j-driver)
        auth-token (.basic auth-api username password)
        driver-factory (.-driver neo4j-driver)
        driver (driver-factory uri auth-token)]
    (replica-with-driver driver database)))
