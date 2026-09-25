(ns adam.replica.neo4j
  (:require [adam.knowledge.store :as knowledge-store]
            [adam.replica.store :as store]
            [clojure.string :as string]
            ["neo4j-driver" :as neo4j-driver]))

(def ^:private session-labels
  ["AdamUser" "AdamIdentity" "AdamSession" "AdamEntry"])

(def ^:private file-evidence-labels
  ["AdamRepository" "AdamCodeFile"])

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
  (ensure-label-constraints! driver database file-evidence-labels))

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

(defn- entry-properties [session-id entry]
  (let [properties #js {}]
    (doseq [[key value include-nil?]
            [["id" (:id entry) false]
             ["sessionId" session-id false]
             ["entryId" (:entry-id entry) false]
             ["type" (:type entry) false]
             ["role" (:role entry) false]
             ["parentId" (:parent-id entry) true]
             ["timestamp" (:timestamp entry) false]
             ["ordinal" (:ordinal entry) false]
             ["rawJson" (:raw-json entry) false]
             ["payloadHash" (:payload-hash entry) false]
             ["payloadBytes" (:payload-bytes entry) false]
             ["parentUrn" (:parent-urn entry) false]]]
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
          #js {:entries (clj->js (mapv #(entry-properties (:id session) %) entries))})))
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
               :entries (clj->js (mapv #(entry-properties (:id session) %) entries))})))
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

(defn- repository-properties [repository]
  (let [properties #js {:id (:id repository)
                        :root (:root repository)}]
    (when-let [remote (:normalized-remote repository)]
      (aset properties "normalizedRemote" remote))
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

(defn- index-file-evidence-transaction! [^js tx projection]
  (let [repository (:repository projection)]
    (-> (.run
         tx
         "MATCH (s:AdamSession {id: $sessionId})-[:HAS_ENTRY]->(entry)
          OPTIONAL MATCH (entry)-[touch:TOUCHES]->()
          DELETE touch"
         #js {:sessionId (:session-id projection)})
        (.then
         (fn [_]
           (.run
            tx
            "MATCH (u:AdamUser {id: $userId}), (s:AdamSession {id: $sessionId})
             OPTIONAL MATCH (s)-[old:WORKED_ON]->()
             DELETE old
             WITH u, s
             MERGE (repository:AdamRepository {id: $repository.id})
             SET repository.root = $repository.root,
                 repository.normalizedRemote = $repository.normalizedRemote
             MERGE (u)-[:OWNS]->(repository)
             MERGE (s)-[worked:WORKED_ON]->(repository)
             SET worked.commit = $commit,
                 worked.branch = $branch,
                 worked.dirty = $dirty,
                 worked.extractorVersion = $extractorVersion,
                 worked.indexedAt = datetime()"
            #js {:userId (:user-id projection)
                 :sessionId (:session-id projection)
                 :repository (repository-properties repository)
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
                 :extractorVersion (:extractor-version projection)}))))))

(defn- session-summary [properties]
  {:id (str (aget properties "id"))
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

(defrecord Neo4jSessionReplica [driver database]
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
                  "MATCH (s:AdamSession {id: $sessionId}) RETURN s"
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
