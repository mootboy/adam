(ns adam.derivation.store
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [clojure.string :as string]
            [cljs.spec.alpha :as s]
            ["node:crypto" :refer [randomUUID]]
            ["neo4j-driver" :as neo4j]))

;; One concrete graph boundary for future Pi generation and both hosts' reads.
;; The principal is injected from host identity, never a query/tool argument.
(s/def ::principal (s/and ::core/reader-id #(re-matches #"urn:adam:user:[^:\s]+" %)))
(s/def ::database (config/text-spec 256))
(s/def ::lease-ms (s/and ::config/positive-integer #(<= % 86400000)))
(s/def ::token (config/text-spec 128))
(s/def ::failure #{:timeout :cancelled :executor-failed :invalid-output :input-too-large
                  :invalid-model :invalid-budget :invalid-memory :unresolved-evidence :unauthorized :schema-unavailable})

(defn with-driver [driver database principal]
  (when-not (and (s/valid? ::principal principal) (s/valid? ::database database))
    (config/reject! :unauthorized "configured graph principal and database are required"))
  {:driver driver :database database :principal principal})

(defn- with-session! [{:keys [driver database]} work]
  (-> (js/Promise.resolve)
      (.then (fn []
               (let [^js driver driver
                     ^js session (.session driver #js {:database database})]
                 (-> (js/Promise.resolve)
                     (.then #(work session))
                     (.finally #(.close session))))))))

(defn- rows [^js result] (array-seq (.-records result)))
(defn- field [^js record key] (.get record key))
(defn- integer [^js value] (if (number? value) value (.toNumber value)))

(def ^:private derivation-labels ["AdamDerivation" "AdamDerivationRun" "AdamDerivedObservation"])
(s/def ::schema-labels (s/and (s/coll-of string? :kind vector?)
                            #(every? (set %) derivation-labels)))

(defn- require-schema! [^js tx]
  (-> (.run tx
        "SHOW CONSTRAINTS YIELD type, labelsOrTypes, properties
         WHERE type = 'UNIQUENESS' AND properties = ['id']
         RETURN collect(labelsOrTypes[0]) AS labels")
      (.then (fn [result]
               (let [labels (when-let [row (first (rows result))] (vec (field row "labels")))]
                 (when-not (s/valid? ::schema-labels labels)
                   (config/reject! :schema-unavailable "initialize derivation id constraints before graph writes")))))))

(defn- write! [{:keys [principal] :as db} work]
  (with-session! db
    (fn [^js session]
      (.executeWrite session
        (fn [tx]
          (-> (require-schema! tx)
              (.then (fn [_]
                       (.run tx "MATCH (u:AdamUser {id: $readerId}) RETURN u.id AS userId"
                             #js {:readerId principal})))
              (.then (fn [result]
                       (if (seq (rows result)) (work tx) {:status :unavailable})))))))))

(defn ensure-schema! [db]
  (with-session! db
    (fn [^js session]
      (reduce (fn [pending label]
                (.then pending
                  (fn [_] (.run session
                            (str "CREATE CONSTRAINT " (.toLowerCase label) "_id IF NOT EXISTS "
                                 "FOR (n:" label ") REQUIRE n.id IS UNIQUE")))))
              (js/Promise.resolve nil)
              derivation-labels))))

(defn- bucket [{:keys [principal]} request]
  (when-not (and (s/valid? ::core/refresh-request request) (s/valid? ::config/name (:name request)))
    (config/reject! :invalid-selector "a configured name and single refresh selector are required"))
  (let [{:keys [name filter]} (core/normalize-read request)
        filter (or filter {:type :none})]
    {:readerId principal :derivationId (core/derivation-id principal name filter)
     :name name :filterType (clojure.core/name (:type filter)) :filterValue (:value filter)}))

(defn acquire-refresh! [db request lease-ms]
  (-> (js/Promise.resolve)
      (.then (fn []
               (when-not (s/valid? ::lease-ms lease-ms)
                 (config/reject! :invalid-lease "lease must be between 1 and 86400000 milliseconds; longer work renews it"))
               ;; Token allocation is outside the retried write transaction.
               (let [params (assoc (bucket db request) :token (randomUUID) :leaseMs ((.-int neo4j) lease-ms))]
                 (write! db
                   (fn [tx]
                     (-> (.run tx
                           "MATCH (u:AdamUser {id: $readerId})
                            MERGE (d:AdamDerivation {id: $derivationId})
                            ON CREATE SET d.name = $name, d.filterType = $filterType, d.filterValue = $filterValue
                            WITH u, d
                            WHERE NOT EXISTS { MATCH (other:AdamUser)-[:OWNS_DERIVATION]->(d) WHERE other <> u }
                            MERGE (u)-[:OWNS_DERIVATION]->(d)
                            SET d.refreshLockVersion = coalesce(d.refreshLockVersion, 0) + 1
                            WITH d, timestamp() AS now
                            WITH d, now, (d.refreshToken IS NULL OR coalesce(d.refreshLeaseUntil, 0) <= now) AS available
                            FOREACH (_ IN CASE WHEN available THEN [1] ELSE [] END |
                              SET d.refreshToken = $token, d.refreshLeaseUntil = now + $leaseMs,
                                  d.lastRefreshAttemptAt = now, d.lastRefreshOutcome = 'refreshing', d.lastRefreshError = null)
                            RETURN CASE WHEN available THEN 'acquired' ELSE 'busy' END AS status,
                                   CASE WHEN available THEN $token ELSE null END AS token,
                                   CASE WHEN available THEN d.refreshLeaseUntil ELSE null END AS leaseUntil"
                           (clj->js params))
                         (.then (fn [result]
                                  (if-let [row (first (rows result))]
                                    (if (= "acquired" (field row "status"))
                                      {:status :acquired :derivation-id (:derivationId params)
                                       :token (field row "token") :lease-until (integer (field row "leaseUntil"))}
                                      {:status :busy :derivation-id (:derivationId params)})
                                    {:status :unavailable})))))))))))

(defn renew-refresh! [db request token lease-ms]
  (-> (js/Promise.resolve)
      (.then (fn []
               (when-not (and (s/valid? ::token token) (s/valid? ::lease-ms lease-ms))
                 (config/reject! :invalid-lease "valid token and bounded renewal are required"))
               (let [params (assoc (bucket db request) :token token :leaseMs ((.-int neo4j) lease-ms))]
                 (write! db
                   (fn [tx]
                     (-> (.run tx
                           "MATCH (:AdamUser {id: $readerId})-[:OWNS_DERIVATION]->(d:AdamDerivation {id: $derivationId})
                            SET d.refreshLockVersion = coalesce(d.refreshLockVersion, 0) + 1
                            WITH d WHERE d.refreshToken = $token AND d.refreshLeaseUntil > timestamp()
                            SET d.refreshLeaseUntil = timestamp() + $leaseMs
                            RETURN d.refreshLeaseUntil AS leaseUntil"
                           (clj->js params))
                         (.then (fn [result]
                                  (if-let [row (first (rows result))]
                                    {:status :renewed :lease-until (integer (field row "leaseUntil"))}
                                    {:status :fenced})))))))))))

(defn release-refresh! [db request token failure]
  (-> (js/Promise.resolve)
      (.then (fn []
               (when-not (and (s/valid? ::token token) (s/valid? ::failure failure))
                 (config/reject! :invalid-lease "valid token and sanitized failure code are required"))
               (let [params (assoc (bucket db request) :token token :failure (name failure))]
                 (write! db
                   (fn [tx]
                     (-> (.run tx
                           "MATCH (:AdamUser {id: $readerId})-[:OWNS_DERIVATION]->(d:AdamDerivation {id: $derivationId})
                            SET d.refreshLockVersion = coalesce(d.refreshLockVersion, 0) + 1
                            WITH d WHERE d.refreshToken = $token
                            SET d.refreshToken = null, d.refreshLeaseUntil = null,
                                d.lastRefreshOutcome = 'failed', d.lastRefreshError = $failure
                            RETURN d.id AS id"
                           (clj->js params))
                         (.then (fn [result] {:status (if (seq (rows result)) :released :fenced)}))))))))))

;; Persist the core's complete, closed candidate, not arbitrary model/host data.
(s/def ::hash (s/and string? #(re-matches #"[0-9a-f]{64}" %)))
(s/def ::created-at
  (s/and (config/text-spec 24)
         #(re-matches #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z" %)
         #(try (= % (.toISOString (js/Date. %))) (catch :default _ false))))
(s/def ::configuration-version ::config/version)
(s/def ::configuration-fingerprint ::hash)
(s/def ::input-fingerprint ::hash)
(s/def ::configuration-snapshot-json (config/text-spec 1048576))
(s/def ::measured-input-tokens ::core/token-count)
(s/def ::input-memory-count ::core/token-count)
(s/def ::provider ::config/provider)
(s/def ::model-id ::config/id)
(s/def ::model (s/and ::core/model (config/only-keys #{:provider :id :context-window :max-output-tokens})))
(s/def ::dependency-fingerprints (s/map-of ::core/id ::hash))
(s/def ::dependency-roles (s/map-of ::core/id (s/coll-of #{:evidence :historical-support :comparison-dependency}
                                                       :kind set? :min-count 1)))
(s/def ::comparison-run-id (s/nilable ::core/id))
(s/def ::diagnostics (s/coll-of #{:comparison-unavailable} :kind vector? :max-count 100))
(s/def ::max-input-tokens ::config/positive-integer)
(s/def ::max-output-tokens ::config/positive-integer)
(s/def ::timeout-ms (s/and ::config/positive-integer #(<= % 2147483647)))
(s/def ::provider-headroom-tokens ::config/positive-integer)
(s/def ::limits (s/and (s/keys :req-un [::max-output-tokens ::timeout-ms] :opt-un [::max-input-tokens])
                      (config/only-keys #{:max-output-tokens :timeout-ms :max-input-tokens})))
(s/def ::effective-limits
  (s/and (s/keys :req-un [::max-input-tokens ::max-output-tokens ::timeout-ms ::provider-headroom-tokens])
         (config/only-keys #{:max-input-tokens :max-output-tokens :timeout-ms :provider-headroom-tokens})))
(s/def ::inferred-min-distinct-occasions ::config/positive-integer)
(s/def ::scope-types (s/coll-of #{"personal" "repository" "group" "session"} :kind vector? :min-count 1 :distinct true))
(s/def ::categories (s/map-of ::config/yaml-name (config/text-spec 8192) :max-count 64))
(s/def ::recipe
  (s/and (s/keys :req-un [::config/version ::config/guidance ::limits ::categories ::inferred-min-distinct-occasions ::scope-types]
                 :opt-un [::config/model])
         (config/only-keys #{:version :guidance :limits :categories :inferred-min-distinct-occasions :scope-types :model})))
(s/def ::policy-version ::config/positive-integer)
(s/def ::configuration-snapshot
  (s/and (s/keys :req-un [::policy-version ::recipe ::effective-limits])
         (config/only-keys #{:policy-version :recipe :effective-limits})))

(defn- configuration-valid? [run]
  (try
    (let [snapshot (js->clj (js/JSON.parse (:configuration-snapshot-json run)) :keywordize-keys true)]
      (and (s/valid? ::configuration-snapshot snapshot)
           (= (:configuration-version run) (get-in snapshot [:recipe :version]))
           (= (:configuration-fingerprint run) (core/fingerprint (select-keys snapshot [:policy-version :recipe])))))
    (catch :default _ false)))

(s/def ::publication
  (s/and ::core/saved-run
         (s/keys :req-un [::created-at ::configuration-version ::configuration-fingerprint ::configuration-snapshot-json
                          ::input-fingerprint ::model ::provider ::model-id ::input-memory-count ::measured-input-tokens
                          ::dependency-fingerprints ::dependency-roles ::comparison-run-id ::diagnostics ::core/links])
         (config/only-keys #{:id :derivation-id :created-at :configuration-version :configuration-fingerprint
                            :configuration-snapshot-json :input-fingerprint :model :provider :model-id
                            :input-memory-count :measured-input-tokens :input-memory-ids :dependency-fingerprints
                            :dependency-roles :comparison-run-id :diagnostics :conclusions :links})
         #(re-matches #"urn:adam:derivation-run:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}" (:id %))
         #(every? (fn [c] (re-matches #"urn:adam:derived-observation:[0-9a-f]{64}" (:id c))) (:conclusions %))
         #(s/valid? (s/coll-of ::core/link :kind vector? :distinct true) (:links %))
         #(= (set (:input-memory-ids %)) (set (keys (:dependency-fingerprints %))) (set (keys (:dependency-roles %))))
         #(or (:comparison-run-id %) (not-any? (fn [roles] (contains? roles :comparison-dependency)) (vals (:dependency-roles %))))
         #(= (:input-memory-count %) (count (filter (fn [[_ roles]] (some roles [:evidence :historical-support])) (:dependency-roles %))))
         #(every? (fn [id] (contains? (get (:dependency-roles %) id) :evidence))
                  (mapcat :supporting-memory-ids (:conclusions %)))
         #(every? (fn [id] (some (get (:dependency-roles %) id) [:evidence :historical-support]))
                  (mapcat :conflicting-memory-ids (:conclusions %)))
         #(= (:provider %) (get-in % [:model :provider]))
         #(= (:model-id %) (get-in % [:model :id]))
         configuration-valid?))

(defn- decode-run [json]
  (let [raw (js/JSON.parse json)]
    (-> (js->clj raw :keywordize-keys true)
        (assoc :dependency-fingerprints (js->clj (aget raw "dependency-fingerprints"))
               :dependency-roles (into {} (map (fn [[id roles]] [id (set (map keyword roles))])
                                             (js->clj (aget raw "dependency-roles")))))
        (update :diagnostics #(mapv keyword %))
        (update :conclusions #(mapv (fn [c] (-> c (update :basis keyword) (update :scope-type keyword))) %))
        (update :links #(mapv (fn [link] (update link :relation keyword)) %)))))

;; Resolve every declared ID independently of relationship completeness. Never
;; infer a reduced permission requirement from missing USED_MEMORY edges.
(defn- dependencies! [^js tx principal ids run-id]
  (-> (.run tx
        "UNWIND $memoryIds AS memoryId
         OPTIONAL MATCH (m) WHERE m.id = memoryId AND (m:AdamObservation OR m:AdamReflection)
         OPTIONAL MATCH (s:AdamSession)-[:HAS_MEMORY]->(m)
         OPTIONAL MATCH (owner:AdamUser)-[:OWNS]->(s)
         WITH memoryId, m, collect(DISTINCT owner.id) AS ownerIds, collect(DISTINCT s.id) AS sessionIds
         RETURN memoryId, ownerIds AS ownerIds, sessionIds,
                CASE WHEN m:AdamObservation AND NOT m:AdamReflection THEN 'observation'
                     WHEN m:AdamReflection AND NOT m:AdamObservation THEN 'reflection' END AS kind,
                m.producer AS producer, m.content AS content,
                (size(ownerIds) = 1 AND (ownerIds[0] = $readerId OR EXISTS {
                  MATCH (:AdamUser {id: ownerIds[0]})-[:GRANTS_MEMORY_ACCESS {permission: 'read'}]->(:AdamUser {id: $readerId})
                })) AS permitted,
                EXISTS { MATCH (:AdamDerivationRun {id: $runId})-[:USED_MEMORY]->(m) } AS linked"
        #js {:memoryIds (clj->js ids) :readerId principal :runId run-id})
      (.then (fn [result]
               (let [records (vec (rows result))
                     by-id (group-by #(field % "memoryId") records)]
                 (cond
                   (or (not= (set ids) (set (keys by-id)))
                       (some (fn [[_ matches]]
                               (or (not= 1 (count matches))
                                   (let [r (first matches)]
                                     (not (and (= 1 (count (field r "ownerIds")))
                                               (= 1 (count (field r "sessionIds")))
                                               (s/valid? ::core/memory
                                                 {:id (field r "memoryId") :owner-id (first (field r "ownerIds"))
                                                  :session-id (first (field r "sessionIds")) :kind (keyword (field r "kind"))
                                                  :producer (field r "producer") :content (field r "content")})))))) by-id))
                   {:status :unresolved-evidence}
                   (not-every? #(true? (field % "permitted")) records) {:status :unauthorized}
                   :else {:status :available :linked? (every? #(true? (field % "linked")) records)}))))))

(defn- require-dependencies! [tx principal ids run-id]
  (-> (dependencies! tx principal ids run-id)
      (.then (fn [result]
               (when-not (= :available (:status result))
                 (config/reject! (:status result) "complete authorized source dependencies are required"))
               result))))

(defn- candidate! [db request run]
  (let [params (bucket db request)]
    (when-not (and (s/valid? ::publication run)
                   (= core/policy-version (aget (js/JSON.parse (:configuration-snapshot-json run)) "policy-version"))
                   (= (:derivationId params) (:derivation-id run)))
      (config/reject! :invalid-output "a complete validated candidate for this derivation is required"))
    params))

(defn- authorized-prior! [^js tx params run]
  (if-not (:comparison-run-id run)
    (js/Promise.resolve nil)
    (-> (.run tx
          "MATCH (:AdamUser {id: $readerId})-[:OWNS_DERIVATION]->(d:AdamDerivation {id: $derivationId})
                 -[:HAS_RUN]->(r:AdamDerivationRun {id: $priorId}) RETURN r.runJson AS runJson"
          (clj->js (assoc params :priorId (:comparison-run-id run))))
        (.then (fn [result]
                 (when-not (= 1 (count (rows result)))
                   (config/reject! :unresolved-evidence "comparison history is unavailable"))
                 (let [prior (try (decode-run (field (first (rows result)) "runJson"))
                                  (catch :default _ nil))]
                   (when-not (and (s/valid? ::publication prior)
                                  (= (:id prior) (:comparison-run-id run))
                                  (= (:derivation-id prior) (:derivation-id run))
                                  (every? (set (:input-memory-ids run)) (:input-memory-ids prior))
                                  (every? #(contains? (get (:dependency-roles run) %) :comparison-dependency)
                                          (:input-memory-ids prior)))
                     (config/reject! :invalid-output "comparison dependencies must remain complete"))
                   (-> (require-dependencies! tx (:readerId params) (:input-memory-ids prior) (:id prior))
                       (.then (fn [_] prior)))))))))

(defn- valid-links? [run prior]
  (let [current (into {} (map (juxt :id identity) (:conclusions run)))
        previous (into {} (map (juxt :id identity) (:conclusions prior)))
        all (merge previous current)]
    (and (not-any? (set (keys previous)) (keys current))
         (every? (fn [{:keys [from to relation]}]
                   (let [source (get current from) target (get all to)]
                     (and source target (not= from to)
                          (= (:scope-type source) (:scope-type target))
                          (= (set (:scope-subject-ids source)) (set (:scope-subject-ids target)))
                          (or (= :contradicts relation) (contains? previous to))))) (:links run)))))

(defn- dependency-roles [run]
  (clj->js (into {} (map (fn [[id roles]] [id (vec (sort (map name roles)))]) (:dependency-roles run)))))

(defn- write-candidate! [^js tx params run]
  (let [parameters
        (clj->js (merge params
                  {:runId (:id run) :runJson (core/snapshot-json run) :createdAt (:created-at run)
                   :memoryIds (:input-memory-ids run) :configurationVersion (:configuration-version run)
                   :configurationFingerprint (:configuration-fingerprint run)
                   :configurationSnapshotJson (:configuration-snapshot-json run)
                   :inputFingerprint (:input-fingerprint run) :provider (:provider run) :modelId (:model-id run)
                   :inputMemoryCount (:input-memory-count run) :comparisonRunId (:comparison-run-id run)}))
        conclusions (clj->js (mapv (fn [c]
                     {:id (:id c) :content (:content c) :category (:category c) :basis (name (:basis c))
                      :scopeType (name (:scope-type c)) :scopeSubjectIds (:scope-subject-ids c)
                      :supportingMemoryIds (:supporting-memory-ids c) :conflictingMemoryIds (:conflicting-memory-ids c)})
                     (:conclusions run)))
        writes
        [["MATCH (r:AdamDerivationRun {id: $runId})
           UNWIND $memoryIds AS id MATCH (m) WHERE m.id = id AND (m:AdamObservation OR m:AdamReflection)
           MERGE (r)-[used:USED_MEMORY]->(m) SET used.roles = $dependencyRoles[id]"
          #js {:runId (:id run) :memoryIds (clj->js (:input-memory-ids run)) :dependencyRoles (dependency-roles run)}]
         ["MATCH (r:AdamDerivationRun {id: $runId}) UNWIND $conclusions AS input
           CREATE (c:AdamDerivedObservation {id: input.id})
           SET c.content = input.content, c.category = input.category, c.basis = input.basis,
               c.scopeType = input.scopeType, c.scopeSubjectIds = input.scopeSubjectIds,
               c.supportingMemoryIds = input.supportingMemoryIds, c.conflictingMemoryIds = input.conflictingMemoryIds
           CREATE (r)-[:HAS_CONCLUSION]->(c)
           WITH c, input
           CALL { WITH c, input UNWIND input.supportingMemoryIds AS id
                  MATCH (m) WHERE m.id = id AND (m:AdamObservation OR m:AdamReflection)
                  MERGE (c)-[:SUPPORTED_BY]->(m) }
           CALL { WITH c, input UNWIND input.conflictingMemoryIds AS id
                  MATCH (m) WHERE m.id = id AND (m:AdamObservation OR m:AdamReflection)
                  MERGE (c)-[:CONFLICTING_EVIDENCE]->(m) }"
          #js {:runId (:id run) :conclusions conclusions}]
         ["UNWIND $links AS input
           MATCH (a:AdamDerivedObservation {id: input.from}), (b:AdamDerivedObservation {id: input.to})
           FOREACH (_ IN CASE WHEN input.relation = 'contradicts' THEN [1] ELSE [] END | MERGE (a)-[:CONTRADICTS]->(b))
           FOREACH (_ IN CASE WHEN input.relation = 'supersedes' THEN [1] ELSE [] END | MERGE (a)-[:SUPERSEDES]->(b))"
          #js {:links (clj->js (mapv #(update % :relation name) (:links run)))}]]]
    (-> (.run tx
          "MATCH (d:AdamDerivation {id: $derivationId})
           WHERE d.refreshToken = $token AND d.refreshLeaseUntil > timestamp()
           CREATE (r:AdamDerivationRun {id: $runId})
           SET r.runJson = $runJson, r.createdAt = $createdAt,
               r.inputMemoryIds = $memoryIds, r.configurationVersion = $configurationVersion,
               r.configurationFingerprint = $configurationFingerprint,
               r.configurationSnapshotJson = $configurationSnapshotJson,
               r.inputFingerprint = $inputFingerprint, r.provider = $provider,
               r.modelId = $modelId, r.inputMemoryCount = $inputMemoryCount,
               r.comparisonRunId = $comparisonRunId
           CREATE (d)-[:HAS_RUN]->(r)
           WITH d, r OPTIONAL MATCH (d)-[old:LATEST_RUN]->() DELETE old
           CREATE (d)-[:LATEST_RUN]->(r)
           SET d.refreshToken = null, d.refreshLeaseUntil = null,
               d.lastRefreshOutcome = 'succeeded', d.lastRefreshError = null
           RETURN r.id AS id" parameters)
        (.then (fn [result]
                 (if-not (seq (rows result)) {:status :fenced}
                   (-> (reduce (fn [pending [query parameters]]
                                 (.then pending (fn [_] (.run tx query parameters))))
                               (js/Promise.resolve nil) writes)
                       (.then (fn [_] {:status :published :run-id (:id run)})))))))))

(defn publish-refresh! [db request token run]
  (-> (js/Promise.resolve)
      (.then (fn []
               (when-not (s/valid? ::token token)
                 (config/reject! :invalid-lease "valid publication token is required"))
               (let [params (assoc (candidate! db request run) :token token)]
                 (write! db
                   (fn [^js tx]
                     (-> (.run tx
                           "MATCH (:AdamUser {id: $readerId})-[:OWNS_DERIVATION]->(d:AdamDerivation {id: $derivationId})
                            SET d.refreshLockVersion = coalesce(d.refreshLockVersion, 0) + 1
                            WITH d WHERE d.refreshToken = $token AND d.refreshLeaseUntil > timestamp()
                            RETURN true AS leaseValid" (clj->js params))
                         (.then (fn [result]
                                  (if-not (seq (rows result)) {:status :fenced}
                                    (-> (require-dependencies! tx (:principal db) (:input-memory-ids run) nil)
                                        (.then (fn [_] (authorized-prior! tx params run)))
                                        (.then (fn [prior]
                                                 (when-not (valid-links? run prior)
                                                   (config/reject! :invalid-output "conclusion relationships require authorized compatible endpoints"))
                                                 (write-candidate! tx params run)))))))))))))))

;; Capture immutable run metadata first. Authorization is always resolved from
;; current source ownership, never from a cached list of permitted users.
(defn- run-records! [^js tx principal request & [derivation-id]]
  (let [{:keys [name filter history? limit]} (core/normalize-read request)]
    (-> (.run tx
          "MATCH (:AdamUser {id: $readerId})-[:OWNS_DERIVATION]->(d:AdamDerivation)-[:HAS_RUN]->(r:AdamDerivationRun)
           WHERE ($name IS NULL OR d.name = $name)
             AND ($derivationId IS NULL OR d.id = $derivationId)
             AND ($filterType IS NULL OR (d.filterType = $filterType AND d.filterValue = $filterValue))
             AND ($history OR EXISTS { MATCH (d)-[:LATEST_RUN]->(r) })
           WITH DISTINCT d, r ORDER BY r.createdAt DESC, r.id LIMIT $limit
           OPTIONAL MATCH (parent:AdamDerivation)-[:HAS_RUN]->(r)
           OPTIONAL MATCH (r)-[:HAS_CONCLUSION]->(c:AdamDerivedObservation)
           RETURN d.id AS derivationId, d.name AS name, d.filterType AS filterType, d.filterValue AS filterValue,
                  r.id AS runId, r.createdAt AS createdAt, r.runJson AS runJson, r.inputMemoryIds AS memoryIds,
                  collect(DISTINCT parent.id) AS parents,
                  (size([(r)-[:USED_MEMORY]->() | 1]) = size(r.inputMemoryIds)
                   AND all(m IN [(r)-[:USED_MEMORY]->(m) | m] WHERE m.id IN r.inputMemoryIds)) AS dependenciesLinked,
                  collect(DISTINCT c {.id, .content, .category, .basis, .scopeType, .scopeSubjectIds,
                                      .supportingMemoryIds, .conflictingMemoryIds,
                                      linked: all(id IN c.supportingMemoryIds WHERE EXISTS {
                                        MATCH (c)-[:SUPPORTED_BY]->(m) WHERE m.id = id
                                      }) AND all(id IN c.conflictingMemoryIds WHERE EXISTS {
                                        MATCH (c)-[:CONFLICTING_EVIDENCE]->(m) WHERE m.id = id
                                      })}) AS conclusions
           ORDER BY createdAt DESC, runId"
          #js {:readerId principal :derivationId derivation-id :name name :filterType (when filter (clojure.core/name (:type filter)))
               :filterValue (:value filter) :history history? :limit ((.-int neo4j) (inc limit))})
        (.then (fn [result] {:records (vec (take limit (rows result))) :truncated? (> (count (rows result)) limit)})))))

(defn- stored-run [principal record]
  (try
    (let [run (decode-run (field record "runJson"))
          filter (cond-> {:type (keyword (field record "filterType"))}
                   (field record "filterValue") (assoc :value (field record "filterValue")))
          name (field record "name")
          derivation (field record "derivationId")
          graph (mapv (fn [c] {:id (aget c "id") :content (aget c "content") :category (aget c "category")
                              :basis (keyword (aget c "basis")) :scope-type (keyword (aget c "scopeType"))
                              :scope-subject-ids (vec (aget c "scopeSubjectIds"))
                              :supporting-memory-ids (vec (aget c "supportingMemoryIds"))
                              :conflicting-memory-ids (vec (aget c "conflictingMemoryIds"))})
                      (field record "conclusions"))]
      (when (and (s/valid? ::publication run) (s/valid? ::config/name name)
                 (contains? #{:none :repository :session :language} (:type filter))
                 (= derivation (core/derivation-id principal name filter) (:derivation-id run))
                 (= (:id run) (field record "runId"))
                 (= [derivation] (vec (field record "parents")))
                 (= (:input-memory-ids run) (vec (field record "memoryIds")))
                 (= (sort-by :id graph) (sort-by :id (:conclusions run))))
        {:name name :filter filter :run run
         :conclusions-linked? (every? #(true? (aget % "linked")) (field record "conclusions"))
         :dependencies-linked? (true? (field record "dependenciesLinked"))}))
    (catch :default _ nil)))

(defn- read-record! [tx principal record]
  (if-let [{:keys [run conclusions-linked? dependencies-linked?] :as saved} (stored-run principal record)]
    (-> (dependencies! tx principal (:input-memory-ids run) (:id run))
        (.then (fn [result]
                 (cond
                   (not= :available (:status result)) {:status (:status result) :run-id (:id run)}
                   (not (and (:linked? result) conclusions-linked? dependencies-linked?)) {:status :unresolved-evidence :run-id (:id run)}
                   :else (assoc (dissoc saved :conclusions-linked? :dependencies-linked?) :status :available)))))
    (js/Promise.resolve {:status :unresolved-evidence :run-id (field record "runId")})))

;; All generation evidence is loaded before processing, including unlinked
;; memories and dropped support records. No LIMIT, file join or recipe filtering
;; may silently reduce this snapshot; core applies the one selected filter.
(s/def ::input-owner-ids (s/coll-of ::principal :kind vector? :count 1))
(s/def ::input-session-ids (s/coll-of ::core/session :kind vector? :count 1))
(s/def ::source-kind #{"pi" "claude-code"})
(s/def ::source-session-id (config/text-spec 2048))
(s/def ::input-source
  (s/and (s/keys :req-un [::input-owner-ids ::input-session-ids ::source-kind ::source-session-id])
         #(= [(:source-kind %) (:source-session-id %)]
             (vec (rest (re-matches #"urn:adam:session:[^:\s]+:([^:\s]+):([^\s]+)"
                                   (first (:input-session-ids %))))))))

(defn- invalid-input-source! []
  (throw (ex-info "source ownership, identity or supports are inconsistent"
                  {:type :derivation :code :unresolved-evidence :invalid-source-count 1})))

(defn- input-producer [source-kind producer]
  (if (and (= "pi" source-kind) (string/blank? producer)) "pi-observational-memory" producer))

(defn- input-supports [value source-kind session-id records]
  (let [declared (vec (or (aget value "supportingObservationIds") []))
        ids (mapv #(aget % "id") records)
        producer (input-producer source-kind (aget value "producer"))
        record-spec (s/and
                      #(s/valid? ::core/id (aget % "id"))
                      #(contains? (set declared) (aget % "memoryId"))
                      #(= producer (input-producer source-kind (aget % "producer")))
                      #(= [session-id] (vec (distinct (aget % "sessionIds")))))]
    (when-not (and (seq declared) (s/valid? ::core/identity-vector declared)
                  (s/valid? ::core/identity-vector ids)
                  (= (count declared) (count records))
                  (= (set declared) (set (map #(aget % "memoryId") records)))
                  (s/valid? (s/coll-of record-spec :kind vector?) records))
      (invalid-input-source!))
    ids))

(defn- input-memories! [^js tx principal]
  (-> (.run tx
        "MATCH (reader:AdamUser {id: $readerId})
         CALL { WITH reader RETURN reader AS owner
                UNION
                WITH reader MATCH (owner:AdamUser)-[:GRANTS_MEMORY_ACCESS {permission: 'read'}]->(reader)
                RETURN owner }
         WITH DISTINCT owner
         MATCH (owner)-[:OWNS]->(:AdamSession)-[:HAS_MEMORY]->(m)
         WHERE m:AdamObservation OR m:AdamReflection
         WITH DISTINCT m
         MATCH (allOwner:AdamUser)-[:OWNS]->(s:AdamSession)-[:HAS_MEMORY]->(m)
         WITH m, collect(DISTINCT allOwner.id) AS ownerIds, collect(DISTINCT s.id) AS sessionIds,
              collect(DISTINCT s) AS sessions
         WITH m, ownerIds, sessionIds, sessions[0] AS s
         CALL { WITH m, s OPTIONAL MATCH (m)-[:SOURCED_FROM]->(entry:AdamEntry)<-[:HAS_ENTRY]-(s)
                RETURN collect(DISTINCT entry {.entryId, streamId: coalesce(entry.streamId, 'main')}) AS sourceEntries }
         CALL { WITH m OPTIONAL MATCH (m)-[:ABOUT]->(direct:AdamCodeFile)
                WITH m, collect(DISTINCT direct.relativePath) AS directPaths
                OPTIONAL MATCH (m:AdamReflection)-[:SUPPORTED_BY]->(:AdamObservation)-[:ABOUT]->(supported:AdamCodeFile)
                WITH directPaths, collect(DISTINCT supported.relativePath) AS supportedPaths
                RETURN directPaths + supportedPaths AS linkedPaths }
         CALL { WITH m OPTIONAL MATCH (m)-[:SUPPORTED_BY]->(support:AdamObservation)
                RETURN collect(DISTINCT support {.id, .memoryId, .producer,
                  sessionIds: [(support)<-[:HAS_MEMORY]-(supportSession:AdamSession) | supportSession.id]}) AS supportRecords }
         CALL { WITH s OPTIONAL MATCH (s)-[:WORKED_ON]->(repo:AdamRepository)
                RETURN collect(DISTINCT repo.normalizedOrigin) AS repositoryOrigins,
                       collect(DISTINCT repo.id) AS repositoryIds }
         RETURN ownerIds, sessionIds, s.sourceKind AS sourceKind,
                coalesce(s.sourceSessionId, s.piSessionId) AS sourceSessionId,
                CASE WHEN size(ownerIds) = 1 AND size(sessionIds) = 1
                     THEN m {.id, .content, .producer, .dropped, .timestamp, .relevance,
                             .recordingEntryId, .recordingEventId, .recordingMemoryRecordId,
                             .supportingObservationIds} END AS inputMemory,
                CASE WHEN m:AdamObservation AND NOT m:AdamReflection THEN 'observation'
                     WHEN m:AdamReflection AND NOT m:AdamObservation THEN 'reflection' END AS kind,
                sourceEntries, linkedPaths, repositoryOrigins, repositoryIds, supportRecords
         ORDER BY m.id"
        #js {:readerId principal})
      (.then (fn [result]
               (mapv
                 (fn [row]
                   (let [owners (vec (field row "ownerIds")) sessions (vec (field row "sessionIds"))
                         value (field row "inputMemory") kind (keyword (field row "kind"))
                         source-kind (field row "sourceKind") source-id (field row "sourceSessionId")]
                     (when-not (and value (s/valid? ::core/kind kind)
                                    (s/valid? ::input-source
                                              {:input-owner-ids owners :input-session-ids sessions
                                               :source-kind source-kind :source-session-id source-id}))
                       (invalid-input-source!))
                     (cond-> {:id (aget value "id") :owner-id (first owners) :session-id (first sessions)
                              :kind kind :producer (input-producer source-kind (aget value "producer")) :content (aget value "content")
                              :dropped? (and (= :observation kind) (= true (aget value "dropped")))
                              :source-kind source-kind :source-session-id source-id
                              :timestamp (aget value "timestamp") :relevance (aget value "relevance")
                              :recording-entry-id (aget value "recordingEntryId")
                              :recording-event-id (aget value "recordingEventId")
                              :recording-record-id (aget value "recordingMemoryRecordId")
                              :source-entries (mapv (fn [entry] {:stream-id (aget entry "streamId") :entry-id (aget entry "entryId")})
                                                   (field row "sourceEntries"))
                              ;; Never treat citation strings as verified occasions.
                              :source-occasions []
                              :linked-paths (vec (field row "linkedPaths"))
                              :repository-origins (vec (field row "repositoryOrigins"))
                              :repository-ids (vec (field row "repositoryIds"))}
                       (= :reflection kind)
                       (assoc :supporting-memory-ids
                              (input-supports value source-kind (first sessions) (vec (field row "supportRecords")))))))
                 (rows result))))))

(defn load-input! [db request]
  (-> (js/Promise.resolve)
      (.then (fn []
               (let [params (bucket db request) principal (:principal db)]
                 (with-session! db
                   (fn [^js session]
                     (.executeRead session
                       (fn [^js tx]
                         (-> (.run tx "MATCH (u:AdamUser {id: $readerId}) RETURN u.id AS userId" #js {:readerId principal})
                             (.then (fn [result]
                                      (when-not (= 1 (count (rows result)))
                                        (config/reject! :unauthorized "source reader must resolve uniquely"))
                                      (input-memories! tx principal)))
                             (.then (fn [memories]
                                      (let [snapshot {:reader-id principal :memories memories
                                                      ;; This DTO grant list reflects the Cypher authorization predicate,
                                                      ;; not independent grant discovery. Publication reauthorizes it.
                                                      :grants (mapv (fn [owner] {:owner-id owner :reader-id principal :permission "read"})
                                                                    (sort (disj (set (map :owner-id memories)) principal)))}]
                                        (-> (run-records! tx principal (assoc request :limit 1) (:derivationId params))
                                            (.then (fn [{:keys [records truncated?]}]
                                                     (cond
                                                       (empty? records) snapshot
                                                       truncated? (assoc snapshot :comparison {:id "unavailable"})
                                                       :else
                                                       (-> (read-record! tx principal (first records))
                                                           (.then (fn [prior]
                                                                    (assoc snapshot :comparison
                                                                           (if (= :available (:status prior))
                                                                             (:run prior) {:id "unavailable"}))))))))))))))))))))))

(defn read-runs! [db request]
  (-> (js/Promise.resolve)
      (.then (fn []
               (core/normalize-read request)
               (with-session! db
                 (fn [^js session]
                   (.executeRead session
                     (fn [tx]
                       (-> (run-records! tx (:principal db) request)
                           (.then (fn [{:keys [records truncated?]}]
                                    (-> (reduce (fn [pending record]
                                                  (.then pending (fn [items]
                                                                   (-> (read-record! tx (:principal db) record)
                                                                       (.then (fn [item] (conj items item)))))))
                                                (js/Promise.resolve []) records)
                                        (.then (fn [items] {:items items :truncated? truncated?}))))))))))))))

(defn- repair-snapshot! [tx principal record]
  (if-let [{:keys [run]} (stored-run principal record)]
    (-> (dependencies! tx principal (:input-memory-ids run) (:id run))
        (.then (fn [result] (assoc result :run run :record record :run-id (:id run)))))
    (js/Promise.resolve {:status :unresolved-evidence :run-id (field record "runId")})))

(defn- restore-links! [^js tx principal {:keys [status run record run-id]}]
  (if-not (= :available status)
    (js/Promise.resolve {:status status :run-id run-id})
    (-> (.run tx
          "MATCH (:AdamUser {id: $readerId})-[:OWNS_DERIVATION]->(d)-[:HAS_RUN]->(r:AdamDerivationRun {id: $runId})
           WHERE r.runJson = $runJson
           OPTIONAL MATCH (r)-[old:USED_MEMORY]->() DELETE old
           WITH DISTINCT r
           CALL { WITH r UNWIND r.inputMemoryIds AS id MATCH (m)
                  WHERE m.id = id AND (m:AdamObservation OR m:AdamReflection)
                  MERGE (r)-[used:USED_MEMORY]->(m) SET used.roles = $dependencyRoles[id] }
           WITH r OPTIONAL MATCH (r)-[:HAS_CONCLUSION]->(c)
           OPTIONAL MATCH (c)-[old:SUPPORTED_BY|CONFLICTING_EVIDENCE]->() DELETE old
           WITH DISTINCT r, c
           CALL { WITH c UNWIND c.supportingMemoryIds AS id MATCH (m)
                  WHERE m.id = id AND (m:AdamObservation OR m:AdamReflection)
                  MERGE (c)-[:SUPPORTED_BY]->(m) }
           CALL { WITH c UNWIND c.conflictingMemoryIds AS id MATCH (m)
                  WHERE m.id = id AND (m:AdamObservation OR m:AdamReflection)
                  MERGE (c)-[:CONFLICTING_EVIDENCE]->(m) }
           RETURN DISTINCT r.id AS id"
          #js {:readerId principal :runId (:id run) :runJson (field record "runJson") :dependencyRoles (dependency-roles run)})
        (.then (fn [result] {:status (if (seq (rows result)) :repaired :unresolved-evidence) :run-id run-id})))))

(defn- sequential! [records work]
  (reduce (fn [pending record]
            (.then pending (fn [items]
                             (-> (work record) (.then (fn [item] (conj items item)))))))
          (js/Promise.resolve []) records))

(defn repair-dependencies! [db request]
  (-> (js/Promise.resolve)
      (.then (fn []
               (core/normalize-read request)
               (write! db
                 (fn [tx]
                   (-> (run-records! tx (:principal db) (assoc request :history true))
                       (.then (fn [{:keys [records truncated?]}]
                                ;; Capture all candidate/dependency state before repairing any.
                                (-> (sequential! records #(repair-snapshot! tx (:principal db) %))
                                    (.then (fn [snapshots]
                                             (sequential! snapshots #(restore-links! tx (:principal db) %))))
                                    (.then (fn [items] {:items items :truncated? truncated?}))))))))))))
