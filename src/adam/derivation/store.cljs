(ns adam.derivation.store
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [cljs.spec.alpha :as s]
            ["node:crypto" :refer [randomUUID]]))

;; One concrete graph boundary for future Pi generation and both hosts' reads.
;; The principal is injected from host identity, never a query/tool argument.
(s/def ::principal (s/and ::core/reader-id #(re-matches #"urn:adam:user:[^:\s]+" %)))
(s/def ::database (config/text-spec 256))
(s/def ::lease-ms (s/and ::config/positive-integer #(<= % 86400000)))
(s/def ::token (config/text-spec 128))
(s/def ::failure #{:timeout :cancelled :executor-failed :invalid-output :input-too-large
                  :invalid-model :invalid-budget :invalid-memory :unresolved-evidence :unauthorized})

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

(defn- write! [{:keys [principal] :as db} work]
  (with-session! db
    (fn [^js session]
      (.executeWrite session
        (fn [tx]
          (-> (.run tx "MATCH (u:AdamUser {id: $readerId}) RETURN u.id AS userId"
                    #js {:readerId principal})
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
              ["AdamDerivation" "AdamDerivationRun" "AdamDerivedObservation"]))))

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
               (let [params (assoc (bucket db request) :token (randomUUID) :leaseMs lease-ms)]
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
               (let [params (assoc (bucket db request) :token token :leaseMs lease-ms)]
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
