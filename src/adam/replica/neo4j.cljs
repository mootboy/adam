(ns adam.replica.neo4j
  (:require [clojure.string :as string]))

(def ^:private session-labels
  ["AdamUser" "AdamIdentity" "AdamSession" "AdamEntry"])

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

(defn ensure-session-constraints! [driver database]
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
        session-labels)))))

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
