(ns adam.derivation.store-live-test
  (:require [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [cljs.test :refer [async deftest is]]
            ["neo4j-driver" :as neo4j]
            ["node:crypto" :refer [randomUUID]]))

(deftest live-refresh-lease-concurrency-expiry-and-failure-preservation
  (async done
    (let [env js/process.env
          acknowledged (= "1" (aget env "ADAM_TEST_NEO4J_DISPOSABLE"))
          uri (aget env "ADAM_TEST_NEO4J_URI") username (aget env "ADAM_TEST_NEO4J_USERNAME")
          password (aget env "ADAM_TEST_NEO4J_PASSWORD")]
      (if-not (and acknowledged uri username password)
        (do (is false "explicit disposable Neo4j acknowledgement and credentials are required") (done))
        (let [principal (str "urn:adam:user:" (randomUUID))
              database (or (aget env "ADAM_TEST_NEO4J_DATABASE") "neo4j")
              ^js driver ((.-driver neo4j) uri (.basic (.-auth neo4j) username password))
              ^js session (.session driver #js {:database database})
              db (store/with-driver driver database principal)
              request {:name "preferences"} other {:name "other"}
              ids (mapv #(core/derivation-id principal % {:type :none}) ["preferences" "other"])
              run-id (str "urn:adam:derivation-run:" (randomUUID))
              params #js {:readerId principal :ids (clj->js ids) :id (first ids) :runId run-id}
              original-token (atom nil)]
          (-> (store/ensure-schema! db)
              (.then (fn [_] (.run session "CREATE (:AdamUser {id: $readerId})" params)))
              (.then (fn [_]
                       (js/Promise.all #js [(store/acquire-refresh! db request 360000)
                                           (store/acquire-refresh! db request 360000)])))
              (.then (fn [results]
                       (is (= #{:acquired :busy} (set (map :status (array-seq results)))))
                       (reset! original-token (:token (first (filter #(= :acquired (:status %)) (array-seq results)))))
                       (store/acquire-refresh! db other 360000)))
              (.then (fn [lease]
                       (is (= :acquired (:status lease)))
                       (store/renew-refresh! db request @original-token 360000)))
              (.then (fn [result]
                       (is (= :renewed (:status result)))
                       (.run session "MATCH (d:AdamDerivation {id: $id})
                                      CREATE (r:AdamDerivationRun {id: $runId})
                                      CREATE (d)-[:HAS_RUN]->(r), (d)-[:LATEST_RUN]->(r)
                                      SET d.refreshLeaseUntil = timestamp() - 1" params)))
              (.then (fn [_] (store/renew-refresh! db request @original-token 360000)))
              (.then (fn [result]
                       (is (= :fenced (:status result)))
                       ;; A new store instance represents process restart; no in-memory lock state.
                       (store/acquire-refresh! (store/with-driver driver database principal) request 360000)))
              (.then (fn [lease]
                       (is (= :acquired (:status lease)))
                       (is (not= @original-token (:token lease)))
                       (-> (store/release-refresh! db request @original-token :timeout)
                           (.then (fn [result]
                                    (is (= :fenced (:status result)))
                                    (store/release-refresh! db request (:token lease) :timeout))))))
              (.then (fn [result]
                       (is (= :released (:status result)))
                       (.run session "MATCH (d:AdamDerivation {id: $id})-[:LATEST_RUN]->(r)
                                      RETURN r.id AS runId, d.refreshToken AS token, d.lastRefreshError AS code" params)))
              (.then (fn [^js result]
                       (let [^js row (first (array-seq (.-records result)))]
                         (is (= run-id (.get row "runId")))
                         (is (nil? (.get row "token")))
                         (is (= "timeout" (.get row "code"))))))
              (.catch (fn [error] (is false (.-stack error))))
              (.finally (fn []
                          ;; Delete only explicitly registered fixture resources, never owner-wide graphs.
                          (-> (.run session "MATCH (u:AdamUser {id: $readerId})-[:OWNS_DERIVATION]->(d:AdamDerivation)
                                              WHERE d.id IN $ids DETACH DELETE d" params)
                              (.then (fn [_] (.run session "MATCH (r:AdamDerivationRun {id: $runId}) DETACH DELETE r" params)))
                              (.then (fn [_] (.run session "MATCH (u:AdamUser {id: $readerId}) WHERE NOT (u)--() DELETE u" params)))
                              (.catch (fn [error] (is false (str "exact fixture cleanup failed: " (.-message error)))))
                              (.finally #(.close session))
                              (.finally #(.close driver)))))
              (.finally done)))))))
