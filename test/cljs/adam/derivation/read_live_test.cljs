(ns adam.derivation.read-live-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.derivation.read :as read]
            [adam.derivation.store :as store]
            [cljs.test :refer [async deftest is]]
            ["neo4j-driver" :as neo4j]
            ["node:crypto" :refer [randomUUID]]))

(when-not (= "1" (aget js/process.env "ADAM_TEST_NEO4J_DISPOSABLE"))
  (throw (js/Error. "Live derivation reads require ADAM_TEST_NEO4J_DISPOSABLE=1")))

(deftest live-read-freshness-provenance-revocation-and-repair
  (async done
    (let [env js/process.env uuid (randomUUID) foreign (randomUUID)
          reader (str "urn:adam:user:" uuid) owner (str "urn:adam:user:" foreign)
          sids (mapv #(str "urn:adam:session:" % ":pi:read-fixture") [uuid foreign])
          mids (mapv #(str "urn:adam:observation:" % ":pi:read-fixture:fixture:m") [uuid foreign])
          eids (mapv #(str "urn:adam:entry:" % ":pi:read-fixture:request") [uuid foreign])
          name "read-preferences" filter {:type :none}
          did (core/derivation-id reader name filter)
          ^js driver ((.-driver neo4j) (aget env "ADAM_TEST_NEO4J_URI")
                       (.basic (.-auth neo4j) (aget env "ADAM_TEST_NEO4J_USERNAME") (aget env "ADAM_TEST_NEO4J_PASSWORD")))
          database (or (aget env "ADAM_TEST_NEO4J_DATABASE") "neo4j")
          ^js session (.session driver #js {:database database}) db (store/with-driver driver database reader)
          recipes (config/parse-config "derivations:\n  read-preferences:\n    version: 1\n    guidance: Preserve attribution.\n")
          params #js {:reader reader :owner owner :sessions (clj->js sids) :memories (clj->js mids) :entries (clj->js eids)}
          registered (atom (into [reader owner did] (concat sids mids eids)))
          deps {:load-configuration! (fn [] recipes) :load-snapshot! #(store/read-snapshot! db %1 %2)}
          request {:name name} model-calls (atom 0) saved (atom nil) original (atom nil)]
      (-> (store/ensure-schema! db)
          (.then (fn [_] (store/await-schema! db)))
          (.then (fn [_]
                   (.run session
                     "CREATE (reader:AdamUser {id: $reader}), (owner:AdamUser {id: $owner})
                      CREATE (owner)-[:GRANTS_MEMORY_ACCESS {permission: 'read'}]->(reader)
                      WITH [reader, owner] AS users UNWIND range(0, 1) AS i
                      WITH users[i] AS user, i
                      CREATE (s:AdamSession {id: $sessions[i], sourceKind: 'pi', sourceSessionId: 'read-fixture'})
                      CREATE (m:AdamObservation {id: $memories[i], producer: 'fixture', content: 'Small interfaces', memoryId: 'm'})
                      CREATE (e:AdamEntry {id: $entries[i], sessionId: $sessions[i], entryId: 'request', streamId: 'main',
                        rawJson: '{\"type\":\"message\",\"id\":\"request\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"Use small interfaces.\"}]}}'})
                      CREATE (user)-[:OWNS]->(s), (s)-[:HAS_MEMORY]->(m), (s)-[:HAS_ENTRY]->(e), (m)-[:SOURCED_FROM]->(e)" params)))
          (.then (fn [_] (store/load-input! db request)))
          (.then (fn [snapshot]
                   (core/refresh!
                    {:measure-input (constantly 200)
                     :execute! (fn [_] (swap! model-calls inc)
                                 {:finish-reason :stop :conclusions
                                  [{:id "c" :content "Small scoped interfaces" :category "architecture" :basis :explicit
                                    :scope-type :personal :scope-subject-ids [reader]
                                    :supporting-memory-ids [(first mids)] :conflicting-memory-ids []}
                                   {:id "alternative" :content "Competing scoped position" :category "architecture" :basis :explicit
                                    :scope-type :personal :scope-subject-ids [reader]
                                    :supporting-memory-ids [(first mids)] :conflicting-memory-ids []}]
                                  :links [{:relation :contradicts :from "c" :to "alternative"}]})}
                    (merge snapshot {:name name :filter filter :recipe (get recipes name)
                                     :model {:provider "fake" :id "fixture" :context-window 30000 :max-output-tokens 8192}}))))
          (.then (fn [{:keys [run]}]
                   (reset! original run)
                   (reset! saved run)
                   (swap! registered into (cons (:id run) (map :id (:conclusions run))))
                   (-> (store/acquire-refresh! db request 60000)
                       (.then #(store/publish-refresh! db request (:token %) run)))))
          ;; Validate cross-run contradiction and scoped supersession through
          ;; publication + saved history, not only the pure output validator.
          (.then (fn [_]
                   (reduce
                    (fn [pending relation]
                      (.then pending
                        (fn [_]
                          (-> (store/load-input! db request)
                              (.then (fn [snapshot]
                                       (core/refresh!
                                        {:measure-input (constantly 200)
                                         :execute! (fn [_]
                                                     (swap! model-calls inc)
                                                     {:finish-reason :stop
                                                      :conclusions [{:id "c" :content (str "Small scoped interfaces - " (clojure.core/name relation)) :category "architecture" :basis :explicit
                                                                     :scope-type :personal :scope-subject-ids [reader]
                                                                     :supporting-memory-ids [(first mids)] :conflicting-memory-ids []}]
                                                      :links [{:relation relation :from "c" :to (get-in snapshot [:comparison :conclusions 0 :id])}]})}
                                        (merge snapshot {:name name :filter filter :recipe (get recipes name)
                                                         :model {:provider "fake" :id "fixture" :context-window 30000 :max-output-tokens 8192}}))))
                              (.then (fn [{:keys [run]}]
                                       (reset! saved run)
                                       (swap! registered into (cons (:id run) (map :id (:conclusions run))))
                                       (-> (store/acquire-refresh! db request 60000)
                                           (.then #(store/publish-refresh! db request (:token %) run)))))))))
                    (js/Promise.resolve) [:contradicts :supersedes])))
          (.then (fn [_] (read/execute! deps (assoc request :history true))))
          (.then (fn [result]
                   (is (= 3 (get-in result [:details :result-count])))
                   (is (.includes (:content result) "CONTRADICTS"))
                   (is (.includes (:content result) "SUPERSEDES"))
                   (is (not (.includes (:content result) "Supports:")))
                   (read/execute! deps (assoc request :limit 1 :evidence (get-in @original [:conclusions 0 :id])))))
          (.then (fn [result]
                   (is (= 1 (get-in result [:details :result-count])))
                   (is (.includes (:content result) (:id @original)) "exact evidence lookup finds an older non-latest run")
                   (is (.includes (:content result) "request"))
                   (is (.includes (:content result) "Evidence text: \"Small interfaces\""))
                   (is (not (.includes (:content result) "Competing scoped position")))
                   (read/execute! deps request)))
          (.then (fn [result]
                   (is (.includes (:content result) "current"))
                   (is (.includes (:content result) "Small scoped interfaces"))
                   (is (.includes (:content result) "fixture"))
                   (is (not (.includes (:content result) "Supports:")))
                   (is (not (.includes (:content result) "Use small interfaces.")) "raw requests are not returned")
                   ;; A different transaction revokes access after streaming has
                   ;; begun. Read-committed reads must reauthorize afterwards.
                   (let [revoked? (atom false)
                         delayed-driver
                         #js {:session (fn [options]
                                         (let [real (.session driver options)]
                                           #js {:close #(.close real)
                                                :executeRead (fn [work]
                                                               (.executeRead real
                                                                 (fn [tx]
                                                                   (work
                                                                    #js {:run (fn [query arguments]
                                                                                (let [result (.run tx query arguments)]
                                                                                  (if (.includes query "AS activityEntry")
                                                                                    #js {:subscribe (fn [observer]
                                                                                                      (.subscribe result
                                                                                                        #js {:onNext (aget observer "onNext") :onError (aget observer "onError")
                                                                                                             :onCompleted (fn [summary]
                                                                                                                            (-> (if (compare-and-set! revoked? false true)
                                                                                                                                  (.run session "MATCH (o:AdamUser {id: $owner})-[g:GRANTS_MEMORY_ACCESS]->(:AdamUser {id: $reader}) DELETE g" params)
                                                                                                                                  (js/Promise.resolve))
                                                                                                                                (.then (fn [_] ((aget observer "onCompleted") summary)))
                                                                                                                                (.catch (aget observer "onError"))))}))}
                                                                                    result)))}))))}))}
                         delayed-db (store/with-driver delayed-driver database reader)]
                     (-> (read/execute! (assoc deps :load-snapshot! #(store/read-snapshot! delayed-db %1 %2))
                                      (assoc request :evidence (get-in @original [:conclusions 0 :id])))
                         (.then (fn [result]
                                  (is @revoked?)
                                  (is (.includes (:content result) "unauthorized"))
                                  (is (not (.includes (:content result) "Small scoped interfaces")))))))))
          (.then (fn [_] (read/execute! deps request)))
          (.then (fn [result]
                   (is (.includes (:content result) "unauthorized"))
                   (is (not (.includes (:content result) "Small scoped interfaces")))
                   (.run session "MATCH (o:AdamUser {id: $owner}), (r:AdamUser {id: $reader}) CREATE (o)-[:GRANTS_MEMORY_ACCESS {permission: 'read'}]->(r)
                                 WITH r MATCH (m:AdamObservation {id: $id}) SET m.content = 'Changed source preference'"
                         #js {:owner owner :reader reader :id (first mids)})))
          (.then (fn [_] (read/execute! deps request)))
          (.then (fn [result]
                   (is (.includes (:content result) "outdated"))
                   (.run session "MATCH (r:AdamDerivationRun {id: $id})-[edge:USED_MEMORY]->() DELETE edge" #js {:id (:id @saved)})))
          (.then (fn [_] (read/execute! deps (assoc request :history true))))
          (.then (fn [result]
                   (is (.includes (:content result) "unavailable"))
                   (is (not (.includes (:content result) "Small scoped interfaces - supersedes")))
                   (is (.includes (:content result) "Small scoped interfaces - contradicts")
                       "independently authorized older history remains visible")
                   (store/repair-dependencies! db request)))
          (.then (fn [_] (read/execute! deps request)))
          (.then (fn [result]
                   (is (.includes (:content result) "outdated"))
                   (is (.includes (:content result) "Small scoped interfaces"))
                   (is (= 3 @model-calls) "only the three explicit fake generations call the executor")))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally (fn []
                      (-> (.run session "MATCH (n) WHERE n.id IN $ids DETACH DELETE n" #js {:ids (clj->js @registered)})
                          (.catch (fn [error] (is false (str "exact cleanup failed: " (.-message error)))))
                          (.finally #(.close session)) (.finally #(.close driver)))))
          (.finally done)))))

(deftest live-depth-buckets-preserve-hidden-dependencies-and-filter-history
  (async done
    (let [env js/process.env uuid (randomUUID) reader (str "urn:adam:user:" uuid)
          sid (str "urn:adam:session:" uuid ":pi:depth-fixture")
          mids (mapv #(str "urn:adam:observation:" uuid ":pi:depth-fixture:fixture:" %) ["a" "b"])
          rid (str "urn:adam:reflection:" uuid ":pi:depth-fixture:fixture:r")
          name "depth-preferences" request {:name name}
          ^js driver ((.-driver neo4j) (aget env "ADAM_TEST_NEO4J_URI")
                      (.basic (.-auth neo4j) (aget env "ADAM_TEST_NEO4J_USERNAME") (aget env "ADAM_TEST_NEO4J_PASSWORD")))
          database (or (aget env "ADAM_TEST_NEO4J_DATABASE") "neo4j")
          ^js session (.session driver #js {:database database}) db (store/with-driver driver database reader)
          recipe (get (config/parse-config "derivations:\n  depth-preferences:\n    version: 1\n    guidance: Preserve attribution.\n") name)
          registered (atom (into [reader sid rid] mids))]
      (-> (store/ensure-schema! db)
          (.then (fn [_] (store/await-schema! db)))
          (.then (fn [_]
                   (.run session
                     "CREATE (u:AdamUser {id: $reader}), (s:AdamSession {id: $sid, sourceKind: 'pi', sourceSessionId: 'depth-fixture'})
                      CREATE (u)-[:OWNS]->(s)
                      CREATE (a:AdamObservation {id: $a, producer: 'fixture', memoryId: 'a', content: 'Active evidence'})
                      CREATE (b:AdamObservation {id: $b, producer: 'fixture', memoryId: 'b', content: 'Hidden support', dropped: true})
                      CREATE (r:AdamReflection {id: $r, producer: 'fixture', memoryId: 'r', content: 'A reflection', supportingObservationIds: ['b']})
                      CREATE (s)-[:HAS_MEMORY]->(a), (s)-[:HAS_MEMORY]->(b), (s)-[:HAS_MEMORY]->(r), (r)-[:SUPPORTED_BY]->(b)"
                     #js {:reader reader :sid sid :a (first mids) :b (second mids) :r rid})))
          (.then (fn [_]
                   (reduce (fn [pending depth]
                             (.then pending
                               (fn [_]
                                 (let [request (assoc request :depth depth)]
                                   (-> (store/load-input! db request)
                                       (.then (fn [snapshot]
                                                (core/refresh!
                                                  {:measure-input (constantly 200) :execute! (fn [_] {:finish-reason :stop :conclusions [] :links []})}
                                                  (merge snapshot {:name name :filter {:type :none} :depth depth :recipe recipe
                                                                   :model {:provider "fake" :id "fixture" :context-window 30000 :max-output-tokens 8192}}))))
                                       (.then (fn [{:keys [run]}]
                                                (swap! registered into [(:id run) (:derivation-id run)])
                                                (when (= "reflections" depth)
                                                  (is (= #{rid (second mids)} (set (:input-memory-ids run))))
                                                  (is (= 1 (:input-memory-count run))))
                                                (-> (store/acquire-refresh! db request 60000)
                                                    (.then #(store/publish-refresh! db request (:token %) run)))))
                                       (.then (fn [result] (is (= :published (:status result)) depth))))))))
                           (js/Promise.resolve) ["full" "active" "observations" "reflections"])))
          (.then (fn [_] (store/read-runs! db request)))
          (.then (fn [result]
                   (is (= 4 (count (:items result))))
                   (is (= #{"full" "active" "observations" "reflections"} (set (map #(get-in % [:run :depth]) (:items result)))))
                   (store/read-runs! db (assoc request :depth "reflections"))))
          (.then (fn [result]
                   (is (= 1 (count (:items result))))
                   (is (= "reflections" (get-in result [:items 0 :run :depth])))))
          (.catch (fn [error] (is false (str "isolated depth round trip failed: " (.-message error)))))
          (.finally (fn []
                      (-> (.run session "MATCH (n) WHERE n.id IN $ids DETACH DELETE n" #js {:ids (clj->js @registered)})
                          (.finally (fn [] (.close session)))
                          (.finally (fn [] (.close driver)))
                          (.finally done))))))))
