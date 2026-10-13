(ns adam.derivation.store-live-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
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
                       (.run session "MATCH (d:AdamDerivation {id: $id}) RETURN d.refreshLeaseUntil AS until" params)))
              (.then (fn [^js result]
                       (is ((.-isInt neo4j) (.get (first (array-seq (.-records result))) "until")))
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

(defn- fake-run! [input comparison empty?]
  (core/refresh!
    {:measure-input (constantly 100)
     :execute! (fn [_]
                 {:finish-reason :stop
                  :conclusions (if empty? [] [{:id "preference" :content "Prefer scoped simple boundaries"
                                              :category "architecture" :basis :explicit :scope-type :personal
                                              :scope-subject-ids [(:reader-id input)]
                                              :supporting-memory-ids [(get-in input [:memories 0 :id])]
                                              :conflicting-memory-ids []}])
                  :links (if (and comparison (not empty?))
                           [{:from "preference" :to (get-in comparison [:conclusions 0 :id]) :relation :supersedes}] [])})}
    (cond-> input comparison (assoc :comparison comparison))))

(defn- publish!
  ([db run] (publish! db {:name "preferences"} run))
  ([db request run]
  (-> (store/acquire-refresh! db request 360000)
      (.then (fn [lease]
               (is (= :acquired (:status lease)))
               (store/publish-refresh! db request (:token lease) run))))))

(deftest live-publication-history-revocation-rebuild-and-rollback
  (async done
    (let [env js/process.env uri (aget env "ADAM_TEST_NEO4J_URI")
          username (aget env "ADAM_TEST_NEO4J_USERNAME") password (aget env "ADAM_TEST_NEO4J_PASSWORD")]
      (if-not (and (= "1" (aget env "ADAM_TEST_NEO4J_DISPOSABLE")) uri username password)
        (do (is false "explicit disposable acknowledgement and credentials are required") (done))
        (let [uuid (randomUUID)
              reader (str "urn:adam:user:" uuid) owner (str "urn:adam:user:" (randomUUID)) third (str "urn:adam:user:" (randomUUID))
              session-ids (mapv #(str "urn:adam:session:" % ":pi:" (randomUUID)) [uuid (randomUUID) (randomUUID)])
              memory-ids (mapv #(str "urn:adam:observation:" uuid ":pi:fixture:synthetic:" %) ["a" "b" "c"])
              derivation-id (core/derivation-id reader "preferences" {:type :none})
              database (or (aget env "ADAM_TEST_NEO4J_DATABASE") "neo4j")
              ^js driver ((.-driver neo4j) uri (.basic (.-auth neo4j) username password))
              ^js session (.session driver #js {:database database})
              db (store/with-driver driver database reader)
              recipe (get (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n") "preferences")
              memories (mapv (fn [id owner sid] {:id id :owner-id owner :session-id sid :producer "synthetic"
                                                :kind :observation :content "Prefer scoped simple boundaries"})
                             memory-ids [reader owner third] session-ids)
              input {:reader-id reader :name "preferences" :filter {:type :none} :recipe recipe
                     :model {:provider "fake" :id "model" :context-window 20000 :max-output-tokens 8192}
                     :grants [{:owner-id owner :reader-id reader :permission "read"}] :memories (subvec memories 0 2)}
              comparison-input (update input :memories #(assoc-in % [1 :dropped?] true))
              params #js {:reader reader :owner owner :third third :derivationId derivation-id
                          :sessions (clj->js session-ids) :memories (clj->js memory-ids)}
              registered (atom (into [reader owner third derivation-id] (concat session-ids memory-ids)))
              first-run (atom nil) second-run (atom nil)
              register! (fn [run]
                          (swap! registered into (cons (:id run) (map :id (:conclusions run))))
                          run)]
          (-> (store/ensure-schema! db)
              (.then (fn [_]
                       (.run session
                         "CREATE (reader:AdamUser {id: $reader}), (owner:AdamUser {id: $owner}), (third:AdamUser {id: $third})
                          CREATE (owner)-[:GRANTS_MEMORY_ACCESS {permission: 'read'}]->(reader)
                          CREATE (third)-[:GRANTS_MEMORY_ACCESS {permission: 'read'}]->(owner)
                          WITH [reader, owner, third] AS users
                          UNWIND range(0, 2) AS i WITH users[i] AS user, i
                          CREATE (s:AdamSession {id: $sessions[i], sourceKind: 'pi', sourceSessionId: last(split($sessions[i], ':'))})
                          CREATE (m:AdamObservation {id: $memories[i], producer: 'synthetic', content: 'Prefer scoped simple boundaries'})
                          CREATE (user)-[:OWNS]->(s), (s)-[:HAS_MEMORY]->(m)" params)))
              (.then (fn [_] (store/load-input! db {:name "preferences"})))
              (.then (fn [snapshot]
                       (is (= (set (take 2 memory-ids)) (set (map :id (:memories snapshot)))))
                       (is (= [{:owner-id owner :reader-id reader :permission "read"}] (:grants snapshot)))
                       (is (nil? (:comparison snapshot)))
                       (fake-run! (assoc input :comparison {:id "missing" :input-memory-ids ["unavailable"] :conclusions []}) nil false)))
              (.then (fn [{:keys [run]}]
                       (is (= [:comparison-unavailable] (:diagnostics run)))
                       (reset! first-run (register! run))
                       (-> (store/acquire-refresh! db {:name "preferences"} 360000)
                           (.then (fn [old]
                                    (-> (.run session "MATCH (d:AdamDerivation {id: $derivationId}) SET d.refreshLeaseUntil = timestamp() - 1" params)
                                        (.then (fn [_] (store/publish-refresh! db {:name "preferences"} (:token old) run)))
                                        (.then (fn [result]
                                                 (is (= :fenced (:status result)))
                                                 (store/acquire-refresh! db {:name "preferences"} 360000)))
                                        (.then (fn [new]
                                                 (-> (store/publish-refresh! db {:name "preferences"} (:token old) run)
                                                     (.then (fn [result]
                                                              (is (= :fenced (:status result)))
                                                              (store/publish-refresh! db {:name "preferences"} (:token new) run))))))))))))
              (.then (fn [result]
                       (is (= :published (:status result)))
                       (store/read-runs! db {:name "preferences"})))
              (.then (fn [result]
                       (is (= :available (get-in result [:items 0 :status])))
                       (is (= @first-run (get-in result [:items 0 :run])))
                       (-> (.run session "MATCH (m:AdamObservation {id: $id}) SET m.dropped = true" #js {:id (second memory-ids)})
                           (.then (fn [_] (fake-run! comparison-input @first-run false))))))
              (.then (fn [{:keys [run]}]
                       (is (= #{:comparison-dependency} (get (:dependency-roles run) (second memory-ids))))
                       (reset! second-run (register! run)) (publish! db run)))
              (.then (fn [result]
                       (is (= :published (:status result)))
                       (.run session "MATCH (a:AdamDerivedObservation)-[:SUPERSEDES]->(b:AdamDerivedObservation)
                                      WHERE a.id IN $ids RETURN count(*) AS count"
                             #js {:ids (clj->js (mapv :id (:conclusions @second-run)))})))
              (.then (fn [^js result]
                       (let [^js row (first (array-seq (.-records result)))
                             ^js count (.get row "count")]
                         (is (= 1 (.toNumber count))))
                       (.run session "MATCH (r:AdamDerivationRun {id: $runId})-[used:USED_MEMORY]->(:AdamObservation {id: $id}) RETURN used.roles AS roles"
                             #js {:runId (:id @second-run) :id (second memory-ids)})))
              (.then (fn [^js result]
                       (is (= ["comparison-dependency"] (vec (.get (first (array-seq (.-records result))) "roles"))))
                       (store/read-runs! db {:name "preferences" :history true :limit 1})))
              (.then (fn [result]
                       (is (:truncated? result)) (is (= 1 (count (:items result))))
                       (is (= (:id @second-run) (get-in result [:items 0 :run :id])))
                       (.run session "MATCH (:AdamUser {id: $owner})-[g:GRANTS_MEMORY_ACCESS]->(:AdamUser {id: $reader}) DELETE g" params)))
              (.then (fn [_] (store/read-runs! db {:history true})))
              (.then (fn [result]
                       (is (= [:unauthorized :unauthorized] (mapv :status (:items result))))
                       (is (not-any? :run (:items result)))
                       (store/load-input! db {:name "preferences"})))
              (.then (fn [snapshot]
                       (is (= [(first memory-ids)] (mapv :id (:memories snapshot))))
                       (is (= {:id "unavailable"} (:comparison snapshot)))
                       ;; Simulate a model candidate completed from the earlier authorized snapshot.
                       (-> (fake-run! comparison-input @second-run false)
                           (.then (fn [{:keys [run]}]
                                    (register! run)
                                    (-> (publish! db run)
                                        (.then (fn [_] (is false "revoked comparison inheritance must block publication")))
                                        (.catch (fn [error] (is (= :unauthorized (config/error-code error)))))))))))
              (.then (fn [_]
                       (.run session "MATCH (d:AdamDerivation {id: $derivationId}) SET d.refreshLeaseUntil = timestamp() - 1" params)))
              (.then (fn [_]
                       (.run session "MATCH (owner:AdamUser {id: $owner}), (reader:AdamUser {id: $reader})
                                      CREATE (owner)-[:GRANTS_MEMORY_ACCESS {permission: 'read'}]->(reader)
                                      WITH owner MATCH (s:AdamSession {id: $sessions[1]})-[:HAS_MEMORY]->(old:AdamObservation)
                                      DETACH DELETE old
                                      WITH s CREATE (m:AdamObservation {id: $memories[1], producer: 'synthetic', content: 'Prefer scoped simple boundaries'})
                                      CREATE (s)-[:HAS_MEMORY]->(m)" params)))
              (.then (fn [_] (store/read-runs! db {:history true})))
              (.then (fn [result]
                       (is (= [:unresolved-evidence :unresolved-evidence] (mapv :status (:items result))))
                       (store/repair-dependencies! db {})))
              (.then (fn [result]
                       (is (= [:repaired :repaired] (mapv :status (:items result))))
                       (store/read-runs! (store/with-driver driver database reader) {:history true})))
              (.then (fn [result]
                       (is (= [:available :available] (mapv :status (:items result))))
                       (is (= #{(:id @first-run) (:id @second-run)} (set (map #(get-in % [:run :id]) (:items result)))))
                       ;; A deliberately forged in-memory grant cannot authorize a transitive graph grant.
                       (fake-run! (-> input (assoc :memories memories)
                                      (update :grants conj {:owner-id third :reader-id reader :permission "read"})) nil true)))
              (.then (fn [{:keys [run]}]
                       (register! run)
                       (-> (publish! db run)
                           (.then (fn [_] (is false "transitive grant must not authorize publication")))
                           (.catch (fn [error] (is (= :unauthorized (config/error-code error))))))))
              (.then (fn [_]
                       ;; Failed publication retains its lease until the caller releases/expires it.
                       (.run session "MATCH (d:AdamDerivation {id: $derivationId}) SET d.refreshLeaseUntil = timestamp() - 1" params)))
              (.then (fn [_] (fake-run! input nil false)))
              (.then (fn [{:keys [run]}]
                       (register! run)
                       ;; Force a last-write constraint failure to verify atomic rollback of LATEST_RUN.
                       (-> (.run session "CREATE (:AdamDerivedObservation {id: $id})"
                                 #js {:id (get-in run [:conclusions 0 :id])})
                           (.then (fn [_] (publish! db run)))
                           (.then (fn [_] (is false "collision must roll back publication")))
                           (.catch (fn [error] (is (= "Neo.ClientError.Schema.ConstraintValidationFailed" (.-code error))))))))
              (.then (fn [_] (store/read-runs! db {})))
              (.then (fn [result]
                       (is (= (:id @second-run) (get-in result [:items 0 :run :id])))
                       (.run session "MATCH (d:AdamDerivation {id: $derivationId}) SET d.refreshLeaseUntil = timestamp() - 1" params)))
              (.then (fn [_] (fake-run! input nil true)))
              (.then (fn [{:keys [run]}] (register! run) (publish! db run)))
              (.then (fn [result]
                       (is (= :published (:status result)))
                       (store/read-runs! db {})))
              (.then (fn [result]
                       (is (= :available (get-in result [:items 0 :status])))
                       (is (empty? (get-in result [:items 0 :run :conclusions])))
                       (store/read-runs! db {:history true})))
              (.then (fn [result]
                       (is (= 3 (count (:items result))))
                       (fake-run! (assoc input :filter {:type :language :value "python"}) nil true)))
              (.then (fn [{:keys [run]}]
                       (let [run (assoc run :created-at "2099-01-01T00:00:00.000Z")]
                         (register! run)
                         (swap! registered conj (:derivation-id run))
                         (publish! db {:name "preferences" :language "py"} run))))
              (.then (fn [result]
                       (is (= :published (:status result)))
                       (store/read-runs! db {:name "preferences" :limit 1})))
              (.then (fn [result]
                       (is (:truncated? result))
                       (is (= {:type :language :value "python"} (get-in result [:items 0 :filter])))
                       (store/read-runs! db {:name "preferences"})))
              (.then (fn [result]
                       (is (= #{{:type :none} {:type :language :value "python"}} (set (map :filter (:items result)))))
                       (store/read-runs! db {:name "preferences" :language "py" :history true})))
              (.then (fn [result]
                       (is (= 1 (count (:items result))))
                       (is (not (:truncated? result)))
                       (is (= {:type :language :value "python"} (get-in result [:items 0 :filter])))))
              (.catch (fn [error] (is false (.-stack error))))
              (.finally (fn []
                          (-> (.run session "MATCH (n) WHERE n.id IN $ids DETACH DELETE n"
                                    #js {:ids (clj->js @registered)})
                              (.catch (fn [error] (is false (str "exact fixture cleanup failed: " (.-message error)))))
                              (.finally #(.close session))
                              (.finally #(.close driver)))))
              (.finally done)))))))

(deftest live-input-repository-and-reflection-language-selection
  (async done
    (let [env js/process.env
          uuid (randomUUID) reader (str "urn:adam:user:" uuid)
          sid (str "urn:adam:session:" uuid ":pi:input")
          ids [reader sid (str "repo:" uuid) (str "file:" uuid) (str "observation:" uuid) (str "reflection:" uuid)]
          ^js driver ((.-driver neo4j) (aget env "ADAM_TEST_NEO4J_URI")
                      (.basic (.-auth neo4j) (aget env "ADAM_TEST_NEO4J_USERNAME") (aget env "ADAM_TEST_NEO4J_PASSWORD")))
          database (or (aget env "ADAM_TEST_NEO4J_DATABASE") "neo4j")
          ^js session (.session driver #js {:database database})
          db (store/with-driver driver database reader)
          entries [{:id (str "activity-user:" uuid) :entryId "user" :streamId "main"
                    :rawJson "{\"id\":\"user\",\"type\":\"message\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"Use specs.\"}}"}
                   {:id (str "activity-call:" uuid) :entryId "call" :streamId "main"
                    :rawJson "{\"id\":\"call\",\"type\":\"message\",\"parentId\":\"user\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"t\",\"name\":\"read\"}]}}"}
                   {:id (str "activity-result:" uuid) :entryId "result" :streamId "main"
                    :rawJson "{\"id\":\"result\",\"type\":\"message\",\"parentId\":\"call\",\"message\":{\"role\":\"toolResult\",\"toolCallId\":\"t\",\"content\":[]}}"}]
          claude-sid (str "urn:adam:session:" uuid ":claude-code:claude-input")
          claude-memory (str "claude-observation:" uuid)
          fork-sid (str "urn:adam:session:" uuid ":pi:fork")
          fork-memory (str "fork-observation:" uuid)
          native (fn [id stream value] {:id (str "native:" uuid ":" id) :entryId id :streamId stream
                                        :rawJson (js/JSON.stringify (clj->js (assoc value :uuid id :sessionId "claude-input")))})
          claude-entries
          [(native "claude-user" "main" {:type "user" :parentUuid nil :message {:role "user" :content "Use specs."}})
           (native "claude-call" "main" {:type "assistant" :parentUuid "claude-user" :requestId "r"
                                        :message {:role "assistant" :content [{:type "tool_use" :id "ct" :name "Read"}]}})
           (native "claude-result" "main" {:type "user" :parentUuid "claude-user" :sourceToolAssistantUUID "claude-call"
                                          :message {:role "user" :content [{:type "tool_result" :tool_use_id "ct" :content "Read done."}]}})
           (native "agent-call" "main" {:type "assistant" :parentUuid "claude-user"
                                       :message {:role "assistant" :content [{:type "tool_use" :id "at" :name "Agent"}]}})
           (native "agent-result" "main" {:type "user" :parentUuid "agent-call" :sourceToolAssistantUUID "agent-call"
                                         :toolUseResult {:agentId "child"}
                                         :message {:role "user" :content [{:type "tool_result" :tool_use_id "at" :content "Handback"}]}})
           (native "child-call" "agent:child" {:type "assistant" :parentUuid nil :agentId "child"
                                             :message {:role "assistant" :content [{:type "tool_use" :id "child-t" :name "Read"}]}})]
          fork-entry (assoc (first entries) :id (str "fork-entry:" uuid))
          cleanup-ids (into ids (concat (map :id entries) (map :id claude-entries)
                                       [claude-sid claude-memory fork-sid fork-memory (:id fork-entry)
                                        (str "urn:adam:session:" uuid ":claude-code:memoryless-copy") (str "copy-request:" uuid)]))
          params #js {:ids (clj->js ids) :entries (clj->js entries) :cleanupIds (clj->js cleanup-ids)
                      :claudeSessionId claude-sid :claudeMemoryId claude-memory :claudeEntries (clj->js claude-entries)
                      :forkSessionId fork-sid :forkMemoryId fork-memory :forkEntry (clj->js fork-entry)
                      :copySessionId (str "urn:adam:session:" uuid ":claude-code:memoryless-copy") :copyEntryId (str "copy-request:" uuid)}
          recipe (get (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n") "preferences")
          model {:provider "fake" :id "model" :context-window 20000 :max-output-tokens 8192}
          check! (fn [request]
                   (-> (store/load-input! db request)
                       (.then (fn [snapshot]
                                (let [filter (:filter (core/normalize-read request))
                                      prepared (core/prepare-refresh
                                                 (assoc snapshot :name "preferences" :recipe recipe :model model
                                                        :filter filter) (constantly 100))]
                                  (is (= (set (take-last 2 ids)) (set (get-in prepared [:snapshot :candidate-ids])))))))))]
      (-> (.run session
            "CREATE (u:AdamUser {id: $ids[0]}), (s:AdamSession {id: $ids[1], sourceKind: 'pi', sourceSessionId: 'input'}),
                    (repo:AdamRepository {id: $ids[2], normalizedOrigin: 'example.com/repo'}),
                    (file:AdamCodeFile {id: $ids[3], relativePath: 'src/a.cljs'}),
                    (o:AdamObservation {id: $ids[4], memoryId: 'obs', producer: 'synthetic', content: 'Prefer simple boundaries'}),
                    (r:AdamReflection {id: $ids[5], producer: 'synthetic', content: 'Scoped synthesis', supportingObservationIds: ['obs']})
             CREATE (u)-[:OWNS]->(s), (s)-[:WORKED_ON]->(repo), (s)-[:HAS_MEMORY]->(o), (s)-[:HAS_MEMORY]->(r),
                    (o)-[:ABOUT]->(file), (r)-[:SUPPORTED_BY]->(o)" params)
          (.then (fn [_] (check! {:name "preferences" :repository "git@example.com:repo.git"})))
          (.then (fn [_] (check! {:name "preferences" :language "cljs"})))
          (.then (fn [_]
                   (.run session "MATCH (s:AdamSession {id: $ids[1]}), (o:AdamObservation {id: $ids[4]})
                                  UNWIND $entries AS input CREATE (e:AdamEntry) SET e = input
                                  CREATE (s)-[:HAS_ENTRY]->(e)
                                  FOREACH (_ IN CASE WHEN input.entryId IN ['call', 'result'] THEN [1] ELSE [] END |
                                    CREATE (o)-[:SOURCED_FROM]->(e))" params)))
          (.then (fn [_] (store/load-input! db {:name "preferences"})))
          (.then (fn [snapshot]
                   (is (= [{:session-id sid :stream-id "main" :entry-id "user"}]
                          (:source-occasions (first (filter #(= :observation (:kind %)) (:memories snapshot))))))))
          (.then (fn [_]
                   (.run session "MATCH (r:AdamReflection {id: $ids[5]}) SET r.supportingObservationIds = ['wrong']" params)))
          (.then (fn [_]
                   (-> (store/load-input! db {:name "preferences"})
                       (.then (fn [_] (is false "graph/declared support mismatch must fail closed")))
                       (.catch (fn [error]
                                 (is (= :unresolved-evidence (config/error-code error)))
                                 (is (= 1 (:invalid-source-count (ex-data error)))))))))
          (.then (fn [_]
                   (.run session "MATCH (r:AdamReflection {id: $ids[5]}), (o:AdamObservation {id: $ids[4]})
                                  SET r.supportingObservationIds = ['obs'], r.producer = '', o.producer = ''" params)))
          (.then (fn [_] (store/load-input! db {:name "preferences"})))
          (.then (fn [snapshot]
                   (is (= #{"pi-observational-memory"} (set (map :producer (:memories snapshot)))))
                   (is (= [(nth ids 4)] (:supporting-memory-ids (first (filter #(= :reflection (:kind %)) (:memories snapshot))))))))
          (.then (fn [_]
                   (.run session "MATCH (u:AdamUser {id: $ids[0]}), (parent:AdamSession {id: $ids[1]})
                                  CREATE (c:AdamSession {id: $claudeSessionId, sourceKind: 'claude-code', sourceSessionId: 'claude-input'}),
                                         (o:AdamObservation {id: $claudeMemoryId, memoryId: 'claude-obs', producer: 'synthetic', content: 'Use specs'}),
                                         (fork:AdamSession {id: $forkSessionId, sourceKind: 'pi', sourceSessionId: 'fork', parentSession: 'locator'}),
                                         (copy:AdamEntry), (fo:AdamObservation {id: $forkMemoryId, memoryId: 'fork-obs', producer: 'synthetic', content: 'Use specs'})
                                  SET copy = $forkEntry
                                  CREATE (u)-[:OWNS]->(c), (c)-[:HAS_MEMORY]->(o), (u)-[:OWNS]->(fork),
                                         (fork)-[:FORKED_FROM]->(parent), (fork)-[:HAS_ENTRY]->(copy),
                                         (fork)-[:HAS_MEMORY]->(fo), (fo)-[:SOURCED_FROM]->(copy)
                                  WITH c, o UNWIND $claudeEntries AS input CREATE (e:AdamEntry) SET e = input
                                  CREATE (c)-[:HAS_ENTRY]->(e)
                                  FOREACH (_ IN CASE WHEN input.entryId IN ['claude-call','claude-result','child-call'] THEN [1] ELSE [] END |
                                    CREATE (o)-[:SOURCED_FROM]->(e))" params)))
          (.then (fn [_] (store/load-input! db {:name "preferences"})))
          (.then (fn [snapshot]
                   (let [by-id (into {} (map (juxt :id identity)) (:memories snapshot))]
                     (is (= [{:session-id sid :stream-id "main" :entry-id "user"}] (:source-occasions (get by-id fork-memory))))
                     (is (= [{:session-id claude-sid :stream-id "main" :entry-id "claude-user"}] (:source-occasions (get by-id claude-memory)))))))
          (.then (fn [_]
                   (.run session "MATCH (u:AdamUser {id: $ids[0]})
                                  CREATE (s:AdamSession {id: $copySessionId, sourceKind: 'claude-code', sourceSessionId: 'memoryless-copy'}),
                                         (e:AdamEntry {id: $copyEntryId, entryId: 'claude-user', streamId: 'main', type: 'user'})
                                  CREATE (u)-[:OWNS]->(s), (s)-[:HAS_ENTRY]->(e)" params)))
          (.then (fn [_] (store/load-input! db {:name "preferences"})))
          (.then (fn [snapshot]
                   (is (= [] (:source-occasions (first (filter #(= claude-memory (:id %)) (:memories snapshot)))))
                       "memoryless original/copy sessions participate in duplicate detection")))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally (fn []
                      (-> (.run session "MATCH (n) WHERE n.id IN $cleanupIds DETACH DELETE n" params)
                          (.finally #(.close session))
                          (.finally #(.close driver)))))
          (.finally done)))))
