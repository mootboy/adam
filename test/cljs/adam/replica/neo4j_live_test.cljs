(ns adam.replica.neo4j-live-test
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.index :as knowledge-index]
            [adam.knowledge.memory-migration :as memory-migration]
            [adam.knowledge.store :as knowledge-store]
            [adam.knowledge.surfaces :as knowledge-surfaces]
            [adam.knowledge.tool :as knowledge-tool]
            [adam.memory.store :as memory-store]
            [adam.memory.sync :as memory-sync]
            [adam.replica.identity :as identity]
            [adam.replica.migration :as replica-migration]
            [adam.replica.neo4j :as adam-neo4j]
            [adam.replica.restore :as restore]
            [adam.replica.store :as store]
            [adam.replica.sync :as sync]
            [adam.sources.claude-code.evidence :as claude-evidence]
            [adam.sources.claude-code.reconcile :as claude-reconcile]
            [adam.sources.claude-code.scanner :as claude-scanner]
            [adam.sources.claude-code.sync :as claude-sync]
            [cljs.test :refer [async deftest is]]
            ["neo4j-driver" :as neo4j]
            ["node:crypto" :refer [randomUUID]]
            ["node:fs" :refer [appendFileSync chmodSync copyFileSync mkdtempSync readFileSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :as node-path :refer [join]]))

(defn- environment [key]
  (aget js/process.env key))

(deftest live-session-round-trip-and-child-first-lineage
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do
          (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required")
          (done))
        (let [user-uuid (randomUUID)
              user-id (identity/user-urn user-uuid)
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              directory (mkdtempSync (join (tmpdir) "adam-neo4j-live-"))
              parent-path (join directory "parent.jsonl")
              child-path (join directory "child.jsonl")
              parent-first-path (join directory "parent-first.jsonl")
              child-after-parent-path (join directory "child-after-parent.jsonl")
              parent-header (js/JSON.stringify
                             #js {:type "session" :version 3
                                  :id "parent-live" :cwd "/repo"})
              child-header (js/JSON.stringify
                            #js {:type "session" :version 3
                                 :id "child-live" :cwd "/repo"
                                 :parentSession parent-path})
              child-entry "{\"type\":\"message\",\"id\":\"entry-live\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call-live\",\"name\":\"read\",\"arguments\":{\"path\":\"src/live.cljs\"}}]}}"
              child-result "{\"type\":\"message\",\"id\":\"result-live\",\"parentId\":\"entry-live\",\"message\":{\"role\":\"toolResult\",\"toolCallId\":\"call-live\"}}"
              child-observation "{\"type\":\"custom\",\"id\":\"memory-live\",\"parentId\":\"result-live\",\"customType\":\"om.observations.recorded\",\"data\":{\"observations\":[{\"id\":\"aaaaaaaaaaaa\",\"content\":\"Live file decision\",\"timestamp\":\"2026-01-01T00:00:00.000Z\",\"relevance\":\"high\",\"sourceEntryIds\":[\"result-live\"],\"tokenCount\":4}],\"coversUpToId\":\"result-live\"}}"
              child-reflection "{\"type\":\"custom\",\"id\":\"reflection-live\",\"parentId\":\"memory-live\",\"customType\":\"om.reflections.recorded\",\"data\":{\"reflections\":[{\"id\":\"bbbbbbbbbbbb\",\"content\":\"Preserve the live decision\",\"supportingObservationIds\":[\"aaaaaaaaaaaa\"],\"tokenCount\":3}],\"coversUpToId\":\"memory-live\"}}"
              child-drop "{\"type\":\"custom\",\"id\":\"drop-live\",\"parentId\":\"reflection-live\",\"customType\":\"om.observations.dropped\",\"data\":{\"observationIds\":[\"aaaaaaaaaaaa\"],\"coversUpToId\":\"reflection-live\"}}"
              repository (evidence/build-repository
                          {:user-uuid user-uuid :root "/repo"
                           :remote "git@github.com:AloiAI/adam.git"
                           :commit "live-commit" :branch "live" :dirty? true})
              tools (atom {})
              _ (knowledge-surfaces/register-tool!
                 #js {:registerTool (fn [definition]
                                      (swap! tools assoc
                                             (aget definition "name") definition))}
                 {:get-store! #(js/Promise.resolve replica)
                  :get-user! #(js/Promise.resolve {:user-uuid user-uuid})
                  :resolve-repository! (fn [_ _] (js/Promise.resolve repository))})
              file-context-tool (get @tools "adam_file_context")
              parent-first-header (js/JSON.stringify
                                   #js {:type "session" :version 3
                                        :id "parent-first-live" :cwd "/repo"})
              child-after-parent-header
              (js/JSON.stringify
               #js {:type "session" :version 3
                    :id "child-after-parent-live" :cwd "/repo"
                    :parentSession parent-first-path})
              child-after-parent-entry "{\"type\":\"message\",\"id\":\"entry-after-parent\",\"parentId\":null}"
              child-session-id (identity/session-urn user-uuid "child-live")
              parent-session-id (identity/session-urn user-uuid "parent-live")
              materialized-path (join directory "materialized-child.jsonl")
              child-after-parent-session-id
              (identity/session-urn user-uuid "child-after-parent-live")
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (file:AdamCodeFile {repositoryId: $repositoryId})
                           DETACH DELETE file"
                          #js {:repositoryId (:id repository)})
                    (.then
                     (fn [_]
                       (.run query-session
                             "MATCH (repository:AdamRepository {id: $repositoryId})
                              DETACH DELETE repository"
                             #js {:repositoryId (:id repository)})))
                    (.then
                     (fn [_]
                       (.run query-session
                             "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                             #js {:userUuid user-uuid})))
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (rmSync directory #js {:recursive true :force true})
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          (writeFileSync parent-path (str parent-header "\n") "utf8")
          (writeFileSync child-path
                         (str child-header "\n" child-entry "\n" child-result "\n"
                              child-observation "\n" child-reflection "\n" child-drop "\n")
                         "utf8")
          (writeFileSync parent-first-path (str parent-first-header "\n") "utf8")
          (writeFileSync child-after-parent-path
                         (str child-after-parent-header "\n" child-after-parent-entry "\n")
                         "utf8")
          (-> (store/initialize! replica {:id user-id})
              (.then
               (fn [_]
                 (sync/sync-session-file!
                  {:path child-path
                   :user-uuid user-uuid
                   :current-leaf-id "drop-live"
                   :replica replica})))
              (.then
               (fn [child-result]
                 (is (= :mirrored (:status child-result)))
                 (sync/sync-session-file!
                  {:path parent-path
                   :user-uuid user-uuid
                   :replica replica})))
              (.then
               (fn [parent-result]
                 (is (= :mirrored (:status parent-result)))
                 (store/read-session! replica child-session-id)))
              (.then
               (fn [restored]
                 (is (= child-header (:header-json restored)))
                 (is (= [child-entry child-result child-observation child-reflection child-drop]
                        (mapv :raw-json (:entries restored))))
                 (is (= "drop-live" (get-in restored [:session :current-leaf-id])))
                 (restore/materialize-session! replica child-session-id materialized-path)))
              (.then
               (fn [materialized]
                 (is (= materialized-path (:path materialized)))
                 (is (= (str child-header "\n" child-entry "\n" child-result "\n"
                             child-observation "\n" child-reflection "\n" child-drop "\n")
                        (readFileSync materialized-path "utf8")))
                 (knowledge-store/ensure-file-evidence-schema! replica)))
              (.then
               (fn [_]
                 (knowledge-index/index-session!
                  {:store replica :path child-path :user-uuid user-uuid
                   :repository repository :current-leaf-id "drop-live"})))
              (.then
               (fn [_]
                 (knowledge-index/index-session!
                  {:store replica :path child-path :user-uuid user-uuid
                   :repository repository :current-leaf-id "drop-live"})))
              (.then
               (fn [_]
                 (knowledge-store/complete-code-memory-rebuild!
                  replica user-id 3)))
              (.then
               (fn [_]
                 (knowledge-store/code-memory-version! replica user-id)))
              (.then
               (fn [version]
                 (is (= 0 version))
                 (knowledge-store/clear-file-evidence!
                  replica parent-session-id 3)))
              (.then
               (fn [_]
                 (knowledge-store/code-memory-version! replica user-id)))
              (.then
               (fn [version]
                 (is (= 3 version))
                 (knowledge-store/query-file-memory!
                  replica user-id (:id repository) "src/live.cljs" 20)))
              (.then
               (fn [memories]
                 ;; The dropped observation is tombstoned in the graph but no
                 ;; longer occupies a result; the reflection it supports remains.
                 (let [[reflection] memories]
                   (is (= 1 (count memories)))
                   (is (= :reflection (:kind reflection)))
                   (is (= "bbbbbbbbbbbb" (:memory-id reflection)))
                   (is (= ["result-live"] (:source-entry-ids reflection)))
                   (is (= [{:stream-id "main" :entry-id "result-live"
                            :commit "live-commit" :branch "live" :dirty? true}]
                          (:source-contexts reflection))))
                 (.run query-session
                       "MATCH (o:AdamObservation {memoryId: 'aaaaaaaaaaaa'})-[:SOURCED_FROM]->(source:AdamEntry {entryId: 'result-live'})
                        WHERE o.id CONTAINS $userUuid
                        RETURN o.dropped AS dropped, count(source) AS sources"
                       #js {:userUuid user-uuid})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= true (.get record "dropped")))
                   (is (= 1 (.toNumber (.get record "sources")))))
                 (.call (aget file-context-tool "execute") file-context-tool
                        "call-live"
                        #js {:origin "https://github.com/AloiAI/adam.git"
                             :path "src/live.cljs"}
                        nil nil #js {:cwd "/unrelated/repository"})))
              (.then
               (fn [tool-result]
                 (let [text (aget (aget (aget tool-result "content") 0) "text")]
                   (is (= "ok" (aget (aget tool-result "details") "status")))
                   (is (not (re-find #"Live file decision" text)))
                   (is (re-find #"Preserve the live decision" text))
                   (is (re-find #"live @ live-co \(dirty\)" text)))
                 (.run query-session
                       "MATCH (:AdamSession {id: $childId})-[:FORKED_FROM]->(parent:AdamSession)
                        RETURN parent.piSessionId AS parentId"
                       #js {:childId child-session-id})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= "parent-live" (.get record "parentId"))))
                 (sync/sync-session-file!
                  {:path parent-first-path
                   :user-uuid user-uuid
                   :replica replica})))
              (.then
               (fn [_]
                 (sync/sync-session-file!
                  {:path child-after-parent-path
                   :user-uuid user-uuid
                   :current-leaf-id "entry-after-parent"
                   :replica replica})))
              (.then
               (fn [_]
                 (sync/sync-session-file!
                  {:path child-after-parent-path
                   :user-uuid user-uuid
                   :current-leaf-id "entry-after-parent"
                   :replica replica})))
              (.then
               (fn [result]
                 (is (= :unchanged (:status result)))
                 (store/read-session! replica child-after-parent-session-id)))
              (.then
               (fn [restored]
                 (is (= [child-after-parent-entry]
                        (mapv :raw-json (:entries restored))))
                 (.run query-session
                       "MATCH (:AdamSession {id: $childId})-[:FORKED_FROM]->(parent:AdamSession)
                        RETURN parent.piSessionId AS parentId"
                       #js {:childId child-after-parent-session-id})))
              (.then
               (fn [^js result]
                 (let [records (array-seq (.-records result))
                       ^js record (first records)]
                   (is (= 1 (count records)))
                   (is (= "parent-first-live" (.get record "parentId"))))
                 (finish! nil)))
              (.catch finish!)))))))

(deftest live-source-scoped-identity-migration-preserves-remote-session-graph
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do
          (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required")
          (done))
        (let [user-uuid (randomUUID)
              user-id (identity/user-urn user-uuid)
              old-parent-id (str "urn:adam:session:" user-uuid ":parent")
              old-child-id (str "urn:adam:session:" user-uuid ":child")
              new-parent-id (identity/session-urn user-uuid "parent")
              new-child-id (identity/session-urn user-uuid "child")
              new-entry-id (identity/entry-urn user-uuid "child" "entry")
              source-observation-id
              (str "urn:adam:observation:" user-uuid ":pi:child:aaaaaaaaaaaa")
              source-reflection-id
              (str "urn:adam:reflection:" user-uuid ":pi:child:bbbbbbbbbbbb")
              source-invalid-observation-id
              (str "urn:adam:observation:" user-uuid ":pi:child:cccccccccccc")
              new-observation-id
              (identity/observation-urn
               user-uuid "pi" "child" "pi-observational-memory" "aaaaaaaaaaaa")
              new-reflection-id
              (identity/reflection-urn
               user-uuid "pi" "child" "pi-observational-memory" "bbbbbbbbbbbb")
              new-defaulted-observation-id
              (identity/observation-urn
               user-uuid "pi" "child" "pi-observational-memory" "cccccccccccc")
              repository-id (str "urn:adam:repository:local:" user-uuid ":legacy")
              file-id (str "urn:adam:file:" user-uuid)
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                          #js {:userUuid user-uuid})
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          (-> (store/initialize! replica {:id user-id})
              (.then
               (fn [_]
                 (.run
                  query-session
                  "MATCH (u:AdamUser {id: $userId})
                   SET u.sessionIdentityVersion = 2,
                       u.memoryIdentityVersion = 1
                   CREATE (parent:AdamSession {id: $oldParentId, piSessionId: 'parent', headerJson: '{\"type\":\"session\",\"id\":\"parent\"}'})
                   CREATE (child:AdamSession {id: $oldChildId, piSessionId: 'child', parentPiSessionId: 'parent', parentSessionId: $oldParentId, headerJson: '{\"type\":\"session\",\"id\":\"child\"}', sourceFile: '/remote-only/child.jsonl'})
                   CREATE (entry:AdamEntry {id: $oldEntryId, sessionId: $oldChildId, entryId: 'entry', parentId: null, parentUrn: null, ordinal: 0, rawJson: $rawJson, payloadHash: 'payload-hash', payloadBytes: 47})
                   CREATE (observation:AdamObservation {id: $oldObservationId, memoryId: 'aaaaaaaaaaaa', producer: 'pi-observational-memory', content: 'legacy observation', dropped: true})
                   CREATE (reflection:AdamReflection {id: $oldReflectionId, memoryId: 'bbbbbbbbbbbb', producer: 'pi-observational-memory', content: 'legacy reflection'})
                   CREATE (invalidObservation:AdamObservation {id: $oldInvalidObservationId, memoryId: 'cccccccccccc', content: 'invalid until repaired'})
                   CREATE (repository:AdamRepository {id: $repositoryId})
                   CREATE (file:AdamCodeFile {id: $fileId, repositoryId: $repositoryId, relativePath: 'src/a.cljs'})
                   CREATE (u)-[:OWNS]->(parent)
                   CREATE (u)-[:OWNS]->(child)
                   CREATE (child)-[:FORKED_FROM]->(parent)
                   CREATE (child)-[:HAS_ENTRY]->(entry)
                   CREATE (child)-[:CURRENT_LEAF]->(entry)
                   CREATE (child)-[:HAS_MEMORY]->(observation)
                   CREATE (child)-[:HAS_MEMORY]->(reflection)
                   CREATE (child)-[:HAS_MEMORY]->(invalidObservation)
                   CREATE (entry)-[:TOUCHES]->(file)
                   CREATE (observation)-[:SOURCED_FROM]->(entry)
                   CREATE (observation)-[:ABOUT]->(file)
                   CREATE (reflection)-[:SUPPORTED_BY]->(observation)"
                  #js {:userId user-id
                       :oldParentId old-parent-id
                       :oldChildId old-child-id
                       :oldEntryId (str "urn:adam:entry:" user-uuid ":child:entry")
                       :oldObservationId (str "urn:adam:observation:" user-uuid ":child:aaaaaaaaaaaa")
                       :oldReflectionId (str "urn:adam:reflection:" user-uuid ":child:bbbbbbbbbbbb")
                       :oldInvalidObservationId (str "urn:adam:observation:" user-uuid ":child:cccccccccccc")
                       :repositoryId repository-id
                       :fileId file-id
                       :rawJson "{\"type\":\"message\",\"id\":\"entry\"}"})))
              (.then (fn [_] (store/session-identity-version! replica user-id)))
              (.then
               (fn [version]
                 (is (= 0 version))
                 (replica-migration/migrate-if-needed!
                  {:store replica :user-id user-id})))
              (.then
               (fn [result]
                 (is (= {:status :migrated :version 2} result))
                 (store/session-identity-version! replica user-id)))
              (.then
               (fn [version]
                 (is (= 2 version))
                 (.run
                  query-session
                  "MATCH (child:AdamSession {id: $childId})-[:FORKED_FROM]->(parent:AdamSession {id: $parentId})
                   MATCH (child)-[:HAS_ENTRY]->(entry:AdamEntry {id: $entryId})
                   MATCH (child)-[:HAS_MEMORY]->(observation:AdamObservation {id: $observationId})
                   MATCH (child)-[:HAS_MEMORY]->(reflection:AdamReflection {id: $reflectionId})
                   MATCH (child)-[:CURRENT_LEAF]->(entry)
                   MATCH (entry)-[:TOUCHES]->(file:AdamCodeFile {id: $fileId})
                   MATCH (observation)-[:SOURCED_FROM]->(entry)
                   MATCH (observation)-[:ABOUT]->(file)
                   MATCH (reflection)-[:SUPPORTED_BY]->(observation)
                   RETURN child, parent, entry, observation, reflection"
                  #js {:childId new-child-id
                       :parentId new-parent-id
                       :entryId new-entry-id
                       :observationId source-observation-id
                       :reflectionId source-reflection-id
                       :fileId file-id})))
              (.then
               (fn [^js result]
                 (let [records (array-seq (.-records result))
                       ^js record (first records)
                       child (.-properties (.get record "child"))
                       entry (.-properties (.get record "entry"))
                       observation (.-properties (.get record "observation"))]
                   (is (= 1 (count records)))
                   (is (= "pi" (aget child "sourceKind")))
                   (is (= "child" (aget child "sourceSessionId")))
                   (is (= new-parent-id (aget child "parentSessionId")))
                   (is (= new-child-id (aget entry "sessionId")))
                   (is (= "{\"type\":\"message\",\"id\":\"entry\"}"
                          (aget entry "rawJson")))
                   (is (= "pi" (aget observation "sourceKind")))
                   (is (= true (aget observation "dropped"))))
                 (.run
                  query-session
                  "MATCH (:AdamSession {id: $childId})-[:HAS_MEMORY]->(invalid:AdamObservation {id: $invalidObservationId})
                   SET invalid.sourceKind = 'claude-code'"
                  #js {:childId new-child-id
                       :invalidObservationId source-invalid-observation-id})))
              (.then
               (fn [_]
                 (knowledge-store/memory-identity-version! replica user-id)))
              (.then
               (fn [version]
                 (is (= 0 version)
                     "a stale marker cannot hide producerless memory identities")
                 (-> (memory-migration/migrate-if-needed!
                      {:store replica :user-id user-id})
                     (.then
                      (fn [_]
                        (throw (js/Error. "expected invalid migration to fail"))))
                     (.catch
                      (fn [error]
                        (is (= :invalid-memory-identity (:type (ex-data error))))
                        :migration-rejected)))))
              (.then
               (fn [result]
                 (is (= :migration-rejected result))
                 (.run
                  query-session
                  "MATCH (u:AdamUser {id: $userId})
                   MATCH (:AdamSession {id: $childId})-[:HAS_MEMORY]->(observation:AdamObservation {id: $observationId})
                   MATCH (:AdamSession {id: $childId})-[:HAS_MEMORY]->(invalid:AdamObservation {id: $invalidObservationId})
                   RETURN u.memoryIdentityVersion AS version, observation, invalid"
                  #js {:userId user-id
                       :childId new-child-id
                       :observationId source-observation-id
                       :invalidObservationId source-invalid-observation-id})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= 1 (.toNumber (.get record "version")))
                       "a rejected migration retains the prior marker")
                   (is (some? (.get record "observation"))
                       "a rejected migration retains valid legacy identities"))
                 (.run
                  query-session
                  "MATCH (:AdamSession {id: $childId})-[:HAS_MEMORY]->(invalid:AdamObservation {id: $invalidObservationId})
                   SET invalid.sourceKind = 'pi'"
                  #js {:childId new-child-id
                       :invalidObservationId source-invalid-observation-id})))
              (.then
               (fn [_]
                 (memory-migration/migrate-if-needed!
                  {:store replica :user-id user-id})))
              (.then
               (fn [result]
                 (is (= {:status :migrated :version 1} result))
                 (.run
                  query-session
                  "MATCH (child:AdamSession {id: $childId})-[:HAS_MEMORY]->(observation:AdamObservation {id: $observationId})
                   MATCH (child)-[:HAS_MEMORY]->(reflection:AdamReflection {id: $reflectionId})
                   MATCH (child)-[:HAS_MEMORY]->(defaulted:AdamObservation {id: $defaultedObservationId})
                   MATCH (child)-[:HAS_ENTRY]->(entry:AdamEntry {id: $entryId})
                   MATCH (entry)-[:TOUCHES]->(file:AdamCodeFile {id: $fileId})
                   MATCH (observation)-[:SOURCED_FROM]->(entry)
                   MATCH (observation)-[:ABOUT]->(file)
                   MATCH (reflection)-[:SUPPORTED_BY]->(observation)
                   RETURN child, entry, observation, reflection, defaulted"
                  #js {:childId new-child-id
                       :entryId new-entry-id
                       :observationId new-observation-id
                       :reflectionId new-reflection-id
                       :defaultedObservationId new-defaulted-observation-id
                       :fileId file-id})))
              (.then
               (fn [^js result]
                 (let [records (array-seq (.-records result))
                       ^js record (first records)
                       observation (.-properties (.get record "observation"))
                       reflection (.-properties (.get record "reflection"))
                       defaulted (.-properties (.get record "defaulted"))]
                   (is (= 1 (count records)))
                   (is (= true (aget observation "dropped")))
                   (is (= "pi-observational-memory" (aget observation "producer")))
                   (is (= "pi-observational-memory" (aget reflection "producer")))
                   (is (= "pi-observational-memory" (aget defaulted "producer"))
                       "producerless Pi memory receives the legacy producer default"))
                 (memory-migration/migrate-if-needed!
                  {:store replica :user-id user-id})))
              (.then
               (fn [result]
                 (is (= {:status :current :version 1} result))
                 (replica-migration/migrate-if-needed!
                  {:store replica :user-id user-id})))
              (.then
               (fn [result]
                 (is (= {:status :current :version 2} result))
                 (finish! nil)))
              (.catch finish!)))))))

(deftest live-canonical-code-identity-is-shared-but-memory-is-user-isolated
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do
          (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required")
          (done))
        (let [first-user (randomUUID)
              second-user (randomUUID)
              first-user-id (identity/user-urn first-user)
              second-user-id (identity/user-urn second-user)
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              directory (mkdtempSync (join (tmpdir) "adam-neo4j-identity-live-"))
              first-path (join directory "first.jsonl")
              second-path (join directory "second.jsonl")
              first-repository (evidence/build-repository
                                {:user-uuid first-user :root "/first-checkout"
                                 :remote "git@github.com:AloiAI/adam.git"
                                 :commit "first-commit" :branch "first" :dirty? false})
              second-repository (evidence/build-repository
                                 {:user-uuid second-user :root "/second-checkout"
                                  :remote "https://github.com/AloiAI/adam.git"
                                  :commit "second-commit" :branch "second" :dirty? false})
              write-session!
              (fn [path session-id memory-id content cwd]
                (writeFileSync
                 path
                 (str (js/JSON.stringify #js {:type "session" :version 3
                                              :id session-id :cwd cwd}) "\n"
                      "{\"type\":\"message\",\"id\":\"call-entry\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call\",\"name\":\"read\",\"arguments\":{\"path\":\"src/shared.cljs\"}}]}}\n"
                      "{\"type\":\"message\",\"id\":\"result\",\"parentId\":\"call-entry\",\"message\":{\"role\":\"toolResult\",\"toolCallId\":\"call\"}}\n"
                      (js/JSON.stringify
                       #js {:type "custom" :id "memory" :parentId "result"
                            :customType "om.observations.recorded"
                            :data #js {:observations
                                       #js [#js {:id memory-id :content content
                                                :timestamp "2026-01-01T00:00:00.000Z"
                                                :relevance "high"
                                                :tokenCount 3
                                                :sourceEntryIds #js ["result"]}]
                                       :coversUpToId "result"}}) "\n")
                 "utf8"))
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (file:AdamCodeFile {repositoryId: $repositoryId}) DETACH DELETE file"
                          #js {:repositoryId (:id first-repository)})
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (repository:AdamRepository {id: $repositoryId}) DETACH DELETE repository"
                                   #js {:repositoryId (:id first-repository)})))
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (n) WHERE n.id CONTAINS $firstUser OR n.id CONTAINS $secondUser DETACH DELETE n"
                                   #js {:firstUser first-user :secondUser second-user})))
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (rmSync directory #js {:recursive true :force true})
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          (write-session! first-path "first-session" "111111111111"
                          "First user memory" "/first-checkout")
          (write-session! second-path "second-session" "222222222222"
                          "Second user memory" "/second-checkout")
          (is (= (:id first-repository) (:id second-repository)))
          (-> (store/initialize! replica {:id first-user-id})
              (.then (fn [_] (store/initialize! replica {:id second-user-id})))
              (.then (fn [_] (knowledge-store/ensure-file-evidence-schema! replica)))
              (.then (fn [_]
                       (sync/sync-session-file!
                        {:path first-path :user-uuid first-user :replica replica})))
              (.then (fn [_]
                       (sync/sync-session-file!
                        {:path second-path :user-uuid second-user :replica replica})))
              (.then (fn [_]
                       (knowledge-index/index-session!
                        {:store replica :path first-path :user-uuid first-user
                         :repository first-repository})))
              (.then (fn [_]
                       (knowledge-index/index-session!
                        {:store replica :path second-path :user-uuid second-user
                         :repository second-repository})))
              (.then (fn [_]
                       (knowledge-store/query-file-memory!
                        replica first-user-id (:id first-repository) "src/shared.cljs" 20)))
              (.then
               (fn [first-memories]
                 (is (= ["First user memory"] (mapv :content first-memories)))
                 (knowledge-store/query-file-memory!
                  replica second-user-id (:id second-repository) "src/shared.cljs" 20)))
              (.then
               (fn [second-memories]
                 (is (= ["Second user memory"] (mapv :content second-memories)))
                 (.run query-session
                       "MATCH (repository:AdamRepository {id: $repositoryId})-[:CONTAINS]->(file:AdamCodeFile {relativePath: $path})
                        OPTIONAL MATCH (:AdamUser)-[ownership:OWNS]->(repository)
                        RETURN count(DISTINCT repository) AS repositories,
                               count(DISTINCT file) AS files,
                               count(ownership) AS ownerships"
                       #js {:repositoryId (:id first-repository)
                            :path "src/shared.cljs"})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))
                       ^js repositories (.get record "repositories")
                       ^js files (.get record "files")
                       ^js ownerships (.get record "ownerships")]
                   (is (= 1 (.toNumber repositories)))
                   (is (= 1 (.toNumber files)))
                   (is (= 0 (.toNumber ownerships))))
                 (finish! nil)))
              (.catch finish!)))))))

(deftest live-claude-transcript-round-trip-and-file-evidence
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do
          (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required")
          (done))
        (let [user-uuid (randomUUID)
              user-id (identity/user-urn user-uuid)
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              main-path (join (.cwd js/process) "test/fixtures/claude/main.jsonl")
              subagent-path (join (.cwd js/process) "test/fixtures/claude/subagent.jsonl")
              repository (evidence/build-repository
                          {:user-uuid user-uuid :root "/work/repo"
                           :remote (str "git@example.com:" user-uuid "/repo.git")
                           :commit "main-head" :branch "main" :dirty? false
                           :worktrees [{:root "/work/repo" :commit "main-head"
                                        :branch "main" :dirty? false}
                                       {:root "/work/tree" :commit "feature-head"
                                        :branch "feature" :dirty? true}]})
              scan (claude-scanner/scan-session
                    {:session-id "claude-session-1"
                     :transcript-path main-path
                     :subagents [{:agent-id "agent-1"
                                  :transcript-path subagent-path}]})
              projection (claude-evidence/extract-projection
                          {:user-uuid user-uuid :repository repository :scan scan})
              session-id (identity/session-urn user-uuid "claude-code"
                                               "claude-session-1")
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (file:AdamCodeFile {repositoryId: $repositoryId}) DETACH DELETE file"
                          #js {:repositoryId (:id repository)})
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (repository:AdamRepository {id: $repositoryId}) DETACH DELETE repository"
                                   #js {:repositoryId (:id repository)})))
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                                   #js {:userUuid user-uuid})))
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          (-> (store/initialize! replica {:id user-id})
              (.then (fn [_]
                       (knowledge-store/ensure-file-evidence-schema! replica)))
              (.then (fn [_]
                       (claude-sync/sync-session-scan!
                        {:store replica :user-uuid user-uuid :scan scan})))
              (.then
               (fn [result]
                 (is (= :mirrored (:status result)))
                 (is (= 16 (:entries-written result)))
                 (knowledge-store/index-file-evidence! replica projection)))
              (.then
               (fn [_]
                 (claude-sync/sync-session-scan!
                  {:store replica :user-uuid user-uuid :scan scan})))
              (.then
               (fn [result]
                 (is (= :unchanged (:status result)))
                 (replica-migration/migrate-if-needed!
                  {:store replica :user-id user-id})))
              (.then
               (fn [migration]
                 (is (= {:status :migrated :version 2} migration))
                 (.run
                  query-session
                  "MATCH (u:AdamUser {id: $userId})-[:OWNS]->(s:AdamSession {id: $sessionId})
                   MATCH (s)-[:HAS_STREAM]->(stream:AdamTranscriptStream)
                   MATCH (s)-[:HAS_ENTRY]->(entry:AdamEntry)
                   OPTIONAL MATCH (s)-[:CURRENT_LEAF]->(leaf:AdamEntry)
                   OPTIONAL MATCH (:AdamEntry {entryId: 'compact-1'})-[:LOGICAL_PARENT]->(logicalParent:AdamEntry)
                   OPTIONAL MATCH (entry)-[touch:TOUCHES]->(file:AdamCodeFile {repositoryId: $repositoryId})
                   RETURN s.sourceKind AS sourceKind,
                          leaf.entryId AS leafId,
                          logicalParent.entryId AS logicalParentId,
                          count(DISTINCT stream) AS streams,
                          count(DISTINCT entry) AS entries,
                          count(DISTINCT touch) AS touches,
                          count(DISTINCT file) AS files,
                          collect(DISTINCT stream.entryCount) AS streamCounts,
                          collect(DISTINCT {id: entry.entryId, raw: entry.rawJson}) AS rawEntries"
                  #js {:userId user-id :sessionId session-id
                       :repositoryId (:id repository)})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= "claude-code" (.get record "sourceKind")))
                   (is (= "r-write" (.get record "leafId")))
                   (is (= "r-edit" (.get record "logicalParentId")))
                   (is (= 2 (.toNumber (.get record "streams"))))
                   (is (= 16 (.toNumber (.get record "entries"))))
                   (is (= 8 (.toNumber (.get record "touches"))))
                   (is (= 4 (.toNumber (.get record "files"))))
                   (is (= #{3 13}
                          (set (map (fn [^js value]
                                      (if (number? value) value (.toNumber value)))
                                    (array-seq (.get record "streamCounts"))))))
                   (is (= (into {} (map (juxt :entry-id :raw-json) (:entries scan)))
                          (into {} (map (fn [^js item]
                                         [(aget item "id") (aget item "raw")])
                                       (array-seq (.get record "rawEntries")))))))
                 (finish! nil)))
              (.catch finish!)))))))

(deftest live-claude-evidence-survives-a-removed-subagent-transcript
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do
          (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required")
          (done))
        (let [user-uuid (randomUUID)
              user-id (identity/user-urn user-uuid)
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              root (mkdtempSync (join (tmpdir) "adam-live-claude-"))
              main-path (join root "main.jsonl")
              subagent-path (join root "subagent.jsonl")
              repository (evidence/build-repository
                          {:user-uuid user-uuid :root "/work/repo"
                           :remote (str "git@example.com:" user-uuid "/repo.git")
                           :commit "main-head" :branch "main" :dirty? false
                           :worktrees [{:root "/work/repo" :commit "main-head"
                                        :branch "main" :dirty? false}
                                       {:root "/work/tree" :commit "feature-head"
                                        :branch "feature" :dirty? true}]})
              session-id (identity/session-urn user-uuid "claude-code" "claude-session-1")
              reconcile!
              (fn [notification]
                (claude-reconcile/reconcile!
                 {:locator-options {:config-home root}
                  :notification notification
                  :user-uuid user-uuid
                  :store replica
                  :resolve-repository! (fn [_ _] (js/Promise.resolve repository))}))
              touches-by-stream!
              (fn []
                (-> (.run query-session
                          "MATCH (s:AdamSession {id: $sessionId})-[:HAS_ENTRY]->(e:AdamEntry)
                           MATCH (e)-[t:TOUCHES]->(f:AdamCodeFile {repositoryId: $repositoryId})
                           RETURN e.streamId AS stream, count(t) AS touches, count(DISTINCT f) AS files
                           ORDER BY stream"
                          #js {:sessionId session-id :repositoryId (:id repository)})
                    (.then (fn [^js result]
                             (into {}
                                   (map (fn [^js record]
                                          [(.get record "stream")
                                           [(.toNumber (.get record "touches"))
                                            (.toNumber (.get record "files"))]])
                                        (array-seq (.-records result))))))))
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (file:AdamCodeFile {repositoryId: $repositoryId}) DETACH DELETE file"
                          #js {:repositoryId (:id repository)})
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (repository:AdamRepository {id: $repositoryId}) DETACH DELETE repository"
                                   #js {:repositoryId (:id repository)})))
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                                   #js {:userUuid user-uuid})))
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (rmSync root #js {:recursive true :force true})
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          (copyFileSync (join (.cwd js/process) "test/fixtures/claude/main.jsonl") main-path)
          (copyFileSync (join (.cwd js/process) "test/fixtures/claude/subagent.jsonl") subagent-path)
          (-> (store/initialize! replica {:id user-id})
              (.then (fn [_]
                       (reconcile! {:event "SubagentStop"
                                    :session-id "claude-session-1"
                                    :transcript-path subagent-path
                                    :parent-transcript-path main-path
                                    :agent-id "agent-1"
                                    :cwd "/work/repo"})))
              (.then (fn [result]
                       (is (= :projected (:projection-status result)))
                       (touches-by-stream!)))
              (.then
               (fn [before]
                 (is (= 8 (reduce + (map first (vals before)))))
                 (is (pos? (first (get before "agent:agent-1"))))
                 ;; Claude removes the finished subagent transcript; the parent
                 ;; then gains one more native Read on the selected branch.
                 (rmSync subagent-path)
                 (appendFileSync
                  main-path
                  (str "{\"type\":\"assistant\",\"uuid\":\"a-read-2\",\"parentUuid\":\"r-write\","
                       "\"sessionId\":\"claude-session-1\",\"cwd\":\"/work/repo\",\"requestId\":\"request-9\","
                       "\"timestamp\":\"2026-01-01T00:00:09.000Z\",\"message\":{\"role\":\"assistant\","
                       "\"content\":[{\"type\":\"tool_use\",\"id\":\"tool-read-2\",\"name\":\"Read\","
                       "\"input\":{\"file_path\":\"/work/repo/src/later.cljs\"}}]}}\n"
                       "{\"type\":\"last-prompt\",\"sessionId\":\"claude-session-1\",\"leafUuid\":\"a-read-2\"}\n")
                  "utf8")
                 (-> (reconcile! {:event "Stop"
                                  :session-id "claude-session-1"
                                  :transcript-path main-path
                                  :cwd "/work/repo"})
                     (.then (fn [result]
                              (is (= :projected (:projection-status result)))
                              (touches-by-stream!)))
                     (.then (fn [after] [before after])))))
              (.then
               (fn [[before after]]
                 (is (= (get before "agent:agent-1") (get after "agent:agent-1"))
                     "evidence of the removed subagent stream is rebuilt from its mirrored entries")
                 (is (= (inc (first (get before "main"))) (first (get after "main")))
                     "the present parent stream is rebuilt from its transcript")
                 (is (= (inc (second (get before "main"))) (second (get after "main"))))
                 (finish! nil)))
              (.catch finish!)))))))

(deftest live-file-memory-retrieval-omits-dropped-observations-before-the-bound
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do
          (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required")
          (done))
        (let [user-uuid (randomUUID)
              user-id (identity/user-urn user-uuid)
              remote (str "git@example.com:" user-uuid "/memories.git")
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              directory (mkdtempSync (join (tmpdir) "adam-neo4j-dropped-live-"))
              path (join directory "session.jsonl")
              repository (evidence/build-repository
                          {:user-uuid user-uuid :root "/checkout" :remote remote
                           :commit "head" :branch "main" :dirty? false})
              observation (fn [id content]
                            #js {:id id :content content
                                 :timestamp "2026-01-01T00:00:00.000Z"
                                 :relevance "high" :tokenCount 3
                                 :sourceEntryIds #js ["result"]})
              custom (fn [id parent custom-type data]
                       (str (js/JSON.stringify #js {:type "custom" :id id :parentId parent
                                                    :customType custom-type :data data})
                            "\n"))
              rows (fn [memories] (mapv (juxt :kind :content) memories))
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (file:AdamCodeFile {repositoryId: $repositoryId}) DETACH DELETE file"
                          #js {:repositoryId (:id repository)})
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (repository:AdamRepository {id: $repositoryId}) DETACH DELETE repository"
                                   #js {:repositoryId (:id repository)})))
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                                   #js {:userUuid user-uuid})))
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (rmSync directory #js {:recursive true :force true})
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          ;; Three tombstoned observations sort lowest by memory id, ahead of the
          ;; legacy (no dropped property) and active observations; a reflection is
          ;; supported only by a dropped observation.
          (writeFileSync
           path
           (str (js/JSON.stringify #js {:type "session" :version 3 :id "dropped-session" :cwd "/checkout"}) "\n"
                "{\"type\":\"message\",\"id\":\"call-entry\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call\",\"name\":\"read\",\"arguments\":{\"path\":\"src/shared.cljs\"}}]}}\n"
                "{\"type\":\"message\",\"id\":\"result\",\"parentId\":\"call-entry\",\"message\":{\"role\":\"toolResult\",\"toolCallId\":\"call\"}}\n"
                (custom "memory-1" "result" "om.observations.recorded"
                        #js {:observations #js [(observation "000000000001" "Dropped memory 1")
                                                (observation "000000000002" "Dropped memory 2")
                                                (observation "000000000003" "Dropped memory 3")
                                                (observation "eeeeeeeeeeee" "Legacy memory")
                                                (observation "ffffffffffff" "Active memory")]
                             :coversUpToId "result"})
                (custom "memory-2" "memory-1" "om.reflections.recorded"
                        #js {:reflections #js [#js {:id "dddddddddddd"
                                                    :content "Reflection over dropped support"
                                                    :supportingObservationIds #js ["000000000001"]
                                                    :tokenCount 4}]
                             :coversUpToId "memory-1"})
                (custom "drop-1" "memory-2" "om.observations.dropped"
                        #js {:observationIds #js ["000000000001" "000000000002" "000000000003"]
                             :coversUpToId "memory-2"}))
           "utf8")
          (-> (store/initialize! replica {:id user-id})
              (.then (fn [_] (knowledge-store/ensure-file-evidence-schema! replica)))
              (.then (fn [_] (sync/sync-session-file! {:path path :user-uuid user-uuid :replica replica})))
              (.then (fn [_] (knowledge-index/index-session!
                              {:store replica :path path :user-uuid user-uuid :repository repository})))
              (.then (fn [_]
                       ;; A legacy projection recorded no dropped property at all.
                       (.run query-session
                             "MATCH (o:AdamObservation {memoryId: 'eeeeeeeeeeee'}) WHERE o.id CONTAINS $userUuid REMOVE o.dropped"
                             #js {:userUuid user-uuid})))
              (.then (fn [_] (knowledge-store/query-file-memory!
                              replica user-id (:id repository) "src/shared.cljs" 20)))
              (.then
               (fn [memories]
                 (is (= [[:observation "Legacy memory"]
                         [:observation "Active memory"]
                         [:reflection "Reflection over dropped support"]]
                        (rows memories)))
                 (knowledge-store/query-file-memory!
                  replica user-id (:id repository) "src/shared.cljs" 2)))
              (.then
               (fn [memories]
                 (is (= [[:observation "Legacy memory"] [:observation "Active memory"]]
                        (rows memories))
                     "dropped observations do not consume result slots at a small bound")
                 (.run query-session
                       "MATCH (:AdamUser {id: $userId})-[:OWNS]->(:AdamSession)-[:HAS_MEMORY]->(o:AdamObservation {dropped: true})
                        MATCH (o)-[:ABOUT]->(file:AdamCodeFile {repositoryId: $repositoryId})
                        MATCH (o)-[:SOURCED_FROM]->(source:AdamEntry)
                        RETURN count(DISTINCT o) AS dropped, count(DISTINCT file) AS files, count(DISTINCT source) AS sources"
                       #js {:userId user-id :repositoryId (:id repository)})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= 3 (.toNumber (.get record "dropped"))))
                   (is (= 1 (.toNumber (.get record "files"))))
                   (is (= 1 (.toNumber (.get record "sources")))))
                 (knowledge-tool/execute!
                  {:get-store! (fn [] (js/Promise.resolve replica))
                   :get-user! (fn [] (js/Promise.resolve {:user-uuid user-uuid}))
                   :resolve-repository! (fn [_ _] (js/Promise.resolve nil))}
                  {:cwd "/elsewhere" :origin remote :path "src/shared.cljs"})))
              (.then
               (fn [{:keys [content details]}]
                 (is (= 3 (:result-count details)))
                 (is (false? (:truncated? details)))
                 (is (re-find #"Legacy memory" content))
                 (is (re-find #"Active memory" content))
                 (is (re-find #"Reflection over dropped support" content))
                 (is (not (re-find #"Dropped memory" content)))
                 (finish! nil)))
              (.catch finish!)))))))

(deftest live-memory-sidecar-round-trip-and-resume
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do
          (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required")
          (done))
        (let [user-uuid (randomUUID)
              user-id (identity/user-urn user-uuid)
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              directory (mkdtempSync (join (tmpdir) "adam-memory-live-"))
              transcript-path (join directory "session.jsonl")
              sidecar-path (join directory "events.jsonl")
              conflict-path (join directory "conflict.jsonl")
              physical-path (join directory "physical.jsonl")
              unsafe-path (join directory "unsafe.jsonl")
              transcript-fixture (.resolve node-path "test/fixtures/claude/main.jsonl")
              memory-root (.resolve node-path "docs/fixtures/memory-protocol-v1")
              locator {:source-kind "claude-code"
                       :source-session-id "session-123"
                       :producer-id "org.example.claude-memory"}
              stream-id (identity/memory-stream-urn
                         user-uuid "claude-code" "session-123"
                         "org.example.claude-memory")
              conflict-stream-id (identity/memory-stream-urn
                                  user-uuid "claude-code" "session-123"
                                  "org.example.conflict")
              physical-stream-id (identity/memory-stream-urn
                                  user-uuid "claude-code" "session-123"
                                  "org.example.physical")
              unsafe-stream-id (identity/memory-stream-urn
                                user-uuid "claude-code" "session-123"
                                "org.example.unsafe")
              source-lines (atom nil)
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (stream:AdamMemoryStream {userId: $userId})
                           OPTIONAL MATCH (stream)-[:HAS_RECORD]->(record:AdamMemoryRecord)
                           DETACH DELETE record, stream"
                          #js {:userId user-id})
                    (.then
                     (fn [_]
                       (.run query-session
                             "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                             #js {:userUuid user-uuid})))
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (rmSync directory #js {:recursive true :force true})
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          (writeFileSync
           transcript-path
           (.replaceAll (readFileSync transcript-fixture "utf8")
                        "claude-session-1" "session-123")
           #js {:encoding "utf8" :mode 384})
          (writeFileSync
           sidecar-path
           (readFileSync (join memory-root "valid-events.jsonl") "utf8")
           #js {:encoding "utf8" :mode 384})
          (writeFileSync
           conflict-path
           (-> (readFileSync (join memory-root "conflicting-event-id.jsonl") "utf8")
               (.replaceAll "org.example.claude-memory" "org.example.conflict"))
           #js {:encoding "utf8" :mode 384})
          (let [physical-valid
                (-> (first (.split (.trimEnd
                                    (readFileSync (join memory-root "valid-events.jsonl")
                                                  "utf8")) "\n"))
                    (.replaceAll "org.example.claude-memory" "org.example.physical"))]
            (writeFileSync physical-path
                           (str physical-valid "\n"
                                (.repeat "x" (inc (* 1024 1024))) "\n")
                           #js {:encoding "utf8" :mode 384}))
          (writeFileSync
           unsafe-path
           (-> (readFileSync (join memory-root "valid-events.jsonl") "utf8")
               (.replaceAll "org.example.claude-memory" "org.example.unsafe"))
           #js {:encoding "utf8" :mode 420})
          (reset! source-lines (vec (.split (.trimEnd (readFileSync sidecar-path "utf8")) "\n")))
          (-> (store/initialize! replica {:id user-id})
              (.then
               (fn [_]
                 (claude-sync/sync-session-scan!
                  {:store replica :user-uuid user-uuid
                   :scan (claude-scanner/scan-session
                          {:session-id "session-123"
                           :transcript-path transcript-path
                           :subagents []})})))
              (.then
               (fn [_]
                 (memory-sync/sync-sidecar-file!
                  (merge locator {:store replica :user-uuid user-uuid
                                  :path sidecar-path :batch-bytes 900}))))
              (.then
               (fn [result]
                 (is (= :mirrored (:status result)))
                 (is (= 4 (:records-written result)))
                 (memory-store/read-memory-records! replica stream-id)))
              (.then
               (fn [records]
                 (is (= @source-lines (mapv :raw-json records)))
                 (memory-sync/sync-sidecar-file!
                  (merge locator {:store replica :user-uuid user-uuid
                                  :path sidecar-path}))))
              (.then
               (fn [result]
                 (is (= :unchanged (:status result)))
                 (let [next-event
                       (js/JSON.stringify
                        #js {:protocolVersion 1
                             :eventId "event-live-append"
                             :kind "source.covered"
                             :producer #js {:id "org.example.claude-memory"
                                            :version "0.1.0"}
                             :source #js {:kind "claude-code" :sessionId "session-123"}
                             :sourceCheckpoint
                             #js {:streams
                                  #js [#js {:streamId "main" :committedBytes 500
                                            :prefixSha256
                                            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                                            :selectedLeafEntryId "entry-5"}]}
                             :recordedAt "2026-09-30T12:04:00.000Z"})]
                   (appendFileSync sidecar-path (str next-event "\n") "utf8")
                   (swap! source-lines conj next-event)
                   (memory-sync/sync-sidecar-file!
                    (merge locator {:store replica :user-uuid user-uuid
                                    :path sidecar-path})))))
              (.then
               (fn [result]
                 (is (= :mirrored (:status result)))
                 (is (= 1 (:records-written result)))
                 (appendFileSync sidecar-path "{\"eventId\":\"partial" "utf8")
                 (memory-sync/sync-sidecar-file!
                  (merge locator {:store replica :user-uuid user-uuid
                                  :path sidecar-path}))))
              (.then
               (fn [result]
                 (is (= :unchanged (:status result)))
                 (memory-store/read-memory-records! replica stream-id)))
              (.then
               (fn [records]
                 (is (= @source-lines (mapv :raw-json records)))
                 (memory-sync/sync-sidecar-file!
                  {:store replica :user-uuid user-uuid :path conflict-path
                   :source-kind "claude-code" :source-session-id "session-123"
                   :producer-id "org.example.conflict"})))
              (.then
               (fn [result]
                 (is (= :conflict (:status result)))
                 (is (= :immutable-event-conflict
                        (get-in result [:conflict :reason])))
                 (is (= 2 (:records-written result)))
                 (memory-store/read-memory-records! replica conflict-stream-id)))
              (.then
               (fn [records]
                 (is (= 2 (count records)))
                 (let [blocked-event
                       (-> (first (.split (.trimEnd
                                          (readFileSync (join memory-root "valid-events.jsonl")
                                                        "utf8")) "\n"))
                           (.replace "event-observations-1" "event-after-conflict")
                           (.replaceAll "org.example.claude-memory" "org.example.conflict"))]
                   (appendFileSync conflict-path (str blocked-event "\n") "utf8")
                   (memory-sync/sync-sidecar-file!
                    {:store replica :user-uuid user-uuid :path conflict-path
                     :source-kind "claude-code" :source-session-id "session-123"
                     :producer-id "org.example.conflict"}))))
              (.then
               (fn [result]
                 (is (= :conflict (:status result)))
                 (is (= 1 (:records-written result)))
                 (memory-store/read-memory-records! replica conflict-stream-id)))
              (.then
               (fn [records]
                 (is (= 3 (count records)))
                 (is (= :blocked (:semantic-status (last records))))
                 (memory-sync/sync-sidecar-file!
                  {:store replica :user-uuid user-uuid :path physical-path
                   :source-kind "claude-code" :source-session-id "session-123"
                   :producer-id "org.example.physical"})))
              (.then
               (fn [result]
                 (is (= :conflict (:status result)))
                 (is (= :record-too-large (get-in result [:conflict :reason])))
                 (is (= 1 (:records-written result)))
                 (memory-store/read-memory-records! replica physical-stream-id)))
              (.then
               (fn [records]
                 (is (= 1 (count records))
                     "records before a physical conflict remain mirrored")
                 (memory-sync/sync-sidecar-file!
                  {:store replica :user-uuid user-uuid :path unsafe-path
                   :source-kind "claude-code" :source-session-id "session-123"
                   :producer-id "org.example.unsafe"})))
              (.then
               (fn [result]
                 (is (= :not-ingested (:status result)))
                 (is (= :unsafe-permissions (:reason result)))
                 (memory-store/read-memory-records! replica unsafe-stream-id)))
              (.then
               (fn [records]
                 (is (empty? records)
                     "an unsafe source does not create a permanent graph conflict")
                 (chmodSync unsafe-path 384)
                 (memory-sync/sync-sidecar-file!
                  {:store replica :user-uuid user-uuid :path unsafe-path
                   :source-kind "claude-code" :source-session-id "session-123"
                   :producer-id "org.example.unsafe"})))
              (.then
               (fn [result]
                 (is (= :mirrored (:status result)))
                 (is (= 4 (:records-written result)))
                 (rmSync unsafe-path)
                 (memory-sync/sync-sidecar-file!
                  {:store replica :user-uuid user-uuid :path unsafe-path
                   :source-kind "claude-code" :source-session-id "session-123"
                   :producer-id "org.example.unsafe"})))
              (.then
               (fn [result]
                 (is (= :not-ingested (:status result)))
                 (is (= :missing-sidecar (:reason result)))
                 (memory-store/read-memory-records! replica unsafe-stream-id)))
              (.then
               (fn [records]
                 (is (= 4 (count records))
                     "a missing source preserves its last mirrored prefix")
                 (memory-store/read-memory-records! replica stream-id)))
              (.then
               (fn [records]
                 (is (= 5 (count records))
                     "conflicting producer streams do not alter the healthy stream")
                 (finish! nil)))
              (.catch finish!)))))))

(deftest live-aggregate-memory-projection-keeps-producers-distinct
  (async done
    (let [uri (environment "ADAM_TEST_NEO4J_URI")
          username (environment "ADAM_TEST_NEO4J_USERNAME")
          password (environment "ADAM_TEST_NEO4J_PASSWORD")
          database (or (environment "ADAM_TEST_NEO4J_DATABASE") "neo4j")]
      (if-not (and uri username password)
        (do (is false "ADAM_TEST_NEO4J_URI, USERNAME, and PASSWORD are required") (done))
        (let [user-uuid (randomUUID)
              user-id (identity/user-urn user-uuid)
              auth-token (.basic (.-auth neo4j) username password)
              ^js driver ((.-driver neo4j) uri auth-token)
              replica (adam-neo4j/replica-with-driver driver database)
              ^js query-session (.session driver #js {:database database})
              root (mkdtempSync (join (tmpdir) "adam-live-aggregate-memory-"))
              first-path (join root "producer-a.jsonl")
              second-path (join root "producer-b.jsonl")
              transcript-path (join (.cwd js/process) "test/fixtures/claude/main.jsonl")
              repository (evidence/build-repository
                          {:user-uuid user-uuid :root "/work/repo"
                           :remote (str "git@example.com:" user-uuid "/aggregate.git")
                           :commit "main-head" :branch "main" :dirty? false})
              event-json
              (fn [producer content]
                (js/JSON.stringify
                 #js {:protocolVersion 1 :eventId (str "event-" producer)
                      :kind "observations.recorded"
                      :recordedAt "2026-01-01T00:00:00.000Z"
                      :producer #js {:id producer :version "1.0.0"}
                      :source #js {:kind "claude-code" :sessionId "claude-session-1"}
                      :sourceCheckpoint
                      #js {:streams #js [#js {:streamId "main" :committedBytes 1
                                              :prefixSha256 (apply str (repeat 64 "a"))
                                              :selectedLeafEntryId "a-request-1"}]}
                      :observations
                      #js [#js {:id "aaaaaaaaaaaa" :content content
                                :timestamp "2026-01-01T00:00:00.000Z"
                                :relevance "high" :tokenCount 2
                                :sourceEntries #js [#js {:streamId "main"
                                                        :entryId "a-request-1"}]}]}))
              reconcile-with!
              (fn [resolved-repository]
                (claude-reconcile/reconcile!
                 {:locator-options {:config-home root}
                  :notification {:event "Stop" :session-id "claude-session-1"
                                 :transcript-path transcript-path :cwd "/work/repo"}
                  :user-uuid user-uuid :store replica
                  :resolve-repository!
                  (fn [_ _] (js/Promise.resolve resolved-repository))}))
              reconcile! #(reconcile-with! repository)
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (file:AdamCodeFile {repositoryId: $repositoryId}) DETACH DELETE file"
                          #js {:repositoryId (:id repository)})
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (repository:AdamRepository {id: $repositoryId}) DETACH DELETE repository"
                                   #js {:repositoryId (:id repository)})))
                    (.then (fn [_]
                             (.run query-session
                                   "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                                   #js {:userUuid user-uuid})))
                    (.catch (fn [_] nil))
                    (.finally
                     (fn []
                       (rmSync root #js {:recursive true :force true})
                       (-> (.close query-session)
                           (.then (fn [_] (store/close! replica)))
                           (.finally
                            (fn []
                              (when error (is false (.-stack error)))
                              (done))))))))]
          (writeFileSync first-path (str (event-json "producer-a" "memory a") "\n")
                         #js {:encoding "utf8" :mode 384})
          (writeFileSync second-path (str (event-json "producer-b" "memory b") "\n")
                         #js {:encoding "utf8" :mode 384})
          (-> (store/initialize! replica {:id user-id})
              (.then (fn [_] (reconcile!)))
              (.then
               (fn [_]
                 (memory-sync/sync-sidecar-file!
                  {:store replica :user-uuid user-uuid :path first-path
                   :source-kind "claude-code" :source-session-id "claude-session-1"
                   :producer-id "producer-a"})))
              (.then
               (fn [_]
                 (memory-sync/sync-sidecar-file!
                  {:store replica :user-uuid user-uuid :path second-path
                   :source-kind "claude-code" :source-session-id "claude-session-1"
                   :producer-id "producer-b"})))
              (.then (fn [_] (reconcile!)))
              (.then
               (fn [result]
                 (is (= :projected (:projection-status result)))
                 (knowledge-store/query-file-memory!
                  replica user-id (:id repository) "src/read.cljs" 10)))
              (.then
               (fn [memories]
                 (is (= [["producer-a" "memory a"] ["producer-b" "memory b"]]
                        (mapv (juxt :producer :content) memories)))
                 (is (every? #(= "claude-code" (:source-kind %)) memories))
                 (is (every? #(= [{:stream-id "main" :entry-id "a-request-1"}]
                                 (:source-entries %)) memories))
                 (is (every? #(= "main" (get-in % [:source-contexts 0 :stream-id]))
                             memories))
                 (.run query-session
                       "MATCH (:AdamSession {sourceKind: 'claude-code', sourceSessionId: 'claude-session-1'})-[:HAS_MEMORY]->(memory:AdamObservation)
                        WHERE memory.memoryId = 'aaaaaaaaaaaa'
                        RETURN count(memory) AS memories, count(DISTINCT memory.producer) AS producers"
                       #js {})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= 2 (.toNumber (.get record "memories"))))
                   (is (= 2 (.toNumber (.get record "producers")))))
                 (-> (memory-store/mark-memory-stream-conflict!
                      replica
                      {:stream-id (identity/memory-stream-urn
                                   user-uuid "claude-code" "claude-session-1" "producer-a")
                       :conflict-class :semantic
                       :reason :immutable-event-conflict
                       :source-file first-path})
                     (.then
                      (fn [_]
                        (rmSync first-path)
                        (reconcile-with! nil))))))
              (.then
               (fn [result]
                 (is (= :projected (:projection-status result)))
                 (.run query-session
                       "MATCH (:AdamSession {sourceKind: 'claude-code', sourceSessionId: 'claude-session-1'})-[:HAS_MEMORY]->(memory:AdamObservation)
                        OPTIONAL MATCH (memory)-[:ABOUT]->(file)
                        RETURN count(DISTINCT memory) AS memories, count(DISTINCT file) AS files"
                       #js {})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= 2 (.toNumber (.get record "memories")))
                       "aggregate memory retains a conflicted producer with a missing sidecar")
                   (is (zero? (.toNumber (.get record "files")))))
                 (reconcile!)))
              (.then
               (fn [_]
                 (knowledge-store/query-file-memory!
                  replica user-id (:id repository) "src/read.cljs" 10)))
              (.then
               (fn [memories]
                 (is (= 2 (count memories))
                     "later repository reconciliation restores file links")
                 (finish! nil)))
              (.catch finish!)))))))
