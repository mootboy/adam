(ns adam.replica.neo4j-live-test
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.index :as knowledge-index]
            [adam.knowledge.store :as knowledge-store]
            [adam.replica.identity :as identity]
            [adam.replica.neo4j :as adam-neo4j]
            [adam.replica.restore :as restore]
            [adam.replica.store :as store]
            [adam.replica.sync :as sync]
            [cljs.test :refer [async deftest is]]
            ["neo4j-driver" :as neo4j]
            ["node:crypto" :refer [randomUUID]]
            ["node:fs" :refer [mkdtempSync readFileSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

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
              materialized-path (join directory "materialized-child.jsonl")
              child-after-parent-session-id
              (identity/session-urn user-uuid "child-after-parent-live")
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (:AdamUser {id: $userId})-[:OWNS]->(:AdamRepository)-[:CONTAINS]->(file:AdamCodeFile)
                           DETACH DELETE file"
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
                 (.run query-session
                       "MATCH (:AdamSession {id: $sessionId})-[:HAS_MEMORY]->(observation:AdamObservation)-[:ABOUT]->(file:AdamCodeFile)
                        MATCH (reflection:AdamReflection)-[:SUPPORTED_BY]->(observation)
                        OPTIONAL MATCH (observation)-[:SOURCED_FROM]->(source:AdamEntry)
                        OPTIONAL MATCH (source)-[touch:TOUCHES]->(file)
                        RETURN observation.memoryId AS observationId, observation.dropped AS dropped,
                               reflection.memoryId AS reflectionId, file.relativePath AS path,
                               source.entryId AS entryId, touch.commit AS commit,
                               touch.branch AS branch, touch.dirty AS dirty"
                       #js {:sessionId child-session-id})))
              (.then
               (fn [^js result]
                 (let [records (array-seq (.-records result))
                       ^js first-record (first records)]
                   (is (= 1 (count records)))
                   (is (= "aaaaaaaaaaaa" (.get first-record "observationId")))
                   (is (= true (.get first-record "dropped")))
                   (is (= "bbbbbbbbbbbb" (.get first-record "reflectionId")))
                   (is (= "src/live.cljs" (.get first-record "path")))
                   (is (= "result-live" (.get first-record "entryId")))
                   (is (= "live-commit" (.get first-record "commit")))
                   (is (= "live" (.get first-record "branch")))
                   (is (= true (.get first-record "dirty"))))
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
