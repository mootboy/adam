(ns adam.replica.neo4j-live-test
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.index :as knowledge-index]
            [adam.knowledge.store :as knowledge-store]
            [adam.knowledge.surfaces :as knowledge-surfaces]
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
                 (let [[observation reflection] memories]
                   (is (= 2 (count memories)))
                   (is (= :observation (:kind observation)))
                   (is (= "aaaaaaaaaaaa" (:memory-id observation)))
                   (is (= true (:dropped? observation)))
                   (is (= :reflection (:kind reflection)))
                   (is (= "bbbbbbbbbbbb" (:memory-id reflection)))
                   (is (= ["result-live"] (:source-entry-ids observation)))
                   (is (= [{:entry-id "result-live" :commit "live-commit"
                            :branch "live" :dirty? true}]
                          (:source-contexts reflection))))
                 (.call (aget file-context-tool "execute") file-context-tool
                        "call-live"
                        #js {:origin "https://github.com/AloiAI/adam.git"
                             :path "src/live.cljs"}
                        nil nil #js {:cwd "/unrelated/repository"})))
              (.then
               (fn [tool-result]
                 (let [text (aget (aget (aget tool-result "content") 0) "text")]
                   (is (= "ok" (aget (aget tool-result "details") "status")))
                   (is (re-find #"Live file decision" text))
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
