(ns adam.replica.neo4j-live-test
  (:require [adam.replica.identity :as identity]
            [adam.replica.neo4j :as adam-neo4j]
            [adam.replica.store :as store]
            [adam.replica.sync :as sync]
            [cljs.test :refer [async deftest is]]
            ["neo4j-driver" :as neo4j]
            ["node:crypto" :refer [randomUUID]]
            ["node:fs" :refer [mkdtempSync rmSync writeFileSync]]
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
              parent-header (js/JSON.stringify
                             #js {:type "session" :version 3
                                  :id "parent-live" :cwd "/repo"})
              child-header (js/JSON.stringify
                            #js {:type "session" :version 3
                                 :id "child-live" :cwd "/repo"
                                 :parentSession parent-path})
              child-entry "{\"type\":\"message\",\"id\":\"entry-live\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"hello\"}}"
              child-session-id (identity/session-urn user-uuid "child-live")
              finish!
              (fn [error]
                (-> (.run query-session
                          "MATCH (n) WHERE n.id CONTAINS $userUuid DETACH DELETE n"
                          #js {:userUuid user-uuid})
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
          (writeFileSync child-path (str child-header "\n" child-entry "\n") "utf8")
          (-> (store/initialize! replica {:id user-id})
              (.then
               (fn [_]
                 (sync/sync-session-file!
                  {:path child-path
                   :user-uuid user-uuid
                   :current-leaf-id "entry-live"
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
                 (is (= [child-entry] (mapv :raw-json (:entries restored))))
                 (is (= "entry-live" (get-in restored [:session :current-leaf-id])))
                 (.run query-session
                       "MATCH (:AdamSession {id: $childId})-[:FORKED_FROM]->(parent:AdamSession)
                        RETURN parent.piSessionId AS parentId"
                       #js {:childId child-session-id})))
              (.then
               (fn [^js result]
                 (let [^js record (first (array-seq (.-records result)))]
                   (is (= "parent-live" (.get record "parentId"))))
                 (finish! nil)))
              (.catch finish!)))))))
