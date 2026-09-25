(ns adam.replica.register-test
  (:require [adam.knowledge.store :as knowledge-store]
            [adam.replica.register :as register]
            [adam.replica.store :as store]
            [cljs.test :refer [async deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defrecord LifecycleReplica [checkpoint initializations writes completions closes evidence-schemas projections]
  knowledge-store/FileEvidenceStore
  (ensure-file-evidence-schema! [_]
    (swap! evidence-schemas inc)
    (js/Promise.resolve nil))
  (index-file-evidence! [_ projection]
    (if (= :fail @projections)
      (js/Promise.reject (js/Error. "evidence unavailable"))
      (do
        (swap! projections conj projection)
        (js/Promise.resolve nil))))

  store/SessionReplicaStore
  (initialize! [_ user]
    (swap! initializations conj user)
    (js/Promise.resolve nil))
  (get-checkpoint! [_ _session-id]
    (js/Promise.resolve @checkpoint))
  (write-batch! [_ request]
    (swap! writes conj request)
    (reset! checkpoint (:checkpoint request))
    (js/Promise.resolve nil))
  (complete-session! [_ request]
    (swap! completions conj request)
    (reset! checkpoint (:checkpoint request))
    (js/Promise.resolve nil))
  (mark-conflict! [_ _conflict]
    (js/Promise.resolve nil))
  (list-sessions! [_ _query]
    (js/Promise.resolve []))
  (read-session! [_ _session-id]
    (js/Promise.resolve nil))
  (close! [_]
    (swap! closes inc)
    (js/Promise.resolve nil)))

(defn- fake-pi []
  (let [commands (atom {})
        tools (atom {})
        events (atom {})]
    {:pi #js {:registerCommand (fn [name definition]
                                (swap! commands assoc name definition))
              :registerTool (fn [definition]
                              (swap! tools assoc (aget definition "name") definition))
              :on (fn [event handler]
                    (swap! events assoc event handler))}
     :commands commands
     :tools tools
     :events events}))

(deftest disabled-registration-keeps-status-available
  (let [{:keys [pi commands tools events]} (fake-pi)
        notifications (atom [])]
    (register/register!
     pi
     {:config {:enabled? false :reason "configuration missing"}})
    (is (contains? @commands "adam:status"))
    (is (contains? @commands "adam:context"))
    (is (not (contains? @tools "adam_file_context")))
    (is (empty? @events))
    ((aget (get @commands "adam:status") "handler")
     ""
     #js {:ui #js {:notify (fn [message level]
                            (swap! notifications conj [message level]))}})
    (is (= "info" (second (first @notifications))))
    (is (re-find #"disabled" (first (first @notifications))))
    (is (re-find #"configuration missing" (first (first @notifications))))))

(deftest retries-after-an-isolated-backend-outage
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-outage-"))
          path (join directory "session.jsonl")
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      (atom 0) (atom []))
          attempts (atom 0)
          {:keys [pi]} (fake-pi)
          notifications (atom [])
          runtime (register/register!
                   pi
                   {:config {:enabled? true}
                    :create-replica
                    (fn []
                      (if (= 1 (swap! attempts inc))
                        (js/Promise.reject (js/Error. "neo4j unavailable"))
                        replica))
                    :load-user (fn [] {:user-uuid "user-1"})
                    :resolve-git-identity (fn [_ctx] (js/Promise.resolve nil))})
          ctx #js {:sessionManager
                   #js {:getSessionFile (fn [] path)
                        :getLeafId (fn [] nil)}
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications conj [message level]))}}]
      (writeFileSync path
                     "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}\n"
                     "utf8")
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:synchronize! runtime) ctx)))
          (.then
           (fn [_]
             (is (= false (:connected? ((:status runtime)))))
             (is (= [["adam session replication unavailable; local JSONL remains authoritative"
                      "warning"]]
                    @notifications))
             ((:synchronize! runtime) ctx)))
          (.then
           (fn [_]
             (is (= true (:connected? ((:status runtime)))))
             (is (= 2 @attempts))
             (is (= "adam session replication recovered" (first (last @notifications))))
             ((:shutdown! runtime))))
          (.then
           (fn [_]
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))

(deftest lifecycle-syncs-a-persisted-session-and-reports-status
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-test-"))
          path (join directory "session.jsonl")
          checkpoint (atom nil)
          initializations (atom [])
          writes (atom [])
          completions (atom [])
          closes (atom 0)
          replica (->LifecycleReplica checkpoint initializations writes completions closes
                                      (atom 0) (atom []))
          {:keys [pi commands tools events]} (fake-pi)
          notifications (atom [])
          clock (atom -10)
          runtime (register/register!
                   pi
                   {:config {:enabled? true}
                    :now-ms (fn [] (swap! clock + 10))
                    :create-replica (fn [] replica)
                    :load-user (fn [] {:user-uuid "user-1"})
                    :resolve-repository (fn [_cwd _user] (js/Promise.resolve nil))
                    :resolve-git-identity
                    (fn [_ctx]
                      (js/Promise.resolve
                       {:id "urn:adam:identity:git-email:hash"
                        :kind "git-email"
                        :value "Linus@Example.com"
                        :normalized-value "linus@example.com"
                        :display-value "Linus@Example.com"}))})
          ctx #js {:cwd "/repo"
                   :sessionManager
                   #js {:getSessionFile (fn [] path)
                        :getLeafId (fn [] "entry-1")}
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications conj [message level]))}}]
      (writeFileSync
       path
       (str "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}\n"
            "{\"type\":\"message\",\"id\":\"entry-1\",\"parentId\":null}\n")
       "utf8")
      (is (contains? @events "session_start"))
      (is (contains? @events "turn_end"))
      (is (contains? @tools "adam_file_context"))
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((get @events "turn_end") #js {} ctx)))
          (.then
           (fn [_]
             (is (= 2 (count @initializations)))
             (is (= "urn:adam:identity:git-email:hash"
                    (get-in (second @initializations) [:identity :id])))
             (is (= 1 (count @writes)))
             (is (= 1 (count @completions)))
             (is (= "entry-1"
                    (get-in (first @writes) [:session :current-leaf-id])))
             (is (= :mirrored (get-in ((:status runtime)) [:last-result :status])))
             (is (= "waiting for a Git cwd or explicit file-tool path"
                    (:file-evidence-status ((:status runtime)))))
             (let [timing (:last-timing ((:status runtime)))]
               (is (= :turn-end (:event timing)))
               (is (every? number?
                           ((juxt :queue-wait-ms :initialization-ms :replica-sync-ms
                                  :repository-discovery-ms :evidence-extraction-ms
                                  :neo4j-projection-ms :total-ms)
                            timing))))
             ((aget (get @commands "adam:status") "handler") "" ctx)
             (is (re-find #"connected" (first (last @notifications))))
             (is (re-find #"Last timing: turn-end" (first (last @notifications))))
             (is (re-find #"l\*\*\*@example.com" (first (last @notifications))))
             (is (not (re-find #"linus@example.com" (first (last @notifications)))))
             ((:shutdown! runtime))))
          (.then
           (fn [_]
             (is (= 1 @closes))
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))

(deftest lifecycle-indexes-file-evidence-after-the-lossless-mirror
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-evidence-"))
          path (join directory "session.jsonl")
          projections (atom [])
          schemas (atom 0)
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      schemas projections)
          {:keys [pi]} (fake-pi)
          repository {:id "urn:adam:repository:user-1:hash"
                      :user-id "urn:adam:user:user-1"
                      :root "/repo"
                      :normalized-remote "github.com/AloiAI/adam"
                      :commit "abc123"
                      :branch "main"
                      :dirty? false
                      :worktrees [{:root "/repo" :commit "abc123"
                                   :branch "main" :dirty? false}]}
          runtime (register/register!
                   pi
                   {:config {:enabled? true}
                    :create-replica (fn [] replica)
                    :load-user (fn [] {:user-uuid "user-1"})
                    :resolve-git-identity (fn [_] (js/Promise.resolve nil))
                    :resolve-repository (fn [_cwd _user] (js/Promise.resolve repository))})
          ctx #js {:cwd "/repo"
                   :sessionManager #js {:getSessionFile (fn [] path)
                                        :getLeafId (fn [] "memory-bad")}
                   :ui #js {:notify (fn [_ _] nil)}}]
      (writeFileSync
       path
       (str "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}\n"
            "{\"type\":\"message\",\"id\":\"assistant-1\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call-1\",\"name\":\"read\",\"arguments\":{\"path\":\"src/a.cljs\"}}]}}\n"
            "{\"type\":\"message\",\"id\":\"result-1\",\"parentId\":\"assistant-1\",\"message\":{\"role\":\"toolResult\",\"toolCallId\":\"call-1\"}}\n"
            "{\"type\":\"custom\",\"id\":\"memory-bad\",\"parentId\":\"result-1\",\"customType\":\"om.observations.recorded\",\"data\":{\"observations\":[],\"coversUpToId\":\"result-1\"}}\n")
       "utf8")
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:synchronize! runtime) ctx)))
          (.then
           (fn [_]
             (is (= 1 @schemas))
             (is (= 1 (count @projections)))
             (is (= ["src/a.cljs"]
                    (mapv :relative-path (:files (first @projections)))))
             (is (= ["assistant-1" "result-1"]
                    (mapv :entry-id (:entry-file-evidence (first @projections)))))
             (is (= true (:connected? ((:status runtime)))))
             (is (= "github.com/AloiAI/adam"
                    (:file-evidence-repository ((:status runtime)))))
             (is (= "1 producer entry ignored"
                    (:memory-adapter-status ((:status runtime)))))
             ((:shutdown! runtime))))
          (.then
           (fn [_]
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))

(deftest file-evidence-failure-does-not-mark-session-replication-unhealthy
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-evidence-failure-"))
          path (join directory "session.jsonl")
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      (atom 0) (atom :fail))
          {:keys [pi]} (fake-pi)
          runtime (register/register!
                   pi
                   {:config {:enabled? true}
                    :create-replica (fn [] replica)
                    :load-user (fn [] {:user-uuid "user-1"})
                    :resolve-git-identity (fn [_] (js/Promise.resolve nil))
                    :resolve-repository
                    (fn [_cwd _user]
                      (js/Promise.resolve
                       {:id "repository" :user-id "user" :root "/repo"
                        :commit "abc" :dirty? false
                        :worktrees [{:root "/repo" :commit "abc" :dirty? false}]}))})
          ctx #js {:sessionManager #js {:getSessionFile (fn [] path)
                                        :getLeafId (fn [] nil)}
                   :ui #js {:notify (fn [_ _] nil)}}]
      (writeFileSync path
                     "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}\n"
                     "utf8")
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:synchronize! runtime) ctx)))
          (.then
           (fn [_]
             (let [status ((:status runtime))]
               (is (= true (:connected? status)))
               (is (= "evidence unavailable" (:file-evidence-error status))))
             ((:shutdown! runtime))))
          (.then
           (fn [_]
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))
