(ns adam.replica.register-test
  (:require [adam.knowledge.store :as knowledge-store]
            [adam.replica.register :as register]
            [adam.memory.inbox :as memory-inbox]
            [adam.replica.store :as store]
            [adam.sources.claude-code.worker :as notification-worker]
            [cljs.test :refer [async deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defrecord LifecycleReplica [checkpoint initializations writes completions closes
                             identity-version identity-migrations
                             evidence-schemas projections migration-version rebuild-completions
                             memory-identity-version memory-identity-migrations]
  store/SessionIdentityMigrationStore
  (session-identity-version! [_ _user-id]
    (js/Promise.resolve @identity-version))
  (migrate-pi-session-identities! [_ user-id target-version]
    (swap! identity-migrations conj [user-id target-version])
    (reset! identity-version target-version)
    (js/Promise.resolve nil))

  knowledge-store/MemoryIdentityMigrationStore
  (memory-identity-version! [_ _user-id]
    (js/Promise.resolve @memory-identity-version))
  (migrate-memory-identities! [_ user-id target-version]
    (swap! memory-identity-migrations conj [user-id target-version])
    (reset! memory-identity-version target-version)
    (js/Promise.resolve nil))

  knowledge-store/MemoryReconciliationStore
  (ensure-memory-source! [_ _user-id _session-id]
    (js/Promise.resolve nil))
  (reproject-retained-memory! [_ _user-uuid _source-kind _source-session-id]
    (js/Promise.resolve {:observations 0 :reflections 0 :unresolved-citations 0}))

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

  knowledge-store/CodeMemoryMigrationStore
  (code-memory-version! [_ _user-id]
    (js/Promise.resolve @migration-version))
  (complete-code-memory-rebuild! [_ user-id version]
    (reset! migration-version version)
    (swap! rebuild-completions conj [user-id version])
    (js/Promise.resolve nil))

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
                                      (atom 0) (atom [])
                                      (atom 0) (atom []) (atom 3) (atom [])
                                      (atom 1) (atom []))
          attempts (atom 0)
          {:keys [pi]} (fake-pi)
          notifications (atom [])
          runtime (register/register!
                   pi
                   {:config {:enabled? true}
                    :worker-inbox-options {:config-home directory}
                    :memory-inbox-options {:state-home directory :data-home directory}
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
          identity-version (atom 0)
          identity-migrations (atom [])
          memory-identity-version (atom 0)
          memory-identity-migrations (atom [])
          replica (->LifecycleReplica checkpoint initializations writes completions closes
                                      identity-version identity-migrations
                                      (atom 0) (atom []) (atom 3) (atom [])
                                      memory-identity-version memory-identity-migrations)
          {:keys [pi commands tools events]} (fake-pi)
          notifications (atom [])
          clock (atom -10)
          runtime (register/register!
                   pi
                   {:config {:enabled? true}
                    :worker-inbox-options {:config-home directory}
                    :memory-inbox-options {:state-home directory :data-home directory}
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
           (fn [result]
             (is (identical? js/undefined result)
                 "side-effect lifecycle handlers must not expose internal maps as Pi boundary results")
             (let [drafts #js [#js {:type "custom" :customType "producer.memory" :data #js {}}]
                   select-entries (js/Function. "result" "entries"
                                    "return result?.entries === undefined ? entries : result.entries;")
                   selected (select-entries result drafts)]
               (is (identical? drafts selected)
                   "Pi's boundary chaining must preserve preceding producer drafts")
               (is (= 1 ((js/Function. "drafts" "return [...drafts].length;") selected))))
             (is (= 2 (count @initializations)))
             (is (= [["urn:adam:user:user-1" 2]] @identity-migrations))
             (is (= 2 @identity-version))
             (is (= [["urn:adam:user:user-1" 1]] @memory-identity-migrations))
             (is (= 1 @memory-identity-version))
             (is (= "migrated"
                    (:memory-identity-migration-status ((:status runtime)))))
             (is (= "urn:adam:session:user-1:pi:session-1"
                    (get-in (first @writes) [:session :id])))
             (is (= "urn:adam:identity:git-email:hash"
                    (get-in (second @initializations) [:identity :id])))
             (is (= 1 (count @writes)))
             (is (= 1 (count @completions)))
             (is (= "entry-1"
                    (get-in (first @writes) [:session :current-leaf-id])))
             (is (= :mirrored (get-in ((:status runtime)) [:last-result :status])))
             (is (= "migrated"
                    (:session-identity-migration-status ((:status runtime)))))
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
             (is (re-find #"Memory identity schema: v1"
                          (first (last @notifications))))
             (is (re-find #"Memory identity migration: migrated"
                          (first (last @notifications))))
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

(deftest lifecycle-callbacks-await-work-and-return-no-host-result
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-return-"))
          path (join directory "session.jsonl")
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      (atom 2) (atom []) (atom 0) (atom []) (atom 3) (atom [])
                                      (atom 1) (atom []))
          release (atom nil)
          started (atom nil)
          replica-ready (js/Promise. (fn [resolve _] (reset! release resolve)))
          creation-started (js/Promise. (fn [resolve _] (reset! started resolve)))
          settled? (atom false)
          fail? (atom false)
          {:keys [pi events]} (fake-pi)
          runtime (register/register!
                    pi {:config {:enabled? true}
                        :worker-inbox-options {:config-home directory}
                        :memory-inbox-options {:state-home directory :data-home directory}
                        :create-replica (fn [] (@started nil) replica-ready)
                        :load-user (fn [] {:user-uuid "user-1"})
                        :resolve-repository (fn [_ _] (js/Promise.resolve nil))
                        :resolve-git-identity (fn [_]
                                                (if @fail?
                                                  (throw (js/Error. "identity unavailable"))
                                                  (js/Promise.resolve nil)))})
          ctx #js {:sessionManager #js {:getSessionFile (fn [] path)
                                        :getLeafId (fn [] "entry-1")}
                   :ui #js {:notify (fn [_ _] nil)}}]
      (writeFileSync path
        (str "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}\n"
             "{\"type\":\"message\",\"id\":\"entry-1\",\"parentId\":null}\n"))
      (let [handler (get @events "turn_end")
            work (-> (.call handler nil #js {} ctx)
                     (.then (fn [result] (reset! settled? true) result)))]
        (-> (js/Promise.race
              #js [creation-started
                   (.then work (fn [_]
                                 (throw (js/Error. (str "callback settled before replica creation: "
                                                       (:last-error ((:status runtime))))))))])
            (.then (fn [_]
                     (is (false? @settled?) "callback must await serialized replica work")
                     (@release replica)
                     work))
            (.then (fn [result]
                     (is (identical? js/undefined result))
                     (is (true? (:connected? ((:status runtime)))))
                     (reduce
                       (fn [pending event]
                         (-> pending
                             (.then (fn [_] ((get @events event) #js {} ctx)))
                             (.then (fn [result]
                                      (is (identical? js/undefined result) event)))))
                       (js/Promise.resolve nil)
                       ["session_start" "turn_end" "session_compact" "session_tree"
                        "session_info_changed" "model_select" "thinking_level_select"])))
            (.then (fn [_]
                     (reset! fail? true)
                     ((get @events "turn_end") #js {} ctx)))
            (.then (fn [result]
                     (is (identical? js/undefined result) "isolated source failure is not a boundary result")
                     (is (= "identity unavailable" (:last-error ((:status runtime)))))
                     ((:shutdown! runtime))))
            (.catch (fn [error] (is false (.-stack error))))
            (.finally (fn [] (rmSync directory #js {:recursive true :force true}) (done))))))))

(deftest lifecycle-indexes-file-evidence-after-the-lossless-mirror
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-evidence-"))
          path (join directory "session.jsonl")
          projections (atom [])
          schemas (atom 0)
          migration-version (atom 2)
          rebuild-completions (atom [])
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      (atom 2) (atom [])
                                      schemas projections migration-version rebuild-completions
                                      (atom 1) (atom []))
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
                    :worker-inbox-options {:config-home directory}
                    :memory-inbox-options {:state-home directory :data-home directory}
                    :list-code-memory-session-files (fn [] [path])
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
             (is (= 2 (count @projections)))
             (is (= ["src/a.cljs"]
                    (mapv :relative-path (:files (first @projections)))))
             (is (= ["assistant-1" "result-1"]
                    (mapv :entry-id (:entry-file-evidence (first @projections)))))
             (is (= 3 @migration-version))
             (is (= [["urn:adam:user:user-1" 3]] @rebuild-completions))
             (is (= "rebuilt" (:code-memory-rebuild-status ((:status runtime)))))
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

(deftest busy-notification-lease-does-not-skip-pi-mirroring-evidence-or-import
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-busy-"))
          path (join directory "session.jsonl")
          imported-path (join directory "imported.jsonl")
          checkpoint (atom nil)
          writes (atom [])
          projections (atom [])
          replica (->LifecycleReplica checkpoint (atom []) writes (atom []) (atom 0)
                                      (atom 2) (atom []) (atom 0) projections (atom 3) (atom [])
                                      (atom 1) (atom []))
          {:keys [pi]} (fake-pi)
          lease-options {:config-home directory}
          runtime (register/register!
                    pi {:config {:enabled? true}
                        :worker-inbox-options lease-options
                        :memory-inbox-options {:state-home directory :data-home directory}
                        :create-replica (fn [] replica)
                        :load-user (fn [] {:user-uuid "user-1"})
                        :resolve-git-identity (fn [_] (js/Promise.resolve nil))
                        :resolve-repository (fn [_ _]
                                              (js/Promise.resolve
                                                {:id "repository" :root "/repo"
                                                 :worktrees [{:root "/repo" :commit "abc" :dirty? false}]}))})
          ctx #js {:sessionManager #js {:getSessionFile (fn [] path)
                                        :getLeafId (fn [] "tool")}
                   :ui #js {:notify (fn [_ _] nil)}}]
      (writeFileSync path
        (str "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}\n"
             "{\"type\":\"message\",\"id\":\"tool\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"id\":\"call\",\"name\":\"read\",\"arguments\":{\"path\":\"src/a.cljs\"}}]}}\n"))
      (writeFileSync imported-path
        (str "{\"type\":\"session\",\"id\":\"session-2\",\"cwd\":\"/repo\"}\n"
             "{\"type\":\"message\",\"id\":\"imported-entry\",\"parentId\":null}\n"))
      (-> (notification-worker/with-lease! lease-options
            #(-> ((:synchronize! runtime) ctx :turn-end)
                 (.then (fn [_]
                          (is (= 1 (count @writes)) "Pi session mirrors even while the drain is busy")
                          (is (= ["src/a.cljs"] (mapv :relative-path (:files (first @projections)))))
                          (is (true? (:connected? ((:status runtime)))))
                          (is (re-find #"Worker busy" (:memory-notifications-error ((:status runtime)))))
                          (is (nil? (:last-error ((:status runtime)))))
                          (reset! checkpoint nil)
                          ((:import-file! runtime) imported-path ctx)))
                 (.then (fn [result]
                          (is (= :mirrored (:status result)) "historical imports are not lease-gated")
                          (is (= 2 (count @writes)))))))
          (.then (fn [_] ((:shutdown! runtime))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally (fn [] (rmSync directory #js {:recursive true :force true}) (done)))))))

(deftest explicit-pi-repair-recovers-parked-work-under-the-notification-lease
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-parked-"))
          inbox-options {:state-home directory :data-home directory}
          lease-options {:config-home directory}
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      (atom 2) (atom []) (atom 0) (atom []) (atom 3) (atom [])
                                      (atom 1) (atom []))
          created (atom 0)
          {:keys [pi commands]} (fake-pi)
          runtime (register/register!
                    pi {:config {:enabled? true} :worker-inbox-options lease-options
                        :memory-inbox-options inbox-options
                        :create-replica (fn [] (swap! created inc) replica)
                        :load-user (fn [] {:user-uuid "user-1"})})
          ctx #js {:sessionManager #js {:getSessionFile (fn [] nil)}
                   :ui #js {:notify (fn [_ _] nil)}}
          repair! #((aget (get @commands "adam:reconcile") "handler") "" ctx)]
      (memory-inbox/enqueue! inbox-options {:source-kind "pi" :source-session-id "remote-only" :producer-id "producer-a"})
      (let [notification (first (memory-inbox/pending inbox-options))]
        (memory-inbox/park! inbox-options notification)
        (memory-inbox/acknowledge! inbox-options notification))
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:synchronize! runtime) ctx :turn-end)))
          (.then (fn [_]
                   (is (zero? @created) "parked-only work must not initialize ordinary ephemeral turns")
                   (notification-worker/with-lease! lease-options
                     #(-> (repair!)
                          (.then (fn [_]
                                   (is (= 1 (count (memory-inbox/parked inbox-options))))
                                   (is (empty? (memory-inbox/pending inbox-options)))
                                   (is (re-find #"Worker busy" (:memory-notifications-error ((:status runtime)))))))))))
          (.then (fn [_] (repair!)))
          (.then (fn [_]
                   (is (empty? (memory-inbox/parked inbox-options)))
                   (is (= 1 (get-in ((:status runtime)) [:memory-notifications :recovered])))
                   (is (= 0 (get-in ((:status runtime)) [:memory-notifications :parked-pending])))
                   (is (nil? (:memory-notifications-error ((:status runtime)))))
                   ((:shutdown! runtime))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally (fn [] (rmSync directory #js {:recursive true :force true}) (done)))))))

(deftest connected-replication-failure-is-not-mislabeled-as-drain-deferral
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-sync-error-"))
          path (join directory "session.jsonl")
          fail? (atom false)
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      (atom 2) (atom []) (atom 0) (atom []) (atom 3) (atom [])
                                      (atom 1) (atom []))
          {:keys [pi]} (fake-pi)
          runtime (register/register!
                    pi {:config {:enabled? true}
                        :worker-inbox-options {:config-home directory}
                        :memory-inbox-options {:state-home directory :data-home directory}
                        :create-replica (fn [] replica)
                        :load-user (fn [] {:user-uuid "user-1"})
                        :resolve-repository (fn [_ _] (js/Promise.resolve nil))
                        :resolve-git-identity (fn [_]
                                                (if @fail?
                                                  (throw (js/Error. "identity resolution failed"))
                                                  (js/Promise.resolve nil)))})
          ctx #js {:sessionManager #js {:getSessionFile (fn [] path) :getLeafId (fn [] nil)}
                   :ui #js {:notify (fn [_ _] nil)}}]
      (writeFileSync path "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}\n")
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((:synchronize! runtime) ctx)))
          (.then (fn [_]
                   (is (true? (:connected? ((:status runtime)))))
                   (reset! fail? true)
                   ((:synchronize! runtime) ctx)))
          (.then (fn [_]
                   (is (= "identity resolution failed" (:last-error ((:status runtime)))))
                   (is (false? (:connected? ((:status runtime)))))
                   (is (nil? (:memory-notifications-error ((:status runtime)))))
                   ((:shutdown! runtime))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally (fn [] (rmSync directory #js {:recursive true :force true}) (done)))))))

(deftest file-evidence-failure-does-not-mark-session-replication-unhealthy
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-register-evidence-failure-"))
          path (join directory "session.jsonl")
          replica (->LifecycleReplica (atom nil) (atom []) (atom []) (atom []) (atom 0)
                                      (atom 2) (atom [])
                                      (atom 0) (atom :fail) (atom 3) (atom [])
                                      (atom 1) (atom []))
          {:keys [pi]} (fake-pi)
          runtime (register/register!
                   pi
                   {:config {:enabled? true}
                    :create-replica (fn [] replica)
                    :load-user (fn [] {:user-uuid "user-1"})
                    :resolve-git-identity (fn [_] (js/Promise.resolve nil))
                    :worker-inbox-options {:config-home directory}
                    :memory-inbox-options {:state-home directory :data-home directory}
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
