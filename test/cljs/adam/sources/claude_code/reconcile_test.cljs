(ns adam.sources.claude-code.reconcile-test
  (:require [adam.knowledge.evidence :as common-evidence]
            [adam.sources.claude-code.reconcile :as reconcile]
            [adam.sources.claude-code.scanner :as scanner]
            [cljs.test :refer-macros [async deftest is]]
            ["node:fs" :refer [copyFileSync existsSync mkdtempSync rmSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :as node-path :refer [join]]))

(def user-uuid "00000000-0000-4000-8000-000000000001")
(def main-fixture (.resolve node-path "test/fixtures/claude/main.jsonl"))
(def subagent-fixture (.resolve node-path "test/fixtures/claude/subagent.jsonl"))

(def repository
  (common-evidence/build-repository
   {:user-uuid user-uuid
    :root "/work/repo"
    :remote "git@github.com:mootboy/adam.git"
    :commit "main-head"
    :branch "main"
    :dirty? false
    :worktrees [{:root "/work/repo" :commit "main-head"
                 :branch "main" :dirty? false}
                {:root "/work/tree" :commit "feature-head"
                 :branch "feature/claude" :dirty? true}]}))

(deftest missing-transcript-is-acknowledged-without-touching-the-graph
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-reconcile-test-"))
          missing (join root "gone.jsonl")]
      (-> (reconcile/reconcile!
           {:locator-options {:config-home root}
            :notification {:event "SessionStart"
                           :session-id "claude-session-1"
                           :transcript-path missing
                           :cwd "/workspace"}
            :user-uuid user-uuid
            :store :store
            :sync! (fn [_] (js/Promise.reject (js/Error. "sync must not run")))
            :resolve-repository! (fn [_ _] (js/Promise.reject (js/Error. "resolve must not run")))})
          (.then
           (fn [result]
             (is (= {:status :missing-transcript :transcript-path missing} result))
             (is (not (existsSync (join root "adam" "claude-sessions"))))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest repository-less-reconciliation-clears-stale-derived-evidence
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-reconcile-test-"))
          cleared (atom nil)]
      (-> (reconcile/reconcile!
           {:locator-options {:config-home root}
            :notification {:event "Stop"
                           :session-id "claude-session-1"
                           :transcript-path main-fixture
                           :cwd "/workspace"}
            :user-uuid user-uuid
            :store :store
            :sync! (fn [_] (js/Promise.resolve {:status :unchanged}))
            :resolve-repository! (fn [_ _] (js/Promise.resolve nil))
            :clear-file-evidence! (fn [_ session-id version]
                                    (reset! cleared [session-id version])
                                    (js/Promise.resolve nil))})
          (.then
           (fn [result]
             (is (= :cleared (:projection-status result)))
             (is (= ["urn:adam:session:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1"
                     common-evidence/extractor-version]
                    @cleared))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest removed-subagent-transcript-projects-evidence-from-its-mirrored-entries
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-reconcile-test-"))
          subagent-copy (join root "subagent.jsonl")
          mirrored (:entries (scanner/scan-stream {:session-id "claude-session-1"
                                                   :transcript-path subagent-fixture
                                                   :stream-id "agent:agent-1"
                                                   :agent-id "agent-1"}))
          projections (atom [])
          read-requests (atom [])
          reconcile-with-file!
          (fn [notification]
            (reconcile/reconcile!
             {:locator-options {:config-home root}
              :notification notification
              :user-uuid user-uuid
              :store :store
              :sync! (fn [request]
                       ;; The removed stream must not be offered to sync.
                       (is (= ["main"] (remove #{"agent:agent-1"}
                                               (map :stream-id (get-in request [:scan :streams])))))
                       (js/Promise.resolve {:status :mirrored}))
              :read-stream-entries! (fn [store stream-id]
                                      (swap! read-requests conj [store stream-id])
                                      (js/Promise.resolve mirrored))
              :resolve-repository! (fn [_ _] (js/Promise.resolve repository))
              :ensure-file-schema! (fn [_] (js/Promise.resolve nil))
              :index-file-evidence! (fn [_ value]
                                      (swap! projections conj value)
                                      (js/Promise.resolve nil))}))]
      (copyFileSync subagent-fixture subagent-copy)
      (-> (reconcile-with-file! {:event "SubagentStop"
                                 :session-id "claude-session-1"
                                 :transcript-path subagent-copy
                                 :parent-transcript-path main-fixture
                                 :agent-id "agent-1"
                                 :cwd "/work/repo"})
          (.then
           (fn [_]
             (rmSync subagent-copy)
             (reconcile-with-file! {:event "Stop"
                                    :session-id "claude-session-1"
                                    :transcript-path main-fixture
                                    :cwd "/work/repo"})))
          (.then
           (fn [result]
             (let [[with-file without-file] @projections]
               (is (= :projected (:projection-status result)))
               (is (= [[:store "urn:adam:stream:00000000-0000-4000-8000-000000000001:claude-code:claude-session-1:agent:agent-1"]]
                      @read-requests))
               (is (= 8 (count (:entry-file-evidence with-file))))
               (is (= (:entry-file-evidence with-file) (:entry-file-evidence without-file)))
               (is (= (:files with-file) (:files without-file))))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))

(deftest reconciliation-mirrors-all-known-streams-before-projecting-file-evidence
  (async done
    (let [root (mkdtempSync (join (tmpdir) "adam-reconcile-test-"))
          events (atom [])
          projection (atom nil)]
      (-> (reconcile/reconcile!
           {:locator-options {:config-home root}
            :notification {:event "SubagentStop"
                           :session-id "claude-session-1"
                           :transcript-path subagent-fixture
                           :parent-transcript-path main-fixture
                           :agent-id "agent-1"
                           :cwd "/workspace"}
            :user-uuid user-uuid
            :store :store
            :sync! (fn [request]
                     (swap! events conj [:sync (count (get-in request [:scan :streams]))])
                     (js/Promise.resolve {:status :mirrored}))
            :resolve-repository! (fn [cwd _user-uuid]
                                   (js/Promise.resolve
                                    (when (.startsWith cwd "/work/repo") repository)))
            :ensure-file-schema! (fn [_] (js/Promise.resolve nil))
            :index-file-evidence! (fn [_ value]
                                    (reset! projection value)
                                    (swap! events conj [:index (count (:entry-file-evidence value))])
                                    (js/Promise.resolve nil))})
          (.then
           (fn [result]
             (is (= [[:sync 2] [:index 8]] @events))
             (is (= :mirrored (:status result)))
             (is (= "claude-code" (:source-kind @projection)))
             (is (empty? (:observations @projection)))
             (is (empty? (:reflections @projection)))))
          (.catch #(is false (str %)))
          (.finally
           (fn []
             (rmSync root #js {:recursive true :force true})
             (done)))))))
