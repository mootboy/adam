(ns adam.sources.claude-code.reconcile-test
  (:require [adam.knowledge.evidence :as common-evidence]
            [adam.sources.claude-code.reconcile :as reconcile]
            [cljs.test :refer-macros [async deftest is]]
            ["node:fs" :refer [mkdtempSync rmSync]]
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
