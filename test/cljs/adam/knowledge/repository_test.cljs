(ns adam.knowledge.repository-test
  (:require [adam.knowledge.repository :as repository]
            [cljs.test :refer [async deftest is]]
            [clojure.string :as string]))

(defn- result [stdout code]
  #js {:stdout stdout :code code})

(deftest resolves-repository-and-all-linked-worktree-revisions
  (async done
    (let [calls (atom [])
          exec
          (fn [_command arguments options]
            (let [args (vec (array-seq arguments))
                  cwd (aget options "cwd")]
              (swap! calls conj [args cwd])
              (js/Promise.resolve
               (case (string/join " " args)
                 "rev-parse --show-toplevel" (result "/work/repo\n" 0)
                 "config --get remote.origin.url" (result "https://token@example.com/AloiAI/adam.git\n" 0)
                 "rev-parse HEAD" (result "main-head\n" 0)
                 "branch --show-current" (result "main\n" 0)
                 "worktree list --porcelain -z"
                 (result (str "worktree /work/repo\u0000HEAD main-head\u0000branch refs/heads/main\u0000\u0000"
                              "worktree /work/trees/feature\u0000HEAD feature-head\u0000branch refs/heads/feature/a\u0000\u0000") 0)
                 "status --porcelain"
                 (result (if (= cwd "/work/trees/feature") " M private.cljs\n" "") 0)
                 (result "" 128)))))]
      (-> (repository/resolve-git-repository! exec "/work/repo/src" "user-1")
          (.then
           (fn [resolved]
             (is (= "example.com/AloiAI/adam" (:normalized-remote resolved)))
             (is (not (re-find #"token|private.cljs" (pr-str resolved))))
             (is (= [{:root "/work/repo" :commit "main-head" :branch "main" :dirty? false}
                     {:root "/work/trees/feature" :commit "feature-head"
                      :branch "feature/a" :dirty? true}]
                    (:worktrees resolved)))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest discovers-a-repository-by-walking-up-from-explicit-file-evidence
  (async done
    (let [candidates (atom [])
          resolver (fn [candidate]
                     (swap! candidates conj candidate)
                     (js/Promise.resolve
                      (when (= candidate "/workspace/repo") {:id "repo"})))]
      (-> (repository/resolve-from-file-path!
           resolver "/workspace" "repo/src/new.cljs")
          (.then
           (fn [resolved]
             (is (= {:id "repo"} resolved))
             (is (= ["/workspace/repo/src" "/workspace/repo"] @candidates))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))
