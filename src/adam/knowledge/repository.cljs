(ns adam.knowledge.repository
  (:require [adam.knowledge.evidence :as evidence]
            [clojure.string :as string]
            ["node:path" :as node-path]))

(def ^:private git-timeout-ms 2000)

(defn- git-value [exec cwd arguments]
  (-> (exec "git" (clj->js arguments) #js {:cwd cwd :timeout git-timeout-ms})
      (.then
       (fn [result]
         (when (zero? (aget result "code"))
           (let [value (string/trim (or (aget result "stdout") ""))]
             (when-not (string/blank? value) value)))))
      (.catch (fn [_] nil))))

(defn parse-worktrees [output]
  (if (string/blank? output)
    []
    (let [results (atom [])
          current (atom {})
          flush! (fn []
                   (when (and (:root @current) (:commit @current))
                     (swap! results conj @current))
                   (reset! current {}))]
      (doseq [field (string/split output "\u0000")]
        (cond
          (string/blank? field) (flush!)
          (string/starts-with? field "worktree ")
          (do (flush!) (swap! current assoc :root (subs field 9)))
          (string/starts-with? field "HEAD ")
          (swap! current assoc :commit (subs field 5))
          (string/starts-with? field "branch refs/heads/")
          (swap! current assoc :branch (subs field 18))))
      (flush!)
      @results)))

(defn- worktree-dirty! [exec root]
  (-> (exec "git" #js ["status" "--porcelain"] #js {:cwd root :timeout git-timeout-ms})
      (.then
       (fn [result]
         (and (zero? (aget result "code"))
              (not (string/blank? (or (aget result "stdout") ""))))))
      (.catch (fn [_] false))))

(defn resolve-git-repository! [exec cwd user-uuid]
  (-> (git-value exec cwd ["rev-parse" "--show-toplevel"])
      (.then
       (fn [root]
         (when root
           (-> (js/Promise.all
                #js [(git-value exec root ["config" "--get" "remote.origin.url"])
                     (git-value exec root ["rev-parse" "HEAD"])
                     (git-value exec root ["branch" "--show-current"])
                     (git-value exec root ["worktree" "list" "--porcelain" "-z"])])
               (.then
                (fn [resolved]
                  (let [remote (aget resolved 0)
                        commit (aget resolved 1)
                        branch (aget resolved 2)
                        listed (parse-worktrees (aget resolved 3))]
                    (when commit
                      (let [listed (if (some #(= root (:root %)) listed)
                                     listed
                                     (into [{:root root :commit commit :branch branch}] listed))]
                        (-> (js/Promise.all
                             (clj->js
                              (mapv (fn [worktree]
                                      (-> (worktree-dirty! exec (:root worktree))
                                          (.then #(assoc worktree :dirty? %))))
                                    listed)))
                            (.then
                             (fn [worktree-array]
                               (let [worktrees (vec (array-seq worktree-array))
                                     primary (or (first (filter #(= root (:root %)) worktrees))
                                                 {:root root :commit commit :branch branch :dirty? false})]
                                 (evidence/build-repository
                                  {:user-uuid user-uuid
                                   :root root
                                   :remote remote
                                   :commit commit
                                   :branch branch
                                   :dirty? (:dirty? primary)
                                   :worktrees worktrees}))))))))))))))))

(defn resolve-from-file-path! [resolve-repository cwd path]
  (let [absolute (.resolve node-path (if (.isAbsolute node-path path)
                                      path
                                      (.resolve node-path cwd path)))]
    (letfn [(walk [candidate]
              (let [parent (.dirname node-path candidate)]
                (-> (resolve-repository candidate)
                    (.then
                     (fn [repository]
                       (cond
                         repository repository
                         (= parent candidate) nil
                         :else (walk parent)))))))]
      (walk (.dirname node-path absolute)))))
