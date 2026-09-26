(ns adam.knowledge.runtime
  (:require [adam.knowledge.repository :as repository]
            [adam.replica.neo4j :as neo4j]
            [adam.replica.state :as global-state]
            [adam.replica.store :as replica-store]
            ["node:child_process" :refer [execFile]]))

(defn- exit-code [error]
  (let [code (when error (.-code error))]
    (if (number? code) code (if error 1 0))))

(defn node-exec!
  [command arguments options]
  (js/Promise.
   (fn [resolve _reject]
     (execFile
      command
      arguments
      #js {:cwd (aget options "cwd")
           :timeout (or (aget options "timeout") 2000)
           :maxBuffer (* 1024 1024)
           :encoding "utf8"}
      (fn [error stdout stderr]
        (resolve #js {:code (exit-code error)
                      :stdout (or stdout "")
                      :stderr (or stderr "")}))))))

(defn create-query-runtime
  ([config]
   (create-query-runtime config {}))
  ([config options]
   (let [replica-promise (atom nil)
         user-promise (atom nil)
         create-replica (or (:create-replica options)
                            #(neo4j/create-replica config))
         load-user (or (:load-user options)
                       global-state/load-or-create-host!)
         exec! (or (:exec options) node-exec!)
         get-store!
         (fn []
           (or @replica-promise
               (let [promise (-> (js/Promise.resolve nil)
                                 (.then (fn [_] (create-replica))))]
                 (reset! replica-promise promise)
                 promise)))
         get-user!
         (fn []
           (or @user-promise
               (let [promise (-> (js/Promise.resolve nil)
                                 (.then (fn [_] (load-user))))]
                 (reset! user-promise promise)
                 promise)))
         dependencies
         {:get-store! get-store!
          :get-user! get-user!
          :resolve-repository!
          (fn [cwd user-uuid]
            (repository/resolve-git-repository! exec! cwd user-uuid))}
         close!
         (fn []
           (if-let [promise @replica-promise]
             (-> promise
                 (.then (fn [replica] (replica-store/close! replica)))
                 (.catch (fn [_] nil)))
             (js/Promise.resolve nil)))]
     {:dependencies dependencies
      :close! close!})))
