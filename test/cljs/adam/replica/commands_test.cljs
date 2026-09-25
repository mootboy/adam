(ns adam.replica.commands-test
  (:require [adam.replica.commands :as commands]
            [adam.replica.store :as store]
            [cljs.test :refer [async deftest is]]
            ["node:crypto" :refer [createHash]]
            ["node:fs" :refer [mkdtempSync readFileSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defrecord CommandReplica [sessions restored]
  store/SessionReplicaStore
  (initialize! [_ _] (js/Promise.resolve nil))
  (get-checkpoint! [_ _] (js/Promise.resolve nil))
  (write-batch! [_ _] (js/Promise.resolve nil))
  (complete-session! [_ _] (js/Promise.resolve nil))
  (mark-conflict! [_ _] (js/Promise.resolve nil))
  (list-sessions! [_ _] (js/Promise.resolve sessions))
  (read-session! [_ _] (js/Promise.resolve restored))
  (close! [_] (js/Promise.resolve nil)))

(defn- fake-pi []
  (let [registered (atom {})]
    {:pi #js {:registerCommand (fn [name definition]
                                (swap! registered assoc name definition))}
     :registered registered}))

(defn- command-handler [registered name]
  (aget (get @registered name) "handler"))

(defn- base-context [notifications]
  #js {:cwd "/repo"
       :hasUI true
       :waitForIdle (fn [] (js/Promise.resolve nil))
       :sessionManager #js {:getSessionDir (fn [] "/sessions")}
       :ui #js {:notify (fn [message level]
                          (swap! notifications conj [message level]))
                :setStatus (fn [_key _value] nil)}})

(deftest default-current-cwd-discovery-excludes-other-session-cwds
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-command-discovery-"))
          included (join directory "included.jsonl")
          excluded (join directory "excluded.jsonl")
          {:keys [pi registered]} (fake-pi)
          notifications (atom [])
          imports (atom [])
          ctx (base-context notifications)]
      (writeFileSync included
                     "{\"type\":\"session\",\"id\":\"included\",\"cwd\":\"/repo\"}\n"
                     "utf8")
      (writeFileSync excluded
                     "{\"type\":\"session\",\"id\":\"excluded\",\"cwd\":\"/other\"}\n"
                     "utf8")
      (aset (aget ctx "sessionManager") "getSessionDir" (fn [] directory))
      (aset (aget ctx "ui") "confirm" (fn [_title _message] (js/Promise.resolve true)))
      (commands/register!
       pi
       {:config {:enabled? true}
        :import-file! (fn [path _ctx]
                        (swap! imports conj path)
                        (js/Promise.resolve {:status :mirrored}))})
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((command-handler registered "adam:import") "" ctx)))
          (.then
           (fn [_]
             (is (= [included] @imports))
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))

(deftest import-requires-confirmation-before-reading-session-content
  (async done
    (let [{:keys [pi registered]} (fake-pi)
          notifications (atom [])
          imports (atom [])
          ctx (base-context notifications)]
      (aset (aget ctx "ui") "confirm" (fn [_title _message] (js/Promise.resolve false)))
      (commands/register!
       pi
       {:config {:enabled? true}
        :list-sessions! (fn [_all? _cwd _session-dir]
                          (js/Promise.resolve [{:path "/sessions/one.jsonl"}]))
        :import-file! (fn [path _ctx]
                        (swap! imports conj path)
                        (js/Promise.resolve {:status :mirrored}))})
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((command-handler registered "adam:import") "" ctx)))
          (.then
           (fn [_]
             (is (empty? @imports))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest import-continues-past-malformed-sessions-and-reports-totals
  (async done
    (let [{:keys [pi registered]} (fake-pi)
          notifications (atom [])
          ctx (base-context notifications)
          malformed (doto (js/Error. "bad log") (aset "name" "SessionJsonlError"))]
      (aset (aget ctx "ui") "confirm" (fn [_title _message] (js/Promise.resolve true)))
      (commands/register!
       pi
       {:config {:enabled? true}
        :list-sessions! (fn [_all? _cwd _session-dir]
                          (js/Promise.resolve [{:path "/sessions/good.jsonl"}
                                               {:path "/sessions/bad.jsonl"}]))
        :import-file! (fn [path _ctx]
                        (if (.endsWith path "bad.jsonl")
                          (js/Promise.reject malformed)
                          (js/Promise.resolve {:status :mirrored})))})
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((command-handler registered "adam:import") "--all" ctx)))
          (.then
           (fn [_]
             (is (= ["adam import complete: 1 mirrored, 0 unchanged, 0 conflicted, 1 malformed, 0 failed"
                     "warning"]
                    (last @notifications)))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest import-clears-progress-status-with-undefined
  (async done
    (let [{:keys [pi registered]} (fake-pi)
          notifications (atom [])
          statuses (atom [])
          ctx (base-context notifications)]
      (aset (aget ctx "ui") "confirm" (fn [_title _message] (js/Promise.resolve true)))
      (aset (aget ctx "ui") "setStatus"
            (fn [key text]
              (swap! statuses conj [key text])))
      (commands/register!
       pi
       {:config {:enabled? true}
        :list-sessions! (fn [_all? _cwd _session-dir]
                          (js/Promise.resolve [{:path "/sessions/one.jsonl"}]))
        :import-file! (fn [_path _ctx]
                        (js/Promise.resolve {:status :mirrored}))})
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((command-handler registered "adam:import") "--all" ctx)))
          (.then
           (fn [_]
             (is (= "Importing 1/1" (second (first @statuses))))
             (is (undefined? (second (last @statuses))))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest resume-prefers-a-conflicted-authoritative-local-session
  (async done
    (let [{:keys [pi registered]} (fake-pi)
          notifications (atom [])
          switched (atom [])
          remote {:id "urn:adam:session:user-1:session-1"
                  :pi-session-id "session-1"
                  :cwd "/repo"
                  :name "Remote"
                  :conflicted? true
                  :complete? false}
          replica (->CommandReplica [remote] nil)
          ctx (base-context notifications)]
      (aset (aget ctx "ui") "select"
            (fn [_title choices] (js/Promise.resolve (aget choices 0))))
      (aset ctx "switchSession"
            (fn [path]
              (swap! switched conj path)
              (js/Promise.resolve #js {:cancelled false})))
      (commands/register!
       pi
       {:config {:enabled? true}
        :list-sessions! (fn [_all? _cwd _session-dir]
                          (js/Promise.resolve [{:id "session-1"
                                                :path "/sessions/local.jsonl"
                                                :name "Local"}]))
        :get-replica! (fn [] (js/Promise.resolve replica))
        :get-user! (fn [] (js/Promise.resolve {:user-uuid "user-1"}))})
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((command-handler registered "adam:resume") "" ctx)))
          (.then
           (fn [_]
             (is (= ["/sessions/local.jsonl"] @switched))
             (is (= ["adam conflict detected; resuming authoritative local JSONL" "warning"]
                    (last @notifications)))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(defn- remote-restoration []
  (let [header "{\"type\":\"session\",\"id\":\"session-1\",\"cwd\":\"/repo\"}"
        entry "{\"type\":\"message\",\"id\":\"root\",\"parentId\":null}"
        contents (str header "\n" entry "\n")
        digest (fn [value]
                 (-> (createHash "sha256") (.update value "utf8") (.digest "hex")))
        summary {:id "urn:adam:session:user-1:session-1"
                 :pi-session-id "session-1"
                 :cwd "/repo"
                 :source-file "/old/session-1.jsonl"
                 :current-leaf-id "root"
                 :conflicted? false
                 :complete? true}]
    {:summary summary
     :contents contents
     :restored {:session summary
                :header-json header
                :entry-count 1
                :log-hash (digest contents)
                :log-bytes (.byteLength js/Buffer contents "utf8")
                :has-final-newline? true
                :entries [{:entry-id "root"
                           :parent-id nil
                           :ordinal 0
                           :raw-json entry
                           :payload-hash (digest entry)
                           :payload-bytes (.byteLength js/Buffer entry "utf8")} ]}}))

(deftest resume-materializes-a-remote-session-and-restores-the-leaf
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-command-resume-"))
          {:keys [pi registered]} (fake-pi)
          notifications (atom [])
          navigated (atom [])
          switched (atom [])
          {:keys [summary restored contents]} (remote-restoration)
          replica (->CommandReplica [summary] restored)
          ctx (base-context notifications)
          replacement #js {:navigateTree
                           (fn [leaf options]
                             (swap! navigated conj [leaf (aget options "summarize")])
                             (js/Promise.resolve #js {:cancelled false}))
                           :ui #js {:notify (fn [message level]
                                             (swap! notifications conj [message level]))}}]
      (aset (aget ctx "sessionManager") "getSessionDir" (fn [] directory))
      (aset (aget ctx "ui") "select"
            (fn [_title choices] (js/Promise.resolve (aget choices 0))))
      (aset ctx "switchSession"
            (fn [path options]
              (swap! switched conj path)
              (-> ((aget options "withSession") replacement)
                  (.then (fn [_] #js {:cancelled false})))))
      (commands/register!
       pi
       {:config {:enabled? true}
        :list-sessions! (fn [_all? _cwd _session-dir] (js/Promise.resolve []))
        :get-replica! (fn [] (js/Promise.resolve replica))
        :get-user! (fn [] (js/Promise.resolve {:user-uuid "user-1"}))})
      (-> (js/Promise.resolve nil)
          (.then (fn [_] ((command-handler registered "adam:resume") "" ctx)))
          (.then
           (fn [_]
             (let [target (join directory "session-1.jsonl")]
               (is (= contents (readFileSync target "utf8")))
               (is (= [target] @switched))
               (is (= [["root" false]] @navigated)))
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))
