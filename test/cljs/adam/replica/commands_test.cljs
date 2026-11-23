(ns adam.replica.commands-test
  (:require [adam.replica.commands :as commands]
            [adam.replica.config :as config]
            [adam.replica.store :as store]
            [cljs.test :refer [async deftest is]]
            [clojure.string :as string]
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

(defn- configuration-case! [{:keys [inputs actions fail-save? published-save? fail-probe? cancel-probe? mode use-driver? fail-session? arguments cancel-field? hang-run? hang-session-close? hang-driver-close? cancel-after-run? probe-timeout-ms]
                             :or {inputs ["synthetic-password" "\r"] actions ["Save"] mode "tui"}}]
  (let [{:keys [pi registered]} (fake-pi)
        saved (atom []) probes (atom []) notifications (atom []) closed (atom 0)
        choices (atom actions) custom-count (atom 0) probe-component (atom nil)
        keys #js {:matches (fn [data action]
                             (= data (get {"tui.select.cancel" "\u001b"
                                           "tui.input.submit" "\r"
                                           "tui.editor.deleteCharBackward" "\u007f"
                                           "tui.editor.deleteToLineStart" "\u0015"} action)))}
        ctx #js {:hasUI true :mode mode
                 :ui #js {:notify (fn [message _level] (swap! notifications conj message))
                          :input (fn [_title _placeholder] (js/Promise.resolve (when-not cancel-field? "")))
                          :select (fn [_title _choices]
                                    (let [choice (first @choices)] (swap! choices rest)
                                         (js/Promise.resolve choice)))
                          :custom
                          (fn [factory]
                            (js/Promise.
                             (fn [resolve _reject]
                               (let [component (factory #js {:requestRender (fn [])} nil keys resolve)
                                     number (swap! custom-count inc)]
                                 (when (= number 1)
                                   (doseq [data inputs]
                                     ((aget component "handleInput") data)
                                     (doseq [width [0 8 80]]
                                       (let [lines ((aget component "render") width)]
                                         (is (not (string/includes? (str lines) "synthetic-password")))
                                         (is (every? #(<= (count %) width) (array-seq lines)))))))
                                 (when (> number 1) (reset! probe-component component))
                                 (when (and (> number 1) cancel-probe?)
                                   ((aget component "handleInput") "\u001b"))))))}}]
    (commands/register! pi {:configure-probe-timeout-ms (or probe-timeout-ms 15000)
                            :configure-read! (fn [] nil)
                            :configure-save! (fn [settings]
                                               (when fail-save? (throw (js/Error. "synthetic-password")))
                                               (swap! saved conj settings)
                                               (when published-save?
                                                 (throw (ex-info "synthetic-password" {::config/published? true}))))
                            :configure-driver!
                            (fn [_uri _auth options]
                              (is (= 5000 (aget options "connectionTimeout")))
                              (is (= 5000 (aget options "connectionAcquisitionTimeout")))
                              (is (= 0 (aget options "maxTransactionRetryTime")))
                              (swap! probes conj :driver)
                              #js {:session (fn [options]
                                              (is (= "READ" (aget options "defaultAccessMode")))
                                              (is (= "neo4j" (aget options "database")))
                                              (when fail-session? (throw (js/Error. "synthetic-password")))
                                              #js {:run (fn [query parameters options]
                                                          (is (= "RETURN 1 AS ok" query))
                                                          (is (= 0 (count (js/Object.keys parameters))))
                                                          (is (= 5000 (aget options "timeout")))
                                                          (when cancel-after-run?
                                                            (js/setTimeout #((aget @probe-component "handleInput") "\u001b") 0))
                                                          (cond
                                                            hang-run? (js/Promise. (fn [_ _]))
                                                            fail-probe? (js/Promise.reject (js/Error. "synthetic-password"))
                                                            :else (js/Promise.resolve #js {})))
                                                   :close (fn [] (if hang-session-close?
                                                                   (js/Promise. (fn [_ _]))
                                                                   (js/Promise.resolve nil)))})
                                   :close (fn [] (swap! closed inc)
                                            (if hang-driver-close?
                                              (js/Promise. (fn [_ _]))
                                              (js/Promise.resolve nil)))})
                            :configure-test! (when-not use-driver?
                                               (fn [settings signal]
                                                 (swap! probes conj settings)
                                                 (cond
                                                   fail-probe? (js/Promise.reject (js/Error. "synthetic-password"))
                                                   (.-aborted signal) (js/Promise.reject (js/Error. "cancelled"))
                                                   :else (js/Promise.resolve true))))})
    (let [timer (atom nil)
          work (js/Promise.resolve ((command-handler registered "adam:configure") (or arguments "") ctx))
          deadline (js/Promise. (fn [resolve _]
                                 (reset! timer (js/setTimeout #(resolve :timed-out) 750))))]
      (-> (js/Promise.race #js [work deadline])
        (.finally #(js/clearTimeout @timer))
        (.then (fn [result]
                 (is (not= :timed-out result) "configuration must settle independently of a pending query/cleanup")
                 (is (not (string/includes? (str @notifications) "synthetic-password")))
                 (when (and use-driver? (seq @probes)) (is (= 1 @closed)))
                 (when published-save?
                   (is (some #(string/includes? % "durability") @notifications))
                   (is (not-any? #(string/includes? % "no settings saved") @notifications)))
                 (when (or cancel-after-run? probe-timeout-ms)
                   (is (some #(string/includes? % "cancelled") @notifications)))
                 {:saved @saved :probes @probes :notifications @notifications}))))))

(deftest configure-command-requires-explicit-save-and-keeps-secrets-off-ui-output
  (async done
    (-> (reduce
         (fn [work [options saves probes password]]
           (.then work
                  (fn [_]
                    (-> (configuration-case! options)
                        (.then (fn [result]
                                 (is (= saves (count (:saved result))))
                                 (is (= probes (count (:probes result))))
                                 (when password (is (= password (:password (first (:saved result))))))
                                 (when (pos? saves)
                                   (is (some #(string/includes? % "Restart") (:notifications result))))))))))
         (js/Promise.resolve nil)
         [[{} 1 0 "synthetic-password"]
          [{:actions ["Test connection" "Save"]} 1 1 "synthetic-password"]
          [{:actions ["Cancel"]} 0 0 nil]
          [{:inputs ["synthetic-password" "\u001b"]} 0 0 nil]
          [{:actions ["Save"] :fail-save? true} 0 0 nil]
          [{:actions ["Save"] :published-save? true} 1 0 "synthetic-password"]
          [{:actions ["Test connection" "Save"] :fail-probe? true} 0 1 nil]
          [{:actions ["Test connection" "Save"] :cancel-probe? true} 0 0 nil]
          [{:mode "rpc"} 0 0 nil]
          [{:mode "print"} 0 0 nil]
          [{:mode "json"} 0 0 nil]
          [{:arguments "synthetic-password"} 0 0 nil]
          [{:cancel-field? true} 0 0 nil]
          [{:inputs ["\r"]} 0 0 nil]
          [{:inputs [(apply str (repeat 8193 "x"))]} 0 0 nil]
          [{:actions ["Test connection" "Save"] :use-driver? true} 1 1 "synthetic-password"]
          [{:actions ["Test connection"] :use-driver? true :fail-probe? true} 0 1 nil]
          [{:actions ["Test connection"] :use-driver? true :fail-session? true} 0 1 nil]
          [{:actions ["Test connection"] :use-driver? true :cancel-probe? true} 0 0 nil]
          [{:actions ["Test connection"] :use-driver? true :hang-run? true :hang-session-close? true :cancel-after-run? true} 0 1 nil]
          [{:actions ["Test connection"] :use-driver? true :hang-run? true :hang-session-close? true :hang-driver-close? true :cancel-after-run? true} 0 1 nil]
          [{:actions ["Test connection"] :use-driver? true :hang-run? true :hang-session-close? true :hang-driver-close? true :probe-timeout-ms 10} 0 1 nil]
          [{:actions ["Test connection" "Save"] :use-driver? true :hang-session-close? true :hang-driver-close? true :probe-timeout-ms 10} 0 1 nil]
          [{:inputs ["oops" "\u0015" "\u001b[200~synthetic-" "password\u001b[201~" "\r"]} 1 0 "synthetic-password"]
          [{:inputs ["synthetic-passwordX" "\u007f" "\r"]} 1 0 "synthetic-password"]
          [{:inputs ["\u001b[97u" "\u001b[98;2u" "\r"]} 1 0 "ab"]])
        (.then (fn [_] (done)))
        (.catch (fn [error] (is false (.-message error)) (done))))))

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
