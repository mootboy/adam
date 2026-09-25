(ns adam.replica.restore-test
  (:require [adam.replica.restore :as restore]
            [adam.replica.store :as store]
            [cljs.test :refer [async deftest is]]
            ["node:crypto" :refer [createHash]]
            ["node:fs" :refer [mkdtempSync readFileSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defrecord RestoreReplica [restored]
  store/SessionReplicaStore
  (initialize! [_ _] (js/Promise.resolve nil))
  (get-checkpoint! [_ _] (js/Promise.resolve nil))
  (write-batch! [_ _] (js/Promise.resolve nil))
  (complete-session! [_ _] (js/Promise.resolve nil))
  (mark-conflict! [_ _] (js/Promise.resolve nil))
  (list-sessions! [_ _] (js/Promise.resolve []))
  (read-session! [_ _] (js/Promise.resolve restored))
  (close! [_] (js/Promise.resolve nil)))

(defn- sha-256 [value]
  (-> (createHash "sha256") (.update value "utf8") (.digest "hex")))

(defn- restored-session []
  (let [header "{\"type\":\"session\",\"version\":3,\"id\":\"session-1\",\"cwd\":\"/repo\"}"
        raw-entry "{\"type\":\"message\",\"id\":\"root\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"image\",\"data\":\"aGVsbG8=\"}]}}"
        contents (str header "\n" raw-entry "\n")]
    {:contents contents
     :restored
     {:session {:id "urn:adam:session:user-1:session-1"
                :pi-session-id "session-1"
                :cwd "/repo"
                :source-file "/old/session-1.jsonl"
                :current-leaf-id "root"
                :conflicted? false
                :complete? true}
      :header-json header
      :entry-count 1
      :log-hash (sha-256 contents)
      :log-bytes (.byteLength js/Buffer contents "utf8")
      :has-final-newline? true
      :entries [{:entry-id "root"
                 :parent-id nil
                 :ordinal 0
                 :raw-json raw-entry
                 :payload-hash (sha-256 raw-entry)
                 :payload-bytes (.byteLength js/Buffer raw-entry "utf8")} ]}}))

(deftest restores-validated-jsonl-without-rewriting-payloads
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-restore-test-"))
          target (join directory "session-1.jsonl")
          {:keys [contents restored]} (restored-session)
          replica (->RestoreReplica restored)]
      (-> (restore/materialize-session! replica "session-id" target)
          (.then
           (fn [result]
             (is (= target (:path result)))
             (is (= "root" (:current-leaf-id result)))
             (is (= contents (readFileSync target "utf8")))
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (rmSync directory #js {:recursive true :force true})
             (is false (.-stack error))
             (done)))))))

(deftest refuses-to-overwrite-an-existing-local-session
  (async done
    (let [directory (mkdtempSync (join (tmpdir) "adam-restore-test-"))
          target (join directory "session-1.jsonl")
          {:keys [restored]} (restored-session)
          replica (->RestoreReplica restored)]
      (writeFileSync target "local authority\n" "utf8")
      (-> (restore/materialize-session! replica "session-id" target)
          (.then
           (fn [_]
             (is false "expected overwrite refusal")
             (rmSync directory #js {:recursive true :force true})
             (done)))
          (.catch
           (fn [error]
             (is (re-find #"refusing to overwrite" (.-message error)))
             (is (= "local authority\n" (readFileSync target "utf8")))
             (rmSync directory #js {:recursive true :force true})
             (done)))))))
