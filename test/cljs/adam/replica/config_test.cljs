(ns adam.replica.config-test
  (:require [adam.replica.config :as config]
            [cljs.test :refer [deftest is testing]]
            [clojure.test.check :as check]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop :include-macros true]
            ["yaml" :as yaml]
            ["node:fs" :as fs]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(def settings {:version 1 :uri "bolt://localhost:7687" :username "neo4j"
               :password "synthetic-password" :database "neo4j"})

(deftest persistent-connection-selection-and-private-persistence
  (let [root (.mkdtempSync fs (join (tmpdir) "adam-config-"))]
    (try
      (is (false? (:enabled? (config/resolve-process-config {} root))))
      (config/save-connection! settings root)
      (is (= (dissoc settings :version)
             (dissoc (config/resolve-process-config {} root) :enabled?)))
      (is (= 384 (bit-and 511 (.-mode (.statSync fs (join root "adam" "connection.yaml"))))))
      (is (= 448 (bit-and 511 (.-mode (.statSync fs (join root "adam"))))))
      (doseq [key config/environment-keys]
        (testing (str "any explicit environment key suppresses file fallback: " key)
          (is (false? (:enabled? (config/resolve-process-config {key ""} root))))))
      (is (= "environment-password"
             (:password (config/resolve-process-config
                         {"ADAM_NEO4J_URI" "bolt://localhost:9999"
                          "ADAM_NEO4J_USERNAME" "other"
                          "ADAM_NEO4J_PASSWORD" "environment-password"} root))))
      (doseq [value [(assoc settings :unknown "secret") (assoc settings :version 2)
                     (assoc settings :password "") (assoc settings :uri "bolt://user:secret@localhost")]]
        (is (thrown? js/Error (config/save-connection! value root))))
      (is (= "synthetic-password" (:password (config/read-connection! root))))
      (doseq [password ["synthetic-password" "  synthetic with spaces  " "synthetic-🔒"
                         "true" "null" "0123" "0x10" "yes" "2026-11-18" "a: #b"]]
        (config/save-connection! (assoc settings :password password) root)
        (is (= password (:password (config/read-connection! root)))))
      (config/save-connection! settings root)
      (.linkSync fs (join root "adam" "connection.yaml") (join root "hardlink"))
      (is (false? (:enabled? (config/resolve-process-config {} root))))
      (.unlinkSync fs (join root "hardlink"))
      (.chmodSync fs (join root "adam") 493)
      (is (false? (:enabled? (config/resolve-process-config {} root))))
      (.chmodSync fs (join root "adam") 448)
      (doseq [source ["password: [synthetic-password" "[]" "null"
                      "version: 1\nuri: bolt://localhost:7687\nusername: neo4j\npassword: true\n"
                      "version: 1\nuri: bolt://localhost:7687\nusername: neo4j\npassword: synthetic-password\npassword: duplicate\n"
                      "version: 1\nuri: bolt://localhost:7687\nusername: &user synthetic-password\npassword: *user\n"
                      "version: 1\nuri: bolt://localhost:7687\nusername: neo4j\npassword: !custom synthetic-password\n"
                      (js/JSON.stringify #js {:version 1 :uri "synthetic-password"})
                      (.repeat "synthetic-password" 4096)
                      (js/Uint8Array. #js [255 254])]]
        (.writeFileSync fs (join root "adam" "connection.yaml") source)
        (let [result (config/resolve-process-config {} root)]
          (is (false? (:enabled? result)))
          (is (not (.includes (:reason result) "synthetic-password"))))
        (is (thrown? js/Error (config/save-connection! settings root))))
      (.writeFileSync fs (join root "adam" "connection.yaml") (.stringify yaml (clj->js settings)))
      (.chmodSync fs (join root "adam" "connection.yaml") 420)
      (is (false? (:enabled? (config/resolve-process-config {} root))))
      (.chmodSync fs (join root "adam" "connection.yaml") 384)
      (.renameSync fs (join root "adam" "connection.yaml") (join root "target"))
      (.symlinkSync fs (join root "target") (join root "adam" "connection.yaml"))
      (is (false? (:enabled? (config/resolve-process-config {} root))))
      (is (thrown? js/Error (config/save-connection! settings root)))
      (finally (.rmSync fs root #js {:recursive true :force true})))))

(defn- observed-save! [root value failure]
  (let [open! (.-openSync fs) close! (.-closeSync fs)
        sync! (.-fsyncSync fs) rename! (.-renameSync fs)
        descriptors (atom {}) events (atom []) error (atom nil)
        directory (join root "adam")]
    (with-redefs [fs/openSync (fn [& args]
                               (let [fd (apply open! args)]
                                 (swap! descriptors assoc fd (first args))
                                 fd))
                  fs/closeSync (fn [fd]
                                (close! fd)
                                (swap! descriptors dissoc fd))
                  fs/fsyncSync (fn [fd]
                                (let [directory? (.isDirectory (.fstatSync fs fd))
                                      target (if directory? (get @descriptors fd) :file)]
                                  (swap! events conj [:sync target])
                                  (when (or (and (= failure :file) (not directory?))
                                            (and (= failure :directory) (= target directory))
                                            (and (= failure :ancestor) directory? (not= target directory)))
                                    (throw (js/Error. "synthetic-password fsync failed")))
                                  (sync! fd)))
                  fs/renameSync (fn [source destination]
                                 (swap! events conj :rename)
                                 (when (= failure :rename) (throw (js/Error. "synthetic-password rename failed")))
                                 (rename! source destination))]
      (try (config/save-connection! value root)
           (catch :default e (reset! error e))))
    (is (empty? @descriptors) "all file and directory descriptors must close, including on failure")
    (when @error (is (not (.includes (.-message @error) "synthetic-password"))))
    {:events @events :error @error}))

(deftest successful-save-syncs-the-renamed-entry-and-new-directory-ancestors
  (doseq [nested? [false true]]
    (let [sandbox (.mkdtempSync fs (join (tmpdir) "adam-save-sync-"))
          root (if nested? (join sandbox "nested" "xdg") sandbox)
          directory (join root "adam")]
      (try
        (let [result (observed-save! root settings nil)
              parents (if nested? [root (join sandbox "nested") sandbox] [root])]
          (is (nil? (:error result)))
          (is (= (into [[:sync :file] :rename [:sync directory]] (map #(vector :sync %) parents))
                 (:events result))))
        (let [result (observed-save! root (assoc settings :password "synthetic-updated") nil)]
          (is (nil? (:error result)))
          (is (= [[:sync :file] :rename [:sync directory]] (:events result))))
        (finally (.rmSync fs sandbox #js {:recursive true :force true}))))))

(deftest save-sync-failures-distinguish-pre-and-post-publication
  (doseq [failure [:file :rename :directory :ancestor]]
    (let [root (.mkdtempSync fs (join (tmpdir) "adam-save-failure-"))
          published? (contains? #{:directory :ancestor} failure)
          updated (assoc settings :password "synthetic-updated")]
      (try
        ;; Ancestor sync only occurs when a directory entry was newly created.
        (when-not (= failure :ancestor) (config/save-connection! settings root))
        (let [result (observed-save! root updated failure)
              error (:error result)]
          (is (some? error) (str failure))
          (is (= published? (true? (::config/published? (ex-data error)))) (str failure))
          (is (= (if published? updated settings) (config/read-connection! root)))
          (is (= ["connection.yaml"] (vec (.readdirSync fs (join root "adam"))))))
        (finally (.rmSync fs root #js {:recursive true :force true}))))))

(deftest private-yaml-roundtrips-preserve-passwords-and-defaults
  (let [root (.mkdtempSync fs (join (tmpdir) "adam-config-property-"))]
    (try
      (let [result (check/quick-check
                    80
                    (prop/for-all [parts (gen/vector (gen/elements ["a" "Z" "0" "\"" "\\" " " "🔒"]) 0 40)
                                   database? gen/boolean]
                      (let [value (cond-> (assoc settings :password (apply str "synthetic-" parts))
                                    (not database?) (dissoc :database))]
                        (config/save-connection! value root)
                        (= (config/normalize-connection value) (config/read-connection! root))))
                    :seed 54037)]
        (is (:pass? result) (pr-str result)))
      (finally (.rmSync fs root #js {:recursive true :force true})))))

(deftest incomplete-configuration-disables-the-replica
  (testing "missing credentials do not prevent adam from loading"
    (is (= {:enabled? false
            :reason "ADAM_NEO4J_URI, ADAM_NEO4J_USERNAME, and ADAM_NEO4J_PASSWORD are required"}
           (config/resolve-config
            {"ADAM_NEO4J_URI" "bolt://localhost:7687"
             "ADAM_NEO4J_USERNAME" "neo4j"})))))

(deftest complete-configuration-is-normalized
  (is (= {:enabled? true
          :uri "bolt://localhost:7687"
          :username "neo4j"
          :password "secret"
          :database "neo4j"}
         (config/resolve-config
          {"ADAM_NEO4J_URI" "  bolt://localhost:7687  "
           "ADAM_NEO4J_USERNAME" " neo4j "
           "ADAM_NEO4J_PASSWORD" "secret"}))))

(deftest non-loopback-unencrypted-transport-is-rejected
  (is (= {:enabled? false
          :reason "unencrypted Neo4j transport is allowed only for loopback hosts"}
         (config/resolve-config
          {"ADAM_NEO4J_URI" "bolt://neo4j.example.com:7687"
           "ADAM_NEO4J_USERNAME" "neo4j"
           "ADAM_NEO4J_PASSWORD" "secret"}))))

(deftest routed-connections-require-tls-for-file-and-environment-settings
  (doseq [[uri accepted?] [["neo4j://localhost:7687" false]
                           ["neo4j://127.0.0.1:7687" false]
                           ["neo4j://[::1]:7687" false]
                           ["neo4j+s://example.com:7687" true]
                           ["neo4j+ssc://localhost:7687" true]
                           ["bolt://127.0.0.1:7687" true]
                           ["bolt://127.attacker.example:7687" false]]]
    (let [value (assoc settings :uri uri)
          environment {"ADAM_NEO4J_URI" uri "ADAM_NEO4J_USERNAME" "neo4j"
                       "ADAM_NEO4J_PASSWORD" "synthetic-password"}]
      (is (= accepted? (:enabled? (config/resolve-config environment))) uri)
      (if accepted?
        (is (= uri (:uri (config/normalize-connection value))))
        (is (thrown? js/Error (config/normalize-connection value)) uri)))))

(deftest invalid-uri-disables-the-replica-without-throwing
  (is (= {:enabled? false
          :reason "ADAM_NEO4J_URI must be a valid bolt or neo4j URI"}
         (config/resolve-config
          {"ADAM_NEO4J_URI" "not a URI"
           "ADAM_NEO4J_USERNAME" "neo4j"
           "ADAM_NEO4J_PASSWORD" "secret"}))))
