(ns adam.replica.jsonl-test
  (:require [adam.replica.jsonl :as jsonl]
            [cljs.test :refer [deftest is]]
            ["node:crypto" :refer [createHash]]
            ["node:fs" :refer [mkdtempSync rmSync writeFileSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(defn- sha-256 [value]
  (-> (createHash "sha256") (.update value) (.digest "hex")))

(defn- with-session-file [contents f]
  (let [directory (mkdtempSync (join (tmpdir) "adam-jsonl-test-"))
        path (join directory "session.jsonl")]
    (try
      (writeFileSync path contents "utf8")
      (f path)
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest scans-a-branched-session-without-changing-payloads
  (let [directory (mkdtempSync (join (tmpdir) "adam-jsonl-test-"))
        path (join directory "session.jsonl")
        header "{\"type\":\"session\",\"version\":3,\"id\":\"session-1\",\"timestamp\":\"2026-01-01T00:00:00.000Z\",\"cwd\":\"/repo\"}"
        root "{\"type\":\"message\",\"id\":\"root\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"image\",\"data\":\"aGVsbG8=\"}]}}"
        child "{\"type\":\"future-entry\",\"id\":\"child\",\"parentId\":\"root\",\"unknown\":true}"
        sibling "{\"type\":\"message\",\"id\":\"sibling\",\"parentId\":\"root\",\"message\":{\"role\":\"assistant\",\"content\":[]}}"
        contents (str header "\n" root "\n" child "\n" sibling "\n")
        entries (atom [])]
    (try
      (writeFileSync path contents "utf8")
      (let [summary (jsonl/scan-file path {:on-entry #(swap! entries conj %)})]
        (is (= {:path path
                :pi-session-id "session-1"
                :header-json header
                :cwd "/repo"
                :version 3
                :created-at "2026-01-01T00:00:00.000Z"
                :entry-count 3
                :last-entry-id "sibling"
                :log-hash (sha-256 contents)
                :log-bytes (.-length (js/Buffer.from contents))
                :largest-entry-bytes (.-length (js/Buffer.from root))
                :has-final-newline? true}
               (select-keys summary [:path :pi-session-id :header-json :cwd :version
                                     :created-at :entry-count :last-entry-id :log-hash
                                     :log-bytes :largest-entry-bytes :has-final-newline?])))
        (is (= [root child sibling] (mapv :raw-json @entries)))
        (is (= [0 1 2] (mapv :ordinal @entries)))
        (is (= [nil "root" "root"] (mapv :parent-id @entries)))
        (is (= ["user" nil "assistant"] (mapv :role @entries)))
        (is (= (sha-256 (str header "\n" root "\n"))
               (:prefix-hash (first @entries)))))
      (finally
        (rmSync directory #js {:recursive true :force true})))))

(deftest rejects-malformed-lines-and-dangling-parents
  (with-session-file
    "{\"type\":\"session\",\"id\":\"session-1\"}\n{\"type\":"
    (fn [path]
      (is (thrown-with-msg? js/Error
                            (re-pattern (str path ":2: invalid JSON"))
                            (jsonl/scan-file path)))))
  (with-session-file
    (str "{\"type\":\"session\",\"id\":\"session-1\"}\n"
         "{\"type\":\"future-entry\",\"id\":\"entry-1\",\"parentId\":\"missing\"}")
    (fn [path]
      (is (thrown-with-msg? js/Error
                            #"unresolved parentId missing"
                            (jsonl/scan-file path))))))

(deftest requires-an-explicit-parent-id
  (with-session-file
    (str "{\"type\":\"session\",\"id\":\"session-1\"}\n"
         "{\"type\":\"future-entry\",\"id\":\"entry-1\"}")
    (fn [path]
      (is (thrown-with-msg? js/Error
                            #"missing or invalid parentId"
                            (jsonl/scan-file path))))))

(deftest streams-lines-larger-than-the-read-buffer-and-skips-empty-lines
  (let [large-data (apply str (repeat 70000 "a"))
        header "{\"type\":\"session\",\"id\":\"session-1\"}"
        entry (js/JSON.stringify #js {:type "message"
                                     :id "entry-1"
                                     :parentId nil
                                     :message #js {:role "user"
                                                   :content large-data}})
        contents (str "\n" header "\n\n" entry)]
    (with-session-file
      contents
      (fn [path]
        (let [entries (atom [])
              summary (jsonl/scan-file path {:on-entry #(swap! entries conj %)})]
          (is (= 1 (:entry-count summary)))
          (is (= entry (:raw-json (first @entries))))
          (is (= false (:has-final-newline? summary)))
          (is (= (sha-256 contents) (:log-hash summary))))))))

(deftest reads-only-the-bounded-session-header
  (let [header "{\"type\":\"session\",\"id\":\"parent-1\",\"cwd\":\"/repo\",\"parentSession\":\"/older.jsonl\"}"]
    (with-session-file
      (str header "\n" (apply str (repeat 70000 "x")))
      (fn [path]
        (is (= {:pi-session-id "parent-1"
                :header-json header
                :cwd "/repo"
                :parent-session "/older.jsonl"}
               (jsonl/read-session-header path)))))))
