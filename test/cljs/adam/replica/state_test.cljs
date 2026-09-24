(ns adam.replica.state-test
  (:require [adam.replica.state :as state]
            [cljs.test :refer [deftest is]]
            ["node:fs" :refer [mkdtempSync readFileSync rmSync statSync]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join]]))

(def test-uuid "0199a11d-0fe1-7000-8000-000000000001")

(deftest global-state-is-created-once-with-owner-only-permissions
  (let [agent-dir (mkdtempSync (join (tmpdir) "adam-state-test-"))]
    (try
      (let [created (state/load-or-create! agent-dir (constantly test-uuid))
            loaded (state/load-or-create! agent-dir
                                          (fn [] (throw (js/Error. "must not replace state"))))
            config-path (join agent-dir "adam" "config.json")
            parsed (js->clj (js/JSON.parse (readFileSync config-path "utf8"))
                            :keywordize-keys true)]
        (is (= created loaded))
        (is (= {:version 1
                :user-uuid test-uuid
                :path config-path}
               created))
        (is (= {:version 1 :userUuid test-uuid} parsed))
        (when-not (= "win32" js/process.platform)
          (is (= 384 (bit-and 511 (.-mode (statSync config-path)))))))
      (finally
        (rmSync agent-dir #js {:recursive true :force true})))))
