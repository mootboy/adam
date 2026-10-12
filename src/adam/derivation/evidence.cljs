(ns adam.derivation.evidence
  (:require [adam.derivation.config :as config]
            [adam.derivation.selector :as selector]))

(defn authorized? [reader-id grants memory]
  (let [owner (:owner-id memory)]
    (and (config/text? owner 2048)
         (or (= reader-id owner)
             (some #(and (= owner (:owner-id %)) (= reader-id (:reader-id %))
                         (= "read" (:permission %))) grants)))))

(defn- validate-memory! [memory]
  (when-not (and (config/text? (:id memory) 2048)
                 (config/text? (:owner-id memory) 2048)
                 (config/text? (:session-id memory) 2048)
                 (config/text? (:content memory) js/Number.MAX_SAFE_INTEGER)
                 (contains? #{:observation :reflection} (:kind memory))
                 (config/text? (:producer memory) 256))
    (config/reject! :invalid-memory "incomplete memory snapshot"))
  memory)

(defn- support-ids [memory]
  (if (= :reflection (:kind memory))
    (let [ids (:supporting-memory-ids memory)]
      (when-not (and (vector? ids) (seq ids) (every? #(config/text? % 2048) ids)
                     (= (count ids) (count (set ids))))
        (config/reject! :unresolved-evidence "reflection support is incomplete"))
      ids)
    []))

(defn select-input [{:keys [reader-id memories grants filter comparison]
                    :or {filter {:type :none} grants []}}]
  (when-not (config/text? reader-id 2048)
    (config/reject! :unauthorized "requesting identity is required"))
  (let [by-id (into {} (map (juxt :id identity) memories))
        roles (atom {})
        diagnostics (atom [])
        add! (fn [id role]
               (let [memory (get by-id id)]
                 (when-not memory
                   (config/reject! :unresolved-evidence "memory dependency is missing"))
                 (when-not (authorized? reader-id grants memory)
                   (config/reject! :unauthorized "memory dependency is not authorized"))
                 (validate-memory! memory)
                 (swap! roles update id (fnil conj #{}) role)
                 memory))]
    (when-not (= (count by-id) (count memories))
      (config/reject! :invalid-memory "memory identities must be unique"))
    (let [candidates (->> memories
                          (filterv #(and (authorized? reader-id grants %)
                                         (not (and (= :observation (:kind %)) (:dropped? %)))
                                         (selector/matches-filter? filter %)))
                          (sort-by :id) vec)]
      (doseq [memory candidates]
        (add! (:id memory) :evidence)
        (doseq [id (support-ids memory)]
          (let [support (add! id :historical-support)]
            (when-not (= :observation (:kind support))
              (config/reject! :unresolved-evidence "reflections must cite observations")))))
      (let [comparison
            (when comparison
              (let [ids (:input-memory-ids comparison)
                    complete? (and (config/text? (:id comparison) 2048)
                                   (vector? (:conclusions comparison))
                                   (vector? ids) (= (count ids) (count (set ids)))
                                   (or (empty? (:conclusions comparison)) (seq ids))
                                   (every? #(and (get by-id %) (authorized? reader-id grants (get by-id %))) ids))]
                (if complete?
                  (do (doseq [id ids] (add! id :comparison-dependency)) comparison)
                  (do (swap! diagnostics conj :comparison-unavailable) nil))))]
        {:reader-id reader-id :filter filter
         :memories (mapv #(assoc (get by-id %) :roles (get @roles %)) (sort (keys @roles)))
         :candidate-ids (mapv :id candidates)
         :comparison comparison :diagnostics @diagnostics}))))

(defn support-occasions [snapshot ids]
  (let [by-id (into {} (map (juxt :id identity) (:memories snapshot)))
        source-ids (mapcat
                    (fn [id]
                      (let [memory (get by-id id)]
                        (when-not (and memory (contains? (set (:candidate-ids snapshot)) id))
                          (config/reject! :invalid-output "support must cite selected source evidence"))
                        (if (= :reflection (:kind memory)) (support-ids memory) [id])))
                    ids)]
    (set (mapcat
           (fn [id]
             (let [memory (get by-id id)]
               (keep (fn [{:keys [session-id stream-id entry-id]}]
                       (when (and (config/text? session-id 2048)
                                  (config/text? stream-id 512) (config/text? entry-id 512))
                         (when-let [[_ source-kind source-session-id]
                                    (re-matches #"urn:adam:session:[^:\s]+:([^:\s]+):([^\s]+)" session-id)]
                           [source-kind source-session-id stream-id entry-id])))
                     (:source-occasions memory))))
           source-ids))))
