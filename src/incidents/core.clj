(ns incidents.core
  (:require [feedparser-clj.core :as feed]
            [clojure.java.io :as io]
            [xtdb.api :as xt]
            [java-time :as t]))

(defn start-xtdb! [dir]
  (letfn [(kv-store [d]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir d
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store (io/file dir "tx-log"))
      :xtdb/document-store (kv-store (io/file dir "doc-store"))
      :xtdb/index-store (kv-store (io/file dir "index-store"))})))

(defn stop-xtdb! [node]
  (.close node))

(defn add-entry-id [entry]
  (assoc entry :xt/id (:uri entry)))

(defn tag [type rec]
  (assoc rec :type type))

(defn prune [rec]
  (into {}
        (remove
         (fn [[k v]]
           (or
            (nil? v)
            (and
             (or
              (string? v)
              (coll? v))
             (empty? v)))) rec)))

(defn put-entry [node entry]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put (->> entry prune (tag :entry) add-entry-id)]])))

(defn get-all-entries [node]
  (->>
   (xt/q (xt/db node) '{:find [(pull e [*])]
                        :where [[e :type :entry]]})
   (mapv first)))

(defn get-all-entry-ids [node]
  (->>
   (xt/q
    (xt/db node)
    '{:find [e]
      :where [[e :type :entry]]})
   (map first)))

(defn clear-all-entries [node]
  (->>
   (get-all-entry-ids node)
   (mapv (fn [i] [::xt/evict i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn delete-entries [node ids]
  (->>
   ids
   (mapv (fn [i] [::xt/delete i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn load-entries [node]
  (let [new-entries (->>
                     "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx"
                     feed/parse-feed
                     :entries)
        new-entry-ids (set (map :xt/id new-entries))
        existing-entry-ids (get-all-entry-ids node)
        removals (delete-entries
                  node
                  (remove new-entry-ids existing-entry-ids))]
    (->>
     new-entries
     (map (partial put-entry node))
     doall)))

(defn -main [& args]
  (let [[action & args] args]
    (with-open [xtdb-node (start-xtdb! "data")]
      (case action
        "load"
        (load-entries xtdb-node)
        "list"
        (let [entries (get-all-entries xtdb-node)]
          (map prn (conj entries (str "Count: " (count entries)))))
        "history"
        (let [entries (get-all-entries xtdb-node)]
          (map prn (conj entries (str "Count: " (count entries)))))
        "clear"
        (map prn (clear-all-entries xtdb-node))
        (prn "list|load|clear")))))

(comment

  (def xtdb-node (start-xtdb! "data"))

  (stop-xtdb! xtdb-node)

  (clear-all-entries xtdb-node)

  (get-all-entries xtdb-node)

  (-main "load")

  (-main "list")

  (-main "clear")

  (-main)

  (with-open [node (start-xtdb! "data")]
    (doall (get-all-entries node)))

  .)
