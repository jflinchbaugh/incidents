(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as feed]
            [clojure.java.io :as io]
            [xtdb.api :as xt]
            [java-time :as t]
            [taoensso.timbre :as log]))

(log/merge-config! {:ns-filter #{"incidents.*"}})

(defn start-xtdb! [dir]
  (letfn [(kv-store [d]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir d
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store (io/file dir "tx-log"))
      :xtdb/document-store (kv-store (io/file dir "doc-store"))
      :xtdb/index-store (kv-store (io/file dir "index-store"))})))

(defn tag [type rec]
  (assoc rec :type type))

(defn add-stage-id [stage]
  (assoc stage :xt/id (tag :stage {:uri (:uri stage)})))

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

(defn put-stage [node stage]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put (->> stage prune (tag :stage) add-stage-id)]])))

(defn get-all-stage [node]
  (->>
   (xt/q (xt/db node) '{:find [(pull e [*])]
                        :where [[e :type :stage]]})
   (mapv first)))

(defn get-all-stage-ids [node]
  (->>
   (xt/q
    (xt/db node)
    '{:find [e]
      :where [[e :type :stage]]})
   (map first)))

(defn clear-all-stage [node]
  (->>
   (get-all-stage-ids node)
   (mapv (fn [i] [::xt/evict i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn delete-stage [node ids]
  (->>
   ids
   (mapv (fn [i] [::xt/delete i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn load-stage [node]
  (let [new-stage (->>
                     "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx"
                     feed/parse-feed
                     :entries
                     (map add-stage-id))
        new-stage-ids (set (map :xt/id new-stage))
        existing-stage-ids (get-all-stage-ids node)
        ids-to-remove (remove new-stage-ids existing-stage-ids)
        removals (delete-stage node ids-to-remove)
        _ (log/info
            (str
              "Updated "
              (count new-stage)
              " and Removed "
              (count ids-to-remove)))]
    (->>
     new-stage
     (map (partial put-stage node))
     doall)))

(defn -main [& args]
  (let [[action & args] args]
    (with-open [xtdb-node (start-xtdb! "data")]
      (case action
        "load"
        (doall (map #(log/info %) (load-stage xtdb-node)))
        "list"
        (let [stage (get-all-stage xtdb-node)]
          (doall (map #(log/info %) (conj stage (str "Count: " (count stage))))))
        "clear"
        (doall (map #(log/info %) (clear-all-stage xtdb-node)))
        (log/info "list|load|clear")))))


(comment

  (-main "load")

  (-main "list")

  (-main "clear")

  (-main)

  .)
