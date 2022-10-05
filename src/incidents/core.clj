(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as feed]
            [clojure.java.io :as io]
            [xtdb.api :as xt]
            [clojure.pprint :as pp]
            [java-time.api :as t]
            [taoensso.timbre :as log]
            [clojure.string :as str]))

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

(defn put-stage! [node stage]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put stage]])))

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

(defn clear-all-stage! [node]
  (->>
   (get-all-stage-ids node)
   (mapv (fn [i] [::xt/evict i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn delete-stage! [node ids]
  (->>
   ids
   (mapv (fn [i] [::xt/delete i]))
   (xt/submit-tx node)
   (xt/await-tx node)))

(defn load-stage! [node source]
  (let [new-stage (->>
                   source
                   feed/parse-feed
                   :entries
                   (map prune)
                   (map (partial tag :stage))
                   (map add-stage-id)
                   (sort-by :uri))
        new-stage-ids (set (map :xt/id new-stage))
        existing-stage (sort-by :uri (get-all-stage node))
        existing-stage-ids (map :xt/id existing-stage)
        ids-to-remove (remove new-stage-ids existing-stage-ids)
        removals (delete-stage! node ids-to-remove)
        updates (remove (set existing-stage) new-stage)
        _ (log/info
           (str
            "Found "
            (count new-stage)
            ", Updated "
            (count updates)
            ", Removed "
            (count ids-to-remove)))]
    (->>
     updates
     (map (partial put-stage! node))
     doall)))

(defn- title-case [s]
  (str/join " " (map str/capitalize (str/split s #" "))))

(defn- parse-units [s]
  (if (nil? s) '()
      (map (comp title-case str/trim) (str/split s #"<br>"))))

(defn- parse-streets [s]
  (if (nil? s) '()
      (map (comp title-case str/trim) (str/split s #"[&/]"))))

(defn parse [in]
  (let [[municipality streets units] (str/split
                                      (get-in in [:description :value])
                                      #"; *")]
    {:uri (:uri in)
     :start-date (:published-date in)
     :title (title-case (:title in))
     :municipality (title-case (str/trim municipality))
     :streets (parse-streets streets)
     :units (parse-units units)}))

(defn add-fact-id [fact]
  (assoc fact :xt/id (tag :fact {:uri (:uri fact)})))

(defn put-fact! [node fact]
  (xt/await-tx
   node
   (xt/submit-tx
    node
    [[::xt/put fact]])))

(defn get-all-active-facts [node]
  (->>
   (xt/q (xt/db node) '{:find [(pull ?e [*])]
                        :where [[?e :type :fact]
                                (not [?e :end-date])]})
   (mapv first)))

(defn get-all-facts [node]
  (->>
   (xt/q (xt/db node) '{:find [(pull ?e [*])]
                        :where [[?e :type :fact]]})
   (mapv first)))

(defn end [date fact]
  (assoc fact
         :end-date date
         :duration-minutes (t/as (t/duration (:start-date fact) date) :minutes)))

(defn transform-facts! [node]
  (let [active-facts (get-all-active-facts node)
        new-facts (->>
                   node
                   get-all-stage
                   (map parse)
                   (map (partial tag :fact))
                   (map add-fact-id))
        updated-facts (remove (set active-facts) new-facts)
        ended-facts (remove (set new-facts) active-facts)]

    (doall
      (concat
        (->>
          ended-facts
          (map (partial end (t/java-date)))
          (map (partial put-fact! node)))
        (->>
          updated-facts
          (map (partial put-fact! node)))))))

(defn -main [& args]
  (let [[action & args] args]
    (with-open [xtdb-node (start-xtdb! "data")]
      (case action
        "load"
        (doall
         (concat
           (load-stage!
             xtdb-node
             "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")
          (transform-facts! xtdb-node)))
        "list"
        (let [stage (get-all-stage xtdb-node)
              facts (get-all-active-facts xtdb-node)]
          (doall (map #(log/info %) stage))
          (log/info "Count of Stage:" (count stage))
          (doall (map #(log/info %) facts))
          (log/info "Count of Facts:" (count facts)))
        "list-all"
        (let [stage (get-all-stage xtdb-node)
              facts (get-all-facts xtdb-node)]
          (doall (map #(log/info %) stage))
          (log/info "Count of Stage:" (count stage))
          (doall (map #(log/info %) facts))
          (log/info "Count of Facts:" (count facts)))
        "clear"
        (doall (map #(log/info %) (clear-all-stage! xtdb-node)))
        (log/info "list|list-all|load|clear")))))

(comment

  (with-open [xtdb-node (start-xtdb! "data")]
    (transform-facts! xtdb-node))

  (with-open [xtdb-node (start-xtdb! "data")]
    (get-all-active-facts xtdb-node))

  (with-open [xtdb-node (start-xtdb! "data")]
    (get-all-facts xtdb-node))

  (with-open [xtdb-node (start-xtdb! "data")]
    (load-stage!
      xtdb-node
      "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx"))

  (-main "load")

  (-main "list")

  (-main "list-all")

  (-main "clear")

  (-main)

  (with-open [node (start-xtdb! "data")]
    (->>
      (xt/q (xt/db node) '{:find [?description]
                           :where [[?e :type :fact]
                                   [?e :description ?description]]})
      (mapv first)
      (take 10)))

  (with-open [node (start-xtdb! "data")]
    (->>
      (xt/attribute-stats node)
      ))

  .)
