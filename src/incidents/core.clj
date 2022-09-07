(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as feed]
            [clojure.java.io :as io]
            [xtdb.api :as xt]
            [clojure.pprint :as pp]
            [java-time :as t]
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

(defn load-stage! [node]
  (let [new-stage (->>
                   "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx"
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
            (count ids-to-remove)))
        _ (pp/pprint
            {:new (first new-stage)
             :existing (first existing-stage)
             :comp (= (first new-stage) (first existing-stage))})]
    (->>
     updates
     (map (partial put-stage! node))
     doall)))

(defn- title-case [s]
  (str/join " " (map str/capitalize (str/split s #" "))))

(defn- parse-units [s]
  (if (nil? s) []
      (map (comp title-case str/trim) (str/split s #"<br>"))))

(defn- parse-streets [s]
  (if (nil? s) []
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
  (assoc fact :end-date date))

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

    (concat
      (->>
        ended-facts
        (map (partial end (java.util.Date.)))
        (map (partial put-fact! node))
        doall)
      (->>
        updated-facts
        (map (partial put-fact! node))
        doall)
      )))

(defn -main [& args]
  (let [[action & args] args]
    (with-open [xtdb-node (start-xtdb! "data")]
      (case action
        "load"
        (doall
          (map
            #(log/info %)
            (concat
              (load-stage! xtdb-node)
              (transform-facts! xtdb-node))))
        "list"
        (let [stage (get-all-stage xtdb-node)
              facts (get-all-active-facts xtdb-node)]
          (pp/pprint stage)
          (println (str "Count of Stage: " (count stage)))
          (pp/pprint facts)
          (println (str "Count of Facts: " (count facts))))
        "clear"
        (doall (map #(log/info %) (clear-all-stage! xtdb-node)))
        (log/info "list|load|clear")))))

(comment

  (with-open [xtdb-node (start-xtdb! "data")]
    (transform-facts! xtdb-node))

  (with-open [xtdb-node (start-xtdb! "data")]
    (get-all-active-facts xtdb-node))

  (with-open [xtdb-node (start-xtdb! "data")]
    (get-all-facts xtdb-node))

  (with-open [xtdb-node (start-xtdb! "data")]
    (load-stage! xtdb-node))

  (-main "load")

  (-main "list")

  (-main "clear")

  (-main)

  (java.util.Date.)


  (=
    {:description
     {:type "text/html",
      :value "RAPHO TOWNSHIP;  LEBANON RD & SHEARERS CREEK; "},
     :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
     :published-date #inst "2022-09-07T05:35:36.000-00:00",
     :title "VEHICLE ACCIDENT-NO INJURIES",
     :uri "1a2ae9e1-fc96-4408-befc-b7eea4fdd785",
     :type :stage,
     :xt/id {:uri "1a2ae9e1-fc96-4408-befc-b7eea4fdd785", :type :stage}}
    {:description
     {:type "text/html",
      :value "RAPHO TOWNSHIP;  LEBANON RD & SHEARERS CREEK; "},
     :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
     :published-date #inst "2022-09-07T05:35:36.000-00:00",
     :title "VEHICLE ACCIDENT-NO INJURIES",
     :uri "1a2ae9e1-fc96-4408-befc-b7eea4fdd785",
     :type :stage,
     :xt/id {:uri "1a2ae9e1-fc96-4408-befc-b7eea4fdd785", :type :stage}})

  (remove (set [1 2 3]) [3 4 5])

  .)
