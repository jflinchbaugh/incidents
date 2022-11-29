(ns incidents.core
  (:gen-class)
  (:require [feedparser-clj.core :as feed]
            [clojure.java.io :as io]
            [xtdb.api :as xt]
            [clojure.pprint :as pp]
            [java-time.api :as t]
            [taoensso.timbre :as log]
            [clojure.string :as str]
            [hiccup.core :as h]
            [hiccup.page :as p]
            [hiccup.element :as e]
            [hiccup.util :as u]))

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

(defn incident-type [fact]
  (cond
    (nil? fact) nil
    (nil? (:title fact)) nil
    (re-matches #".*(EMS|Medical|Transfer).*" (:title fact)) :medical
    (re-matches #".*(Traffic|Vehicle).*" (:title fact)) :traffic
    :else :fire))

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

(defn- override [s]
  (get
   {"Bls" "BLS"
    "Ems" "EMS"
    "Qrs" "QRS"
    "Amb" "Ambulance"
    "Tac" "TAC"
    "Int" "INT"
    "1a" "1A"
    "2a" "2A"
    "3a" "3A"
    "Co" "CO"}
   s
   s))

(defn- mc-fix [s]
  (str/replace
   s
   #"^Mc(.)"
   (fn [[_ letter]] (str "Mc" (str/upper-case letter)))))

(defn- title-case [s]
  (str/join (map (comp mc-fix override str/capitalize) (str/split s #"\b"))))

(def format-unit (comp title-case str/trim))

(def format-street (comp title-case str/trim))

(defn- parse-units [s]
  (if (nil? s) '()
      (map format-unit (str/split s #"<br>"))))

(defn- parse-streets [s]
  (if (nil? s) '()
      (map format-street (str/split s #"[&/]"))))

(defn- format-title [title]
  (-> title
      (str/replace #" +- +" "-")
      title-case))

(defn- format-municipality [name]
  (str/trim (title-case name)))

(defn add-incident-type [fact]
  (assoc fact :incident-type (incident-type fact)))

(defn parse [in]
  (let [parts (str/split (get-in in [:description :value]) #"; *")
        [municipality streets units] parts
        [streets units] (if
                         (re-matches #".*COUNTY$" municipality)
                          [nil streets]
                          [streets units])]
    (->>
      {:uri (:uri in)
         :start-date (:published-date in)
         :title (format-title (:title in))
         :municipality (format-municipality municipality)
         :streets (parse-streets streets)
       :units (parse-units units)}
      add-incident-type)))

(defn cleanup [fact]
  (->>
    {:title (format-title (:title fact))
     :municipality (format-municipality (:municipality fact))
     :streets (map format-street (:streets fact))
     :units (map format-unit (:units fact))}
    (merge fact)
    add-incident-type))

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

(defn format-date [d]
  (t/format "yyyy-MM-dd HH:mm" (t/local-date-time d (t/zone-id))))

(defn- format-streets [streets]
  (str/join " & " streets))

(defn- format-map-location [fact]
  (let [municipality (:municipality fact)
        streets (:streets fact)]
    (str/join
     " "
     [(format-streets streets)
      (str/replace municipality #" Township| City| Borough" "")
      "PA"])))

(defn report-entry [f]
  [:li
   [:div.streets
    (e/link-to
     {:target "_blank"}
     (u/url "https://www.google.com/maps/search/"
            {:api 1 :query (format-map-location f)})
     (format-streets (:streets f)))]
   [:div.municipality (:municipality f)]
   [:div.start-date (format-date (:start-date f))]
   [:div.title (:title f)]
   [:div.units (str/join ", " (:units f))]])

(comment
  {:uri "63d885a1-8b82-44ec-a7be-e159e8e34846",
   :start-date #inst "2022-11-10T18:37:25.000-00:00",
   :title "Routine Transfer-class 3",
   :municipality "West Hempfield Township",
   :streets ("Marietta Ave" "Westover Dr"),
   :units ("Amb 77-2"),
   :type :fact,
   :xt/id {:uri "63d885a1-8b82-44ec-a7be-e159e8e34846", :type :fact}})

(defn report-active [facts output-dir]
  (let [title "Active Incidents"]
    (spit (str output-dir "/index.html")
          (str
           (p/html5 {:lang "en"}
                    [:head
                     [:title title]
                     (p/include-css "style.css")]
                    [:body
                     [:h1 title]
                     [:ul
                      (map report-entry (reverse (sort-by :start-date facts)))]])))))

(defn copy-file! [src dest]
  (io/copy (io/input-stream (io/resource src)) (io/file dest)))

(defn copy-resources! [dest]
  (copy-file! "web/htaccess" (str dest "/.htaccess"))
  (copy-file! "web/style.css" (str dest "/style.css")))

(defn -main [& args]
  (let [[action & args] args]
    (with-open [xtdb-node (start-xtdb! "data")]
      (case action
        "cleanup"
        (->> xtdb-node
             get-all-facts
             (pmap (comp (partial put-fact! xtdb-node) cleanup))
             doall
             (#(log/info "Processed: " (count %))))
        "load"
        (doall
         (concat
          (load-stage!
           xtdb-node
           "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")
          (transform-facts! xtdb-node)))
        "list-active"
        (let [stage (get-all-stage xtdb-node)
              facts (get-all-active-facts xtdb-node)]
          (->> stage (map pp/pprint) doall)
          (log/info "Count of Stage:" (count stage))
          (->> facts (map pp/pprint) doall)
          (log/info "Count of Facts:" (count facts)))
        "list-all"
        (let [stage (get-all-stage xtdb-node)
              facts (get-all-facts xtdb-node)]
          (doall (map prn stage))
          (log/info "Count of Stage:" (count stage))
          (doall (map prn facts))
          (log/info "Count of Facts:" (count facts)))
        "report-active"
        (if (nil? (first args))
          (log/info "report-active <output-dir>")
          (let [facts (get-all-active-facts xtdb-node)
                output-dir (first args)]
            (report-active facts output-dir)
            (copy-resources! output-dir)))
        "load-and-report"
        (if (nil? (first args))
          (log/info "report-active <output-dir>")
          (do
            (doall
             (concat
              (load-stage!
               xtdb-node
               "https://webcad.lcwc911.us/Pages/Public/LiveIncidentsFeed.aspx")
              (transform-facts! xtdb-node)))
            (let [facts (get-all-active-facts xtdb-node)
                  output-dir (first args)]
              (report-active facts output-dir)
              (copy-resources! output-dir))))
        "clear"
        (doall (map #(log/info %) (clear-all-stage! xtdb-node)))
        (log/info "cleanup|list-active|list-all|report-active|load|clear")))))

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

  (-main "cleanup")

  (-main)

  (with-open [node (start-xtdb! "data")]
    (->>
     (xt/q (xt/db node) '{:find [?description]
                          :where [[?e :type :fact]
                                  [?e :description ?description]]})))

  (with-open [node (start-xtdb! "data")]
    (->>
     (xt/attribute-stats node)))

  (with-open [node (start-xtdb! "data")]
    (->> node
         get-all-facts
         (map :title)
         (group-by identity)
         keys
         sort))

  (with-open [node (start-xtdb! "data")]
    (->> node
      get-all-facts
      (take 10)
      ))

  .)
