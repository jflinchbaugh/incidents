(ns incidents.report
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hiccup.element :as e]
            [hiccup.page :as p]
            [hiccup.util :as u]
            [nextjournal.clerk :as clerk]
            [taoensso.telemere :as log]
            [tick.core :as tc]))

(defn format-date-part [fmt d]
  (tc/format fmt (tc/date-time d)))

(defn format-date-time [d]
  (format-date-part "yyyy-MM-dd HH:mm:ss" d))

(defn format-date [d]
  (format-date-part "yyyy-MM-dd" d))

(defn format-streets [streets]
  (str/join " & " (or streets [])))

(defn format-map-location
  [municipality streets]
  (str/join
   " "
   [(format-streets streets)
    municipality
    "PA"]))

(defn map-link
  ([municipality streets]
   (u/url "https://www.google.com/maps/search/"
          {:api 1 :query (format-map-location municipality streets)}))
  ([fact]
   (map-link (:municipality fact) (:streets fact))))

(defn report-entry [f]
  [:li.incident-card {:class (name (:incident-type f))}
   [:div.streets
    (e/link-to
     {:target "_blank"}
     (map-link f)
     (format-streets (:streets f)))]
   [:div.municipality
    (e/link-to
      {:target "_blank"}
      (map-link f)
      (:municipality f))]
   [:div.start-date (format-date-time (:start-date f))]
   [:div.title (:title f)]
   [:div.units (str/join ", " (:units f))]])

(defn format-incident-type [type]
  (str/capitalize (name type)))

(defn reload-js [period delay]
   [:script
    (->
      "scheduleReload(${period}, ${delay});"
      (str/replace "${period}" (str period))
      (str/replace "${delay}" (str delay)))])

(defn report-active [facts output-dir]
  (let [title "Active Incidents"
        out-file (io/file output-dir "index.html")]
    (io/make-parents out-file)
    (spit out-file
          (str
           (p/html5 {:lang "en"}
                    [:head
                     [:meta {:name "viewport"
                             :content "width=device-width, initial-scale=1"}]
                     [:title title]
                     (p/include-css "style.css")
                     [:link
                      {:rel "shortcut icon"
                       :href "emergency-light.png"}]]
                    [:body
                     [:header.main-header
                      [:img.logo {:src "emergency-light.png" :alt ""}]
                      [:h1 title]]
                     (apply
                      concat
                      (for [type [:traffic :fire :medical]]
                        (let [incidents (->>
                                         facts
                                         (filter
                                          (fn [fact]
                                            (= type (:incident-type fact))))
                                         (sort-by :start-date)
                                         reverse)]
                          [[:h2
                            (format-incident-type type)
                            " ("
                            (count incidents)
                            ")"]
                           [:ul
                            (map
                             report-entry
                             incidents)]])))
                     [:footer
                      (e/link-to
                       {:target "_blank"}
                       "clerk/index.html"
                       "Clerk")
                      " • "
                      (e/link-to
                       {:target "_blank"}
                       "https://www.lcwc911.us/live-incident-list"
                       "Lancaster Live Incidents")]
                     (p/include-js "reloader.js")
                     (reload-js 60000 0)])))))

(defn copy-file! [src dest]
  (io/copy (io/input-stream (io/resource src)) (io/file dest)))

(defn copy-resources! [dest]
  (copy-file! "web/htaccess" (str dest "/.htaccess"))
  (copy-file! "web/emergency-light.png" (str dest "/emergency-light.png"))
  (copy-file! "web/style.css" (str dest "/style.css"))
  (copy-file! "web/reloader.js" (str dest "/reloader.js")))

(defn start-clerk! []
  (clerk/serve! {:browse? true :watch-paths ["notebooks"]}))

(defn stop-clerk! []
  (clerk/halt!))

(defn log-lines [s]
  (doseq [line (str/split s #"\n")]
    (log/log! :info line)))

(defn build-clerk! [out-path]
  (->
   {:paths ["notebooks/incidents.clj"]
    :out-path (io/file out-path "clerk")}
   clerk/build!
   with-out-str
   log-lines))
