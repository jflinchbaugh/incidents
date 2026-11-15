(ns incidents.parse
  (:require [clojure.string :as str]
            [feedparser-clj.core :as fp]
            [tick.core :as tc]))

(defn tag [type rec]
  (assoc rec :type type))

(defn prune [rec]
  (into {}
        (remove
         (fn [[_ v]]
           (or
            (nil? v)
            (and
             (or
              (string? v)
              (coll? v))
             (empty? v)))) rec)))

(defn incident-type [fact]
  (cond
    (nil? fact) nil
    (nil? (:title fact)) nil
    (re-matches #".*(Standby).*" (:title fact)) :fire
    (re-matches #".*(EMS|Medical|Transfer).*" (:title fact)) :medical
    (re-matches #".*(Traffic|Vehicle).*" (:title fact)) :traffic
    :else :fire))

(defn override [s]
  (get
   {"Bls" "BLS"
    "Ems" "EMS"
    "Qrs" "QRS"
    "Tac" "TAC"
    "Int" "INT"
    "1a" "1A"
    "2a" "2A"
    "3a" "3A"
    "Co" "CO"
    "Ecc" "ECC"
    "Ema" "EMA"
    "Micu" "MICU"}
   s
   s))

(defn mc-fix [s]
  (str/replace
   s
   #"^Mc(.)"
   (fn [[_ letter]] (str "Mc" (str/upper-case letter)))))

(defn title-case [s]
  (str/join (map (comp mc-fix override str/capitalize) (str/split s #"\b"))))

(def format-unit (comp title-case str/trim))

(def format-street (comp title-case str/trim))

(defn parse-units [s]
  (if (nil? s) []
      (mapv format-unit (str/split s #"<br>"))))

(defn parse-streets [s]
  (if (nil? s) []
      (mapv format-street (str/split s #"[&/]"))))

(defn format-title [title]
  (if (nil? title) nil
      (->
       title
       (str/replace #" +- +" "-")
       title-case
       str/trim)))

(defn format-municipality [name]
  (if (nil? name) nil
      (str/trim (title-case name))))

(defn add-incident-type [fact]
  (if (nil? fact) nil
      (assoc fact :incident-type (incident-type fact))))

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
      :active? true
      :title (format-title (:title in))
      :municipality (format-municipality municipality)
      :streets (parse-streets streets)
      :units (parse-units units)}
     add-incident-type)))

(defn end [date fact]
  (assoc fact
         :end-date date
         :active? false
         :duration-minutes (tc/minutes (tc/between (:start-date fact) date))))

(defn parse-feed [feed]
  (->>
   feed
   :doc
   java.io.StringBufferInputStream.
   fp/parse-feed
   :entries
   (map prune)
   (sort-by :uri)
   (map parse)
   (mapv (partial tag :fact))))
