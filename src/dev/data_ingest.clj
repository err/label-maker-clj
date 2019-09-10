(ns data-ingest
  (:require [clj-uuid :as uuid]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn substring-position
  [string substring]
  (let [first-idx (str/index-of string substring)
        last-idx  (str/last-index-of string substring)]
    (cond
      (nil? first-idx)       nil
      (= first-idx last-idx) [first-idx (+ first-idx (count substring))]
      :else                  (throw (ex-info "Multiple occurrences of substring within string."
                                             {:string string :substring string})))))

(defn add-text-phrases
  [{:q/keys [input] :as row}]
  (let [labels (dissoc row :q/input :q/status :q/unparsed)
        phrases (into #{}
                      (keep (fn [[label phrase]]
                              (when (not (str/blank? phrase))
                                (when-let [pos (substring-position input phrase)]
                                  {:phrase/label label
                                   :phrase/pos pos}))))
                      labels)]
    (assoc row :text/phrases phrases)))

(defn prepare-text-doc
  "Shapes data into the format expected by storage layer"
  [{:as     rec
    text    :q/input
    phrases :text/phrases
    ts-id   :text-set/id}]
  {:crux.db/id  (uuid/v1)
   :text-set/id ts-id
   :text/raw    text
   :text/phrases phrases})

(defn prepare-input-file
  [{f :file
    ts-id :text-set/id
    ts-name :text-set/name}]
  {:pre [(and ts-id
              (not (str/blank? ts-name))
              (not (str/blank? f)))]}
  (with-open [r (io/reader f)]
    (let [[[_status _input _unparsed & features] & rows] (csv/read-csv r)
          labels (into [:q/status :q/input :q/unparsed]
                       (mapv keyword features))
          ;;_ (prn [:labels labels])
          ;;_ (prn [:rows-n (count rows)])
          process-row (comp (take 4)
                            (map #(zipmap labels %))
                            (map add-text-phrases)
                            (map #(assoc % :text-set/id ts-id))
                            (map prepare-text-doc))]
      {:text-set {:crux.db/id ts-id
                  :text-set/name ts-name}
       :texts (into [] process-row rows)})))

(comment ;; Usage
  (prepare-input-file {:file "/Path/To/Data/File.csv"
                       :text-set/id (uuid/v1)
                       :text-set/name "MTurk Sample Set N"}))

(comment ;; Testing substring-position
  (let [input "2bd 2ba loft atl ga under 1500 for a family of 3"]
    (map #(substring-position input %)
         ["2bd"
          "2ba"
          "loft"
          "atl ga"
          "under 1500"]))
  ;;=>
  ;; ([0 3] [4 7] [8 12] [13 19] [20 30])
  ;; matches the values in the `sample-texts`
  ;; from comment at end of app.db namespace
  )

(comment ;; Testing add-text-phrases
  (let [input "2bd 2ba loft atl ga under 1500 for a family of 3"
        csv-row {:q/input input
                 :beds "2bd"
                 :baths "2ba"
                 :ptype "loft"
                 :cityst "atl ga"
                 :price-lte "under 1500"}]

    (assert
     (= (add-text-phrases csv-row)
        (assoc csv-row :text/phrases #{{:phrase/pos [ 0  3] :phrase/label :beds}
                                       {:phrase/pos [ 4  7] :phrase/label :baths}
                                       {:phrase/pos [ 8 12] :phrase/label :ptype}
                                       {:phrase/pos [13 19] :phrase/label :cityst}
                                       {:phrase/pos [20 30] :phrase/label :price-lte}})))))
