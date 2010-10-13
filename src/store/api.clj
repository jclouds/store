(ns store.api
  (:use store.s3)
  (:require [clomert :as v]))

;;TODO: can get rid of all these and ust partially apply try-default in the data-domain fn below.
(defn put*- [bucket s3 v k]
(try-default nil
  put-clj s3 bucket (str k) v))

(defn get*- [bucket s3 k]
  (try-default nil
   get-clj s3 bucket (str k)))

(defn update*- [bucket s3 k]
  (try-default nil
   append-clj s3 bucket (str k)))

(defn delete*- [bucket s3 k]
  (try-default nil
   delete-object s3 bucket (str k)))

(defn exists?*- [bucket s3 k]
  (or
   (some #(= k (.getKey %))
	 (try-default nil
		 (comp seq objects)
		 s3 bucket (str k)))
   false))

;;example {:user "s3-bucket-name-for-user"}

(defmacro data-domain
  [m]
  `(do
     (defn put* [k# & args#] (apply put*- (~m k#) args#))
     (defn get* [k# & args#] (apply get*- (~m k#) args#))
     (defn update* [k# & args#] (apply update*- (~m k#) args#))
     (defn delete* [k# & args#] (apply delete*- (~m k#) args#))
     (defn exists?* [k# & args#] (apply exists?*- (~m k#) args#))))

(data-domain learner-data)    

;;TODO: can't compose in this way becasue macro evaluates the map at macroexpand time.  change in clomert.
;; (defn factory [c]
;;               (v/make-socket-store-client-factory
;;                (v/make-client-config c)))

(defmacro vstore
  [factory]
  `(do
     (defn put* [bucket# k# v#]
       (v/do-store
	(v/make-store-client factory (str bucket#))
	(:put k# v#)))
     (defn get* [bucket# k#]
       (v/versioned-value (v/do-store
			   (v/make-store-client factory (str bucket#))
			   (:get k#))))
     (defn update* [bucket# k# v#]
      (v/store-apply-update
       (v/make-store-client factory (str bucket#))
       (fn [client]
         (let [ver (v/store-get client k#)
               val (v/versioned-value ver)]
           (v/store-conditional-put client
                                    k#
                                    (v/versioned-set-value! ver (append 
                                                                 v#
                                                                 val)))))))
     (defn delete*  [bucket# k#]
       (v/do-store
	(v/make-store-client factory (str bucket#))
	(:delete k#)))
     (defn exists?* [k# & args#] )))

;;(vstore (factory  {:bootstrap-urls "tcp://localhost:6666"}))