# Contrat d’import JSON v1

Cette API attend des lignes déjà extraites et vérifiées structurellement. Elle ne reçoit ni PDF/base64 ni nom de fichier. Elle n’est pas le service existant `/process-GermanySite`.

POST `/api/v1/imports`

En-têtes : `Content-Type: application/json`, `Authorization: Bearer <jeton approuvé>`, `Idempotency-Key: <clé stable de document/pièce jointe>`. Clé : 1 à 196 caractères ASCII, premier alphanumérique, suite alphanumérique ou `_.:/-`. Ne pas inclure de donnée personnelle sensible dans la clé. Une empreinte stable de l’identité source convient.

Objet exactement composé de `file_type` et `rows`. Types : `EDI` ou `LIVRAISON`. Maximum 10 000 lignes et 16 Mio par requête. Colonnes inconnues refusées ; types imbriqués refusés.

## Livraison

Toutes les colonnes sont obligatoires : `Site`, `AVOMaterialNo`, `DeliveryNo`, `Quantity`, `Date`, `Status`.

- Quantity : entier positif, maximum 2 147 483 647.
- Date : AAAA-MM-JJ valide.
- Status : Dispatched, Delivered, InTransit. Alias Sent et variantes « In transit » acceptés.
- DeliveryNo : 28 caractères maximum, suffixe interne `_T` réservé dans la limite historique de 30.
- Les codes restent du texte. Les suffixes PL/SP séparés par un espace sont joints à AVOMaterialNo.
- Les lignes identiques sur Site/Article/Numéro/Date/Statut sont agrégées par somme avant insertion.

## EDI

Obligatoires : `Site`, `ClientCode`, `ClientMaterialNo`, `AVOMaterialNo`, `DateFrom`, `Quantity`, `ForecastDate`, `EDIStatus`.

Facultatives : `DateUntil`, `LastDeliveryDate`, `LastDeliveredQuantity`, `CumulatedQuantity`, `ProductName`, `LastDeliveryNo`. Elles deviennent null si absentes ou vides.

Quantités entières non négatives. DateFrom, ForecastDate, LastDeliveryDate : date ISO ou semaine ISO valide AAAA-WSS. DateUntil reste un texte pour compatibilité. EDIStatus : Forecast, Forcast (alias historique conservé), Firm, PO. Les valeurs ne sont pas toutes harmonisées avant stockage : valider les attentes des consommateurs.

## Réponses

Après commit réussi, HTTP 201 :

```json
{"status":"imported","import_id":"uuid","rows_imported":1,"rows_received":1,"file_type":"LIVRAISON"}
```

Même clé et même contenu normalisé : HTTP 200, même receipt avec `status: already_imported`. `rows_received` compte les lignes de la requête reçue ; `rows_imported` compte les lignes logiques après agrégation ; un événement Dispatched peut aussi créer/mettre à jour un solde InTransit.

Erreur :

```json
{"status":"error","error":{"code":"validation_failed","message":"…","retryable":false,"details":[{"row":2,"column":"Quantity","message":"…"}]},"request_id":"…"}
```

- 400 bad_request : JSON ou clé invalide/manquante.
- 401 unauthorized : jeton absent ou invalide.
- 409 idempotency_conflict : même clé, contenu différent. Ne pas créer automatiquement une nouvelle clé pour contourner ce refus.
- 409 inventory_conflict : plusieurs soldes transit, dépassement ou incohérence. Réconciliation humaine.
- 413 payload_too_large ; 415 unsupported_media_type ; 422 validation_failed : correction requise.
- 503 database_unavailable, retryable=true, Retry-After:30 : reprise bornée avec la même clé ET le même contenu. Un timeout de commit peut être ambigu ; la clé protège la reprise.
- 422 database_data_error ou 409 database_constraint_conflict : incompatibilité de données/contrainte, retryable=false.
- 503 not_configured ou database_schema_error, retryable=false : configuration/schéma, intervention opérateur.
- 500 internal_error, retryable=false : support ; conserver le document source.

## Barrière Make

Ne déplacer/archiver le mail qu’après vérification de HTTP 200/201 ET status imported/already_imported ET import_id non vide, file_type attendu et rows_received égal au nombre de lignes source, rows_imported strictement positif et inférieur ou égal à rows_received (égal pour EDI). Si un mail contient plusieurs pièces jointes, toutes doivent avoir un résultat terminal acceptable avant de déplacer le mail. Une branche parallèle en succès ne prouve pas le succès des autres.

Le service bearer doit être protégé en HTTPS, rate-limité par la plateforme et limité au périmètre d’intégration autorisé. L’authentification déployée n’a pas été configurée par ce travail.
