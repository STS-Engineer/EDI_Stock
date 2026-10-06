# Vérifications du lot

Vérifications initiales du 5 octobre 2026 ; suites applicative et d’intégration réexécutées le 6 octobre 2026 dans une copie de travail isolée, Python 3.12.

## Réussies

- 132 tests applicatifs hors réseau : validation de quantités/dates/colonnes, conservation des zéros, CSV et classeur synthétique, dates Excel natives, PDF simulés, absence d’invention d’en-têtes, CSRF, session et expiration, accès croisé refusé, annulation, répétition de confirmation, API/auth/idempotence, limites, isolation SQL site/article, rollback simulé et erreur de commit, classification d’erreurs et absence de diagnostics sensibles.
- 73 tests publics d’intégration hors réseau : parseur, reçus, état persistant, API Flask locale et transport simulé. Les cinq tests de configurations du kit privé ne font pas partie de cette exécution.
- Compatibilité des colonnes : DateUntil obligatoire et validé comme date/semaine réelle, frontières de toutes les limites varchar, caractères Unicode, maintien des obligations métier et du plafond DeliveryNo de 28, refus API/aperçu avant écriture, contrat serveur/parseur identique et exemples synthétiques réconciliés. Le profil Valeo normalise DateUntil pour ses formats ISO, pointés et CW.
- Limites de texte et nullabilité de DeliveryDetails/EDIGlobal inspectées en lecture seule en production le 6 octobre 2026. Aucun essai d’écriture ni migration n’est effectué par ce correctif.
- Préparation distincte du correctif : clés primaires vérifiées, aucun groupe InTransit dupliqué ni solde InTransit négatif observé ; registre edi_imports appliqué après un essai réussi d’annulation DDL et vérifié vide. Cela ne constitue pas un test d’import métier.
- Test concurrent avec deux appels et simulation des verrous transactionnels : un import, un accusé de réception de rejeu. Ce n’est pas une validation sur PostgreSQL réel.
- Ruff : aucune anomalie dans les règles configurées.
- Compilation Python des modules et tests : réussie.
- Interface : sept états Jinja, noms des champs, échappement, aperçu plafonné, états de confirmation, syntaxe JavaScript, protection des envois répétés et restauration pageshow vérifiés. Contrastes calculés : bouton principal 5,2:1 ; texte secondaire 5,12:1.
- Logo AVOCarbon Group officiel : image d’origine inspectée visuellement, présence et texte alternatif vérifiés sur les deux flux, route statique et empreinte SHA-256 vérifiées par deux nouveaux tests. Les trois écrans de l’aperçu autonome intègrent l’image originale sans ressource réseau. Les styles préservent les proportions de l’image.

## Non exécutées ou bloquées

- Aucun essai SQL d’import métier/d’intégration, benchmark ni preuve de concurrence PostgreSQL pour ce lot. L’inspection des colonnes et la préparation du registre ne remplacent pas ces étapes.
- Aucun appel des scénarios Make, du service `/process-GermanySite` ni essai d’import sur la production Azure par ce correctif.
- Aucun test avec pièces client réelles. Les parseurs PDF nécessitent un corpus représentatif approuvé et une réconciliation des pages/totaux.
- Chromium n’a pas pu démarrer : création de socket refusée dans l’environnement. Aucun screenshot ni validation visuelle desktop/mobile. Les contrôles HTML/JS ne remplacent pas cette étape.
- La publication de ce lot dans une branche GitHub de revue ne déploie pas la production. Aucune modification de sécurité effective, création/rotation de credentials ou activation de scénario.

La version est une proposition de modernisation testée hors ligne, pas une version approuvée pour la production.
