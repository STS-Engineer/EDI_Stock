# Vérifications du lot

Exécutées le 5 octobre 2026 dans une copie de travail isolée, Python 3.12.

## Réussies

- 78 tests automatisés hors réseau : validation de quantités/dates/colonnes, conservation des zéros, CSV et classeur synthétique, dates Excel natives, PDF simulés, absence d’invention d’en-têtes, CSRF, session et expiration, accès croisé refusé, annulation, répétition de confirmation, API/auth/idempotence, limites, isolation SQL site/article, rollback simulé et erreur de commit, classification d’erreurs et absence de diagnostics sensibles.
- Test concurrent avec deux appels et simulation des verrous transactionnels : un import, un accusé de réception de rejeu. Ce n’est pas une validation sur PostgreSQL réel.
- Ruff : aucune anomalie dans les règles configurées.
- Compilation Python des modules et tests : réussie.
- Interface : sept états Jinja, noms des champs, échappement, aperçu plafonné, états de confirmation, syntaxe JavaScript, protection des envois répétés et restauration pageshow vérifiés. Contrastes calculés : bouton principal 5,2:1 ; texte secondaire 5,12:1.
- Logo AVOCarbon Group officiel : image d’origine inspectée visuellement, présence et texte alternatif vérifiés sur les deux flux, route statique et empreinte SHA-256 vérifiées par deux nouveaux tests. Les trois écrans de l’aperçu autonome intègrent l’image originale sans ressource réseau. Les styles préservent les proportions de l’image.

## Non exécutées ou bloquées

- Aucune connexion PostgreSQL réelle, inspection du schéma réel, migration, essai SQL d’intégration, benchmark ni preuve de concurrence PostgreSQL.
- Aucun appel des scénarios Make, du service `/process-GermanySite` ou de la production Azure.
- Aucun test avec pièces client réelles. Les parseurs PDF nécessitent un corpus représentatif approuvé et une réconciliation des pages/totaux.
- Chromium n’a pas pu démarrer : création de socket refusée dans l’environnement. Aucun screenshot ni validation visuelle desktop/mobile. Les contrôles HTML/JS ne remplacent pas cette étape.
- La publication de ce lot dans une branche GitHub de revue ne déploie pas la production. Aucune modification de sécurité effective, création/rotation de credentials ou activation de scénario.

La version est une proposition de modernisation testée hors ligne, pas une version approuvée pour la production.
