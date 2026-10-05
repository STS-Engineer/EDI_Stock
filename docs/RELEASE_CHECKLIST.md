# Revue et mise en service

## Non négociable avant publication

1. Faire révoquer/renouveler par l’administrateur le secret PostgreSQL exposé dans le code historique. Le retirer du nouveau fichier ne le retire pas de l’historique Git et ne neutralise pas l’ancien identifiant. Aucune rotation réalisée ici.
2. Revoir ce lot dans une branche de travail. Ne pas copier directement dans master : son workflow existant déploie automatiquement sur push. Le workflow fourni ajoute les tests avant déploiement, mais ne constitue pas une autorisation de publication.
3. Vérifier le DDL réel : types/longueurs de colonnes, clés, unicité, index et droits. Auditer les doublons de transit par Site/AVOMaterialNo sans les supprimer automatiquement.
4. Valider les changements métier : rejet des quantités négatives/fractionnaires/invalides ; une seule ligne de solde InTransit ; surlivraison bloquée au lieu d’une remise silencieuse à zéro ; numéro limité à 28 caractères ; validation des champs EDI. Le cas Delivered sans solde conserve le comportement historique et doit être approuvé.
5. Confirmer la protection d’accès de l’interface via la plateforme. Ne pas activer UI_IMPORTS_ENABLED sur une URL publique sans authentification. Provisionner les secrets de façon autorisée et sécurisée.

## Staging isolé

- Appliquer après autorisation `migrations/001_import_ledger.sql` dans une DB de test, jamais dans la production pendant les tests.
- Fournir DATABASE_URL, SECRET_KEY et, si intégration API approuvée, IMPORT_API_TOKEN. Ne jamais copier les secrets dans Git ou Make en clair.
- Vérifier les droits minimaux et TLS. Prévoir sauvegarde et procédure de restauration avant tout déploiement.
- Exécuter tous les tests hors réseau, puis des tests PostgreSQL de staging autorisés : deux appels concurrents même clé, même clé/contenu différent, deux imports chevauchant un article, rollback et perte de réponse après commit.
- Rejouer un corpus anonymisé validé de PDF/CSV/XLS/XLSX avec résultats attendus, contrôler totaux et références. Aucun corpus réel n’a été utilisé pour ce lot.
- Vérifier visuellement desktop/mobile et navigation arrière, rechargement, double clic, annulation, erreurs et expiration d’aperçu. Lancement Chromium bloqué dans l’environnement de préparation ; vérification visuelle non réalisée.
- Fixer une version verrouillée de toutes les dépendances après validation de votre politique de sécurité. requirements.txt borne les versions majeures mais n’est pas un lockfile complet.

## Déploiement et retour arrière

- Approbation explicite du responsable, fenêtre de changement et bascule Make après validation de bout en bout.
- Avant la bascule, arrêter/désactiver sous autorisation les anciens writers directs PostgreSQL : ils ne respectent pas les nouveaux verrous advisory et peuvent encore provoquer des courses. Ne pas faire coexister les deux chemins d’écriture sur le même périmètre.
- Conserver le registre edi_imports entre versions ; ne pas le purger au rollback, sinon les reprises pourraient être réinsérées.
- Un rollback applicatif n’annule pas les données insérées. Réconcilier les imports via import_id. Ne jamais restaurer l’ancien secret dans le code.
- Surveiller codes 409/422/503, délais, volumes et utilisation du stockage d’aperçus. Ne pas journaliser pièces jointes, paramètres SQL, bearer tokens ou URL de DB.
- Supprimer du futur paquet de déploiement les outputs historiques avec données métier. Le workflow fourni les exclut ; le retrait de Git/historique doit être traité séparément sous autorisation.
