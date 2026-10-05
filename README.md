# EDI Stock · version de modernisation à valider

Version de travail basée sur `STS-Engineer/EDI_Stock`, commit `8711935cb7545497412955e79de955959f6d221b` (branche master, consultée le 5 octobre 2026). Publication proposée dans une branche de revue et une pull request en brouillon. Aucun déploiement Azure ni changement de base réelle ou de scénario Make n’est effectué par cette publication.

## Changements

- Application Flask modulaire, compatible `gunicorn App:app`, sans connexion DB à l’import du module.
- Interface française responsive : modèle → import → validation → confirmation. Validation serveur complète avant toute écriture, aperçu limité à 20 lignes, erreurs par ligne.
- CSV/XLS/XLSX avec codes textuels préservés ; PDF de facture de livraison compatible avec le format historique, extraction des tableaux une seule fois et repli texte par page.
- Quantités entières contrôlées, dates réelles vérifiées, champs obligatoires et colonnes inconnues contrôlés. Aucune conversion silencieuse de « 12 kg » en 12 ou d’une fraction en entier.
- Aperçus à identifiants aléatoires, signés et liés à une session, CSRF, expiration à une heure, limites de taille et erreurs sans détails techniques sensibles.
- Transactions DB et registre d’idempotence commun à l’interface et à l’API. Les reprises après timeout réutilisent la même clé.
- Corrections des mises à jour de transit : Site et AVOMaterialNo inclus dans la clause WHERE ; sérialisation transactionnelle par site/article ; incohérences bloquées.
- API JSON post-parsing pour Make. Elle ne remplace pas le service séparé `sts-api /process-GermanySite`, qui reçoit des pièces jointes base64.
- Tests hors réseau, lint et étape de test avant le déploiement Azure existant. Le déploiement Azure reste bloqué jusqu’à validation des prérequis.

## Installation locale

Python 3.12. Installer `requirements-dev.txt` dans un environnement virtuel. Exécuter :

```sh
python -m pytest
ruff check .
python -m compileall -q App.py edi_stock tests
```

Les tests injectent des dépôts et connexions simulés. Ne pas renseigner de vrais accès de production pour les lancer. Ne pas tester sur les tables de production.

L’application se lance avec `gunicorn --bind 127.0.0.1:8000 App:app`. Les imports interactifs sont désactivés par défaut. Les secrets sont fournis par les paramètres sécurisés de l’hébergeur ; le fichier `.env.example` n’est pas chargé automatiquement. Les secrets ne doivent jamais être committés.

## Configuration requise

- `DATABASE_URL` : URL PostgreSQL SQLAlchemy, TLS requis en hébergement.
- `SECRET_KEY` : secret stable et robuste de session, identique entre workers.
- `IMPORT_API_TOKEN` : jeton approuvé pour les appels API. À provisionner de façon sécurisée ; aucun jeton créé dans ce livrable.
- `PREVIEW_DIR` : répertoire privé persistant partagé entre workers de la même instance. En multi-instance, utiliser un stockage commun ou de l’affinité de session ; le registre d’idempotence est en PostgreSQL.
- `COOKIE_SECURE=true` en HTTPS ; `false` uniquement pour un essai local HTTP.
- `UI_IMPORTS_ENABLED=false` par défaut. Ne passer à `true` qu’après vérification de l’authentification et des restrictions d’accès de la plateforme. L’application ne fournit pas de compte utilisateur ni de SSO.

`/healthz` indique seulement que le processus répond, pas que la DB ou la migration est prête.

## Intégration

Voir [docs/API.md](docs/API.md) et [docs/RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md). Les routes `/`, `/preview`, `/insert` et `/download/template/<name>.<ext>` et les colonnes métier restent présentes. Les formulaires nécessitent désormais un jeton CSRF ; `temp_file` devient un jeton opaque signé, pas un chemin. Les anciens liens d’aperçu et `/view/temp/*` ne sont plus pris en charge. Ne pas connecter une automatisation aux routes HTML.

La table `edi_imports` est nouvelle. La migration SQL est fournie pour revue et exécution autorisée en staging, jamais lancée automatiquement. Toutes les lignes métier et le registre sont validés dans la même transaction.

## Limites explicites

- Schéma PostgreSQL, contraintes existantes, concurrence réelle, performances et données métier réelles non vérifiés. Les tests de concurrence simulent les verrous advisory ; ils ne remplacent pas un essai PostgreSQL en staging.
- Le registre protège une clé réutilisée. Une nouvelle clé pour le même fichier constitue un nouvel import. Dans l’UI, confirmer deux fois le même aperçu est protégé ; téléverser à nouveau crée un nouvel aperçu et peut doubler les données. Une déduplication métier inter-fichiers doit être définie séparément.
- Les PDF scannés, formats inconnus ou lignes non reconnues nécessitent une vérification humaine dans le modèle Excel. Le parseur de facture historique affecte le site Tunisia et le statut Dispatched ; valider ces valeurs dans l’aperçu. Il ne peut pas garantir la complétude de tous les PDF.
- Les codes saisis comme nombres dans Excel peuvent avoir perdu leurs zéros avant l’import. Les modèles générés mettent les cellules en format texte.
- Les aperçus expirés sont nettoyés au prochain upload. Prévoir une politique de rétention opérationnelle pour une instance sans nouveaux uploads.
- Le préfixe d’API est proposé pour une intégration après extraction de lignes normalisées. Les scénarios Make existants ne deviennent pas compatibles par simple remplacement d’URL.
- Les fichiers d’origine contenant des données et les anciens secrets ne sont pas reproduits dans cette livraison. Les anciens outputs déjà suivis dans Git ne sont pas supprimés automatiquement.

## Sources de conception

- [Code d’origine](https://github.com/STS-Engineer/EDI_Stock/tree/8711935cb7545497412955e79de955959f6d221b)
- [Sécurité Flask](https://flask.palletsprojects.com/en/stable/web-security/)
- [Tests Flask](https://flask.palletsprojects.com/en/stable/testing/)
- [Transactions SQLAlchemy](https://docs.sqlalchemy.org/en/20/tutorial/dbapi_transactions.html)
- [Verrous transactionnels PostgreSQL](https://www.postgresql.org/docs/15/functions-admin.html)
