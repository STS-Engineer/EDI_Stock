# Préparation de la mise en production

Contrôles initiaux du 5 octobre 2026, contrat de colonnes corrigé le 6 octobre 2026 après inspection en lecture seule. Cette branche est une revue technique ; elle ne doit pas être fusionnée tant que les contrôles ci-dessous restent ouverts.

## Déploiement observé

- Dépôt public STS-Engineer/EDI_Stock, branche par défaut `master`.
- Le workflow `.github/workflows/master_edi-sotck.yml` se déclenche sur chaque push à `master` et via déclenchement manuel. Il cible l’application Azure `Edi-sotck`, slot `Production`.
- La dernière exécution de déploiement enregistrée a réussi le 15 janvier 2026, sur `8711935cb7545497412955e79de955959f6d221b`. Aucun artefact récupérable n’est retourné pour cette exécution lors du contrôle.
- Le workflow ajouté `tests.yml` ne fait que les tests hors réseau lors d’une pull request. Aucune migration n’est exécutée par les workflows.

## Contrôles bloquants avant bascule

1. Traiter le secret PostgreSQL exposé historiquement et provisionner les paramètres requis via une procédure sécurisée. Ne jamais remettre le secret historique dans les fichiers, les journaux ou un rollback. La suppression de la valeur dans cette branche ne la révoque pas et ne nettoie pas l’historique.
2. Confirmer l’accès administratif Azure, l’URL réelle, la configuration d’authentification, le stockage des aperçus et les paramètres attendus. Une réponse de processus à `/healthz` ne valide pas la base.
3. Les longueurs, la nullabilité et les clés primaires des colonnes métier sont vérifiées ; DateUntil et les limites varchar sont alignés dans les deux validateurs. Aucun groupe InTransit dupliqué ni solde négatif observé lors du contrôle. Le registre `edi_imports` a été appliqué séparément après essai réussi d’annulation DDL et vérifié vide. Vérifier encore les droits et les autres contraintes, puis tester les imports sur tables isolées, les vraies courses concurrentes, le rollback transactionnel et la perte de réponse après commit.
4. Le noyau local `integration/edi_intake` implémente maintenant le parseur CSV et la coordination persistante. Valider les adaptateurs de production et un fichier représentatif vers `/api/v1/imports`. Les anciens appels Make envoient `file_name` et `file_content_base64` ; changer uniquement leur URL est incompatible. Les PDF et autres formats clients restent bloqués.
5. Persister les reçus d’import et l’état de chaque pièce jointe avant tout déplacement de mail. Vérifier le cas de deux pièces jointes dont une échoue, la reprise avec la même clé et le même contenu, et les réponses erronées ou incomplètes.
6. Inventorier les anciens chemins d’écriture, puis organiser leur bascule sans coexistence sur le même périmètre. Les chemins historiques ne participent pas aux nouveaux verrous transactionnels.
7. Obtenir une preuve de sauvegarde et de restauration, un point de retour applicatif utilisable et une procédure de réconciliation des imports. Un rollback applicatif n’annule pas les écritures métier ; conserver le registre `edi_imports`.
8. Valider un corpus PDF/CSV/XLS/XLSX représentatif et l’interface dans un navigateur desktop/mobile. Les tests simulés ne couvrent pas ces validations.

Les configurations et observations opérationnelles Make sont conservées dans le kit privé de revue, pas publiées dans ce dépôt. Ses correctifs pilotes et son scénario API inactif ne sont pas appliqués à la production.

## Vérifications de cette proposition

- Application : 132 tests hors réseau réussis ; Ruff et compilation Python réussis.
- Intégration publique : 73 tests hors réseau réussis (parseur, reçus, état persistant, API Flask locale et transport simulé). Les cinq tests de configurations opérationnelles du kit privé ne sont pas inclus ni réexécutés ici.
- Non validés : imports métier/concurrence PostgreSQL réels, restauration, bout en bout Make vers API, formats PDF réels et navigateur graphique. Le registre vide a été créé séparément ; aucun déploiement de ce code n’en découle.

Les anciens fichiers de données et images déjà suivis restent inchangés. `outputs/` est exclu de l’artefact de déploiement ; aucune suppression d’historique n’a été effectuée.
