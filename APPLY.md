# Appliquer ce lot à une branche de travail

Ce ZIP contient les fichiers de remplacement et ajouts, pas une copie du dépôt ni son historique. Le point d’entrée App.py est remplacé complètement afin de ne pas reproduire la ligne confidentielle de l’ancienne version dans un diff.

1. Partir d’une branche de travail du dépôt STS-Engineer/EDI_Stock, basée sur le commit 8711935cb7545497412955e79de955959f6d221b ou réconcilier les modifications intervenues depuis.
2. Copier le contenu de ce dossier à la racine de cette branche en revue. Préserver les fichiers non concernés. Ne pas pousser sur master : son workflow déploie automatiquement.
3. Installer les dépendances dans un environnement de test et exécuter les vérifications de README.md.
4. Revoir docs/RELEASE_CHECKLIST.md, puis faire les essais autorisés en staging.
5. Publier uniquement après revue et autorisation explicite. La migration SQL est distincte de l’application des fichiers et ne doit pas être exécutée automatiquement.

Fichiers existants remplacés : App.py, requirements.txt, .github/workflows/master_edi-sotck.yml. Autres fichiers de ce lot : ajouts. Les données historiques outputs et images du dépôt ne sont pas incluses ; le nouveau dashboard ne dépend pas des images historiques.
