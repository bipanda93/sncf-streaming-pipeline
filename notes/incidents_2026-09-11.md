# Incidents 2026-09-11 — Rotation de secret, accès Key Vault, intégration code

## 1. Installation azure-cli — Homebrew bloqué en compilation

**Symptôme** : `brew install azure-cli` lancé la veille au soir, encore en
cours au réveil — Homebrew compilait depuis les sources (openssl@3 seul :
47 minutes) au lieu d'utiliser des binaires précompilés, à cause d'outils
Xcode Command Line Tools désynchronisés.

**Fix** : abandon de la compilation, bascule vers `pip install azure-cli
--break-system-packages` — quelques minutes, binaire natif fonctionnel.

**Effet de bord noté, non traité** : conflits de dépendances dans
l'environnement conda `(base)` partagé entre projets (`pydantic` downgradé
sous le minimum requis par `apache-airflow-core` installé localement ;
`PyGithub` downgradé, casse `spyder`). Sans impact sur le pipeline réel
(qui tourne dans Docker, environnement isolé). Dette technique : isoler ce
genre d'outils dans un environnement virtuel dédié.

---

## 2. Cause racine du "Azure CLI not found on path"

`az` n'était qu'une fonction shell dans `.zshrc` lançant Azure CLI dans un
conteneur Docker — invisible pour `subprocess` (utilisé par
`AzureCliCredential`), qui ne résout que les vrais binaires du `PATH`,
jamais les fonctions/alias shell. D'où l'échec malgré un `az login`
parfaitement valide en interactif. Résolu par l'installation native (item
1).

---

## 3. Résolution réelle de l'accès Key Vault (suite du 10/09)

**Symptôme confirmé ce jour** (une fois l'authentification native
fonctionnelle) : `403 Forbidden` — "does not have secrets get permission".

**Diagnostic** : `az keyvault show --query properties.accessPolicies` a
révélé une seule entrée active, `object_id = e53f6eaa-...` — différent de
l'identité personnelle de Franck (`1471379d-...`). Ça a invalidé la
conclusion de la veille (qui pensait les deux identités identiques).

**Cause racine confirmée** : `env | grep ARM_` a montré que Terraform
s'authentifie via un Service Principal dédié (`ARM_CLIENT_ID` etc.), pas
via la session personnelle `az login` — les deux identités sont
réellement distinctes. Une politique d'accès Key Vault n'autorise qu'une
seule entrée par identité ; l'édition de la veille (`object_id` réécrit en
dur sur `terraform_sp`) aurait retiré l'accès au Service Principal réel au
prochain `apply`.

**Fix définitif** : restauration de `terraform_sp` à sa référence
dynamique d'origine (`data.azurerm_client_config.current.object_id`) +
ajout d'un second bloc séparé, `franck_read`, avec uniquement
`secret_permissions = ["Get"]`. `terraform plan` confirmé propre (1 ajout,
0 destruction) avant apply.

---

## 4. Incident de sécurité — `ARM_CLIENT_SECRET` exposé en clair, rotation à deux reprises

**Ce qui s'est passé** : en diagnostiquant l'identité Terraform (item 3),
`env | grep ARM_` a été demandé sans préciser de ne partager que
l'existence des variables, pas leurs valeurs — le vrai secret a été collé
en clair dans la conversation. Rotation immédiate lancée via
`az ad app credential reset`, mais la commande de rotation elle-même
imprime la nouvelle valeur en sortie, qui a de nouveau été collée par
erreur — nécessitant une **deuxième rotation**.

**Fix procédural retenu pour la suite** : toute rotation de secret doit
capturer la valeur dans une variable shell (`$(...)`) sans jamais
l'afficher à l'écran, l'écrire directement dans le fichier cible, puis
`unset` — jamais de copier-coller manuel d'une valeur sensible. La
tentative `sed` a échoué (caractères spéciaux dans le secret généré
cassant la commande) ; bascule vers une réécriture de fichier en Python
(lecture/écriture de texte brut, aucune interprétation shell de la
valeur) — fonctionne peu importe les caractères. Vérification finale
faite via `terraform plan` réussi (test d'authentification réel), jamais
en relisant la variable localement.

**Leçon retenue** : un `object_id`/identifiant n'est pas un secret (safe à
partager) ; un `client secret`, un `access token`, une `connection string`
avec `SharedAccessKey` le sont toujours — distinction appliquée
correctement dès ce moment pour le reste de la session (ex. secret Event
Hubs jamais collé, juste sa longueur).

---

## 5. Bug Python — portée de variable dans `build_kafka_config()`

**Symptôme** : après intégration de l'appel Key Vault dans `config.py`,
`KAFKA_SASL_PASSWORD` réassigné par erreur *à l'intérieur* de
`build_kafka_config()`, dans un bloc conditionnel dupliquant la logique
déjà présente au niveau du module.

**Cause** : en Python, toute assignation à un nom à l'intérieur d'une
fonction rend ce nom local à *toute* la fonction, dès sa première ligne —
même sous condition. Résultat : Key Vault interrogé une seconde fois à
chaque appel de la fonction, au lieu d'une seule fois au chargement du
module comme prévu.

**Fix** : suppression des 2 lignes en trop ; la fonction lit désormais
naturellement `KAFKA_SASL_PASSWORD` au niveau module, comme les autres
variables `KAFKA_*` du même dict.

---

## 6. Timeout `AzureCliCredential` — `DefaultAzureCredential` échoue en subprocess

**Symptôme** : `config.py` en mode `SASL_SSL` échoue avec
`AzureCliCredential: Failed to invoke the Azure CLI` — différent de
l'erreur "not found on path" du 10/09 (le binaire est bien trouvé cette
fois, mais son exécution échoue).

**Diagnostic** : `az account get-access-token --resource
https://vault.azure.net` en direct dans le terminal réussit sans
problème — donc pas un souci du CLI lui-même. Test isolé de
`AzureCliCredential(process_timeout=30)` (au lieu du défaut
`DefaultAzureCredential`) confirme : timeout par défaut de 10 secondes
trop court dans ce contexte précis (invocation via `subprocess` Python,
pas un terminal interactif direct) — catégorie de bug documentée côté
Microsoft pour cette combinaison azure-cli/Python/subprocess.

**Fix** : `DefaultAzureCredential()` → `DefaultAzureCredential(process_timeout=30)`
dans `_get_secret_from_keyvault()`. Confirmé fonctionnel : secret Event
Hubs récupéré (169 caractères), `build_kafka_config()` (producteur) ET
`build_kafka_read_options()` (Spark) tous deux opérationnels en mode
Azure sans avoir eu besoin de toucher `bronze_ingestion.py` — la
factorisation déjà en place ("jamais dupliqué", héritée d'une session
antérieure) a tenu son rôle.

## 7. CI/CD Terraform bloquée — secrets GitHub Actions absents, puis mal renseignés, puis désynchronisés

**Déclencheur** : email GitHub signalant l'échec de `terraform-plan` sur
le workflow `deploy.yml` (découvert à cette occasion — pipeline CI/CD déjà
existant dans le repo, plan automatique sur chaque push touchant
`infra/terraform/`, apply/destroy strictement manuels via
`workflow_dispatch` + protection d'environnement `production`).

**Étape 1 — secrets absents** : `Please run 'az login' to setup account`
dans les logs CI, révélant qu'aucun des 4 secrets `ARM_*` requis par le
workflow n'était configuré côté GitHub ("This repository has no
secrets"). Créés une première fois — mais le run suivant a montré une
erreur d'un tout autre type (voir étape 2), révélant que la création
n'avait en réalité rien enregistré (bouton de validation jamais atteint).

**Étape 2 — texte de commande collé au lieu de la valeur** :
`AADSTS900023: Specified tenant identifier 'echo -n "$arm_tenant_id" |
pbcopy' is neither a valid DNS name...` — le texte littéral d'une
commande `pbcopy` s'est retrouvé collé dans le champ valeur du secret
GitHub, signe qu'un clic sur une icône "copier" (au lieu d'une exécution
réelle dans le terminal suivie d'un collage du résultat) avait copié le
texte de la commande plutôt que son résultat.

**Étape 3 — plusieurs `pbcopy` lancés d'affilée, sans coller entre
chaque** : en corrigeant l'étape 2, les 4 commandes `pbcopy` (une par
secret) ont été exécutées à la suite dans le terminal, sans revenir sur
GitHub coller après chacune. Comme chaque `pbcopy` écrase le presse-papier
précédent, seule la dernière valeur (`ARM_SUBSCRIPTION_ID`) était encore
récupérable — les 3 autres n'avaient jamais atteint GitHub. Protocole
strict établi pour la suite : une commande, un collage, une validation,
confirmation explicite avant de passer à la suivante.

**Étape 4 — presse-papier vide, cause racine réelle** : même en suivant
le protocole strict, `ARM_TENANT_ID` restait faux côté CI. Vérification
du presse-papier via `pbpaste | wc -c` → `0`. La variable
`$ARM_TENANT_ID` était en réalité **vide dans cette session de
terminal** — pas un problème de méthode de copier-coller, un problème de
variable jamais chargée.

**Découverte de la cause racine** : `az` (déjà connu, wrapper Docker
défini dans `.zshrc`) n'était pas le seul — `terraform` l'est également,
et sa fonction charge en plus un fichier `.env.terraform` local
(`[ -f ./.env.terraform ] && source ./.env.terraform`), testé
relativement au dossier courant. Ce fichier — pas `.zshrc` — est la
vraie source des 4 variables `ARM_*` pour tout usage local de Terraform.
Il vit dans `infra/terraform/` (jamais à la racine du projet),
correctement ignoré par Git. Explique pourquoi `env | grep ARM_` avait
fonctionné les jours précédents (session avec `terraform` déjà appelé au
moins une fois) mais pas dans une session fraîche comme celle utilisée
pour configurer les secrets GitHub.

**Vérification de fraîcheur avant de propager** : avant de recopier
`ARM_CLIENT_SECRET` depuis `.env.terraform` vers GitHub, un `terraform
plan` local (avec la valeur du fichier fraîchement chargée) a servi de
test définitif — un secret périmé aurait échoué avec une erreur
d'authentification explicite. Succès confirmé (seul changement dans le
plan : la dérive cosmétique déjà connue sur `upgrade_settings` d'AKS),
donc valeur à jour, propagée en confiance.

**Résultat** : les 4 secrets GitHub Actions et le fichier
`.env.terraform` local utilisent maintenant la même source de vérité.
`terraform-plan` confirmé vert en CI (15s, toutes les étapes passées).

**Leçon retenue** : quand un secret censé être partagé entre plusieurs
usages (ici : session shell interactive et pipeline CI) se comporte de
façon incohérente, chercher s'il existe plusieurs mécanismes de
chargement distincts (ici : deux fonctions Docker séparées, une seule
avec un fichier `.env` additionnel) avant de soupçonner une erreur de
manipulation.

## 8. Test bout-en-bout réel Event Hubs — clé d'écriture confondue avec la clé de lecture

**Contexte** : première bascule réelle de `.env` vers Azure
(`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL=SASL_SSL`), après
tous les tests unitaires de la veille sur `config.py`.

**Symptôme** : le producteur (`src/ingestion/main.py --once`) se connecte
bien à Event Hubs (SASL_SSL établi, 804 perturbations récupérées depuis
l'API SNCF) mais échoue à l'écriture : `KafkaException:
TOPIC_AUTHORIZATION_FAILED`, précédé d'un warning `Topic sncf-raw
partition count changed from 2 to 0`.

**Cause racine** : `config.py` (écrit la veille) ne câblait qu'une seule
variable `KAFKA_SASL_PASSWORD`, remplie avec le secret
`eventhub-listen-connection-string` (droits lecture seule, `listen=true,
send=false`) — pensé à l'origine pour Spark, qui lit. Le producteur
partage cette même variable pour s'authentifier, mais lui a besoin
d'écrire — d'où le refus d'Event Hubs.

**Fix** : séparation en deux variables distinctes dans `config.py` —
`KAFKA_SASL_PASSWORD` (lecture, `eventhub-listen-connection-string`,
utilisé par Spark) et `KAFKA_SASL_PASSWORD_SEND` (écriture,
`eventhub-connection-string`, utilisé par `build_kafka_config()` — le
producteur). Une seule ligne changée dans `build_kafka_config()`
(`sasl.password` pointe désormais vers la nouvelle variable).

**Vérification** : après fix, 812 perturbations publiées avec succès
(`812 perturbation(s) publiée(s) sur le topic 'sncf-raw'`), warning de
partition disparu (même origine que l'erreur d'autorisation, pas un
problème séparé). Puis `bronze_ingestion.py --once` confirmé fonctionnel
en lecture : nouvelle transaction Delta (`00000000000000000252.json`)
écrite avec succès, malgré un avertissement bénin de transition d'offset
(`KafkaMicroBatchStream: offset was changed from 54456 to 384` — attendu,
le checkpoint local se souvenait de l'ancien Kafka, pas d'Event Hubs ;
absorbé sans erreur grâce à `failOnDataLoss=false` déjà en place depuis
l'incident du 28/08).

**Résultat** : boucle complète confirmée fonctionnelle en conditions
réelles — API SNCF → Event Hubs (Azure) → Spark → Delta local.

**Leçon retenue** : dès qu'un secret/clé a une portée volontairement
restreinte (ici, lecture seule vs écriture), vérifier explicitement que
chaque consommateur du code utilise la bonne — une seule variable de
config partagée entre deux usages aux besoins différents est un piège
classique, invisible tant qu'on ne teste pas les deux usages séparément.
