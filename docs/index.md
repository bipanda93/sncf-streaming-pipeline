---
hide:
  - toc
---

<div class="hero" markdown>

<p class="eyebrow">PROJET 2 · MÉMOIRE · DATA ENGINEERING</p>

# SNCF Streaming Pipeline

Un pipeline temps réel sur les perturbations du réseau ferré, de l'API au tableau de bord.

[Architecture](notes/decisions_architecture.md){ .md-button }
[Lire les incidents →](incidents.md){ .md-button .md-button--ghost }

<p class="stats"><b>8</b> contrôles CI obligatoires <i>·</i> <b>22</b> journaux d'incidents <i>·</i> <b>~6,8 Go</b> de RAM mesurés au pic Spark</p>

</div>

<p class="flow">API SNCF → Kafka → Bronze → Silver → Gold → Power BI</p>
<p class="flow-sub">Spark Structured Streaming et Delta Lake, orchestrés par Airflow. Déploiement actuel : local.</p>
