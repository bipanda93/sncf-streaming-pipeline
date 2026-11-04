# Outils et flux

## Architecture

```mermaid
flowchart LR
  AF{{"Airflow<br/>6 DAGs"}}
  subgraph PIPE["Pipeline SNCF"]
    direction LR
    API["API SNCF<br/>temps réel"] --> PROD["Producteur<br/>Python"] --> KAF["Kafka<br/>Event Hubs sur Azure"]
    OD["Open data SNCF<br/>régularité"]
    REF["Référentiel<br/>gares"]
    BR[("Bronze<br/>Delta")]
    SI[("Silver<br/>Delta")]
    GO[("Gold<br/>Delta")]
    KAF --> BR
    OD --> BR
    BR --> SI
    REF --> SI
    SI --> GO
    GO --> IF["Isolation Forest<br/>anomalies"]
    GO --> CL["Claude API<br/>résumés"]
    GO --> PB["Power BI"]
    GO --> ST["Streamlit<br/>santé, carte, ML"]
    IF --> ST
    CL --> ST
  end
  AF -.orchestre.-> PIPE
  AF -.échec.-> SL["Slack<br/>alertes"]
```

Les données arrivent de l'API SNCF (temps réel) et de l'open data (historique), passent par Kafka puis par les trois couches Delta Bronze, Silver et Gold. Le Gold alimente Power BI, Streamlit, l'Isolation Forest et les résumés Claude. Airflow orchestre l'ensemble et prévient Slack en cas d'échec.

## Outils utilisés

Vue d'ensemble des outils et de leur place dans le pipeline.

```mermaid
flowchart TB
  subgraph SRC["Sources"]
    direction LR
    A["API SNCF<br/>temps réel"]
    B["Open data SNCF<br/>régularité 2013-2026"]
    C["Référentiel gares"]
  end
  subgraph ING["Ingestion"]
    direction LR
    P["Python<br/>producteur"] --> K["Kafka<br/>(Event Hubs sur Azure)"]
  end
  subgraph TRT["Traitement : PySpark et Delta Lake, orchestré par Airflow"]
    direction LR
    BR["Bronze"] --> SI["Silver"] --> GO["Gold"]
  end
  subgraph IA["IA"]
    direction LR
    CL["Claude API<br/>résumés"]
    IF["Isolation Forest<br/>anomalies"]
  end
  subgraph RES["Restitution"]
    direction LR
    PB["Power BI"]
    ST["Streamlit"]
  end
  subgraph OPS["Exploitation"]
    direction LR
    TF["Terraform"] --> AZ["Azure"]
    PR["Prometheus"] --> GR["Grafana"]
  end
  subgraph LIV["Qualité et livraison"]
    direction LR
    DP["Dependabot"] --> GH["GitHub Actions<br/>8 contrôles"] --> MK["MkDocs<br/>GitHub Pages"]
    GH --> SL["Slack"]
  end
  A --> P
  K --> BR
  B --> BR
  C --> ST
  GO --> PB
  GO --> ST
  GO --> CL
  GO --> IF
  CL --> ST
  IF --> ST
  AF["Apache Airflow<br/>6 DAGs"] -.orchestre.-> TRT
  OPS ~~~ LIV
```

Lecture du schéma : les sources alimentent Kafka (Event Hubs sur Azure) puis la chaîne Bronze, Silver, Gold de PySpark et Delta Lake, orchestrée par Airflow. Power BI et Streamlit restituent les résultats ; Claude produit les résumés mensuels et l'Isolation Forest détecte les anomalies sur les métriques d'exécution. Terraform décrit l'infrastructure Azure (déployée une fois puis détruite), Prometheus et Grafana surveillent l'infrastructure, et GitHub Actions, Dependabot, Slack et MkDocs assurent la qualité et la livraison.
