# Script manuel : teste l'accès au Key Vault kv-sncf-dev.
# Ne fonctionne que si l'infra Terraform est recréée (détruite le 15/09).
# Non lancé par la CI.

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

key_vault_name = "kv-sncf-dev"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
secret_name = "eventhub-listen-connection-string"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_uri, credential=credential)

retrieved_secret = client.get_secret(secret_name)
print(f"Secret '{secret_name}' récupéré avec succès")